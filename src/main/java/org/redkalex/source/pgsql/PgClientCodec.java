/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.*;
import org.redkale.net.client.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class PgClientCodec extends ClientCodec<PgClientRequest, PgResultSet> {

    protected static final Logger logger = Logger.getLogger(PgClientCodec.class.getSimpleName());

    protected char halfFrameCmd;

    protected int halfFrameLength;

    protected ByteArray halfFrameBytes;

    private ByteArray recyclableArray;

    PgResultSet lastResult = null;

    Throwable lastExc = null;

    int lastCount;

    public PgClientCodec(ClientConnection connection) {
        super(connection);
    }

    protected ByteArray pollArray() {
        if (recyclableArray == null) {
            recyclableArray = new ByteArray();
            return recyclableArray;
        }
        recyclableArray.clear();
        return recyclableArray;
    }

    @Override //解析完成返回true，还需要继续读取返回false; 返回true: array会clear, 返回false: buffer会clear
    public boolean codecResult(final ByteBuffer realbuf, ByteArray array) {
        PgClientConnection conn = (PgClientConnection) connection;
        if (!realbuf.hasRemaining()) return false;
        ByteBuffer buffer = realbuf;
        //logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", realbuf读到的长度: " + realbuf.remaining() + ", halfFrameBytes=" + (halfFrameBytes == null ? -1 : halfFrameBytes.length()));
        if (halfFrameBytes != null) {  //存在半包
            int remain = realbuf.remaining();
            if (halfFrameCmd == 0) {
                int cha = 5 - halfFrameBytes.length();
                if (remain < cha) { //还是不够5字节
                    halfFrameBytes.put(realbuf);
                    return false;
                }
                halfFrameBytes.put(realbuf, cha);
                halfFrameCmd = (char) halfFrameBytes.get(0);
                halfFrameLength = halfFrameBytes.getInt(1);
            }
            //此时必然有halfFrameCmd、halfFrameLength
            int bulen = halfFrameLength + 1 - halfFrameBytes.length(); //还差多少字节
            if (bulen > remain) { //不够，继续读取
                halfFrameBytes.put(realbuf);
                return false;
            }
            halfFrameBytes.put(realbuf, bulen);
            //此时halfFrameBytes是完整的frame数据            
            buffer = ByteBuffer.wrap(halfFrameBytes.content(), 0, halfFrameBytes.length());
            halfFrameBytes = null;
            halfFrameCmd = 0;
            halfFrameLength = 0;
        } else if (realbuf.remaining() < 5) { //第一次就只有几个字节buffer
            halfFrameBytes = pollArray();
            halfFrameBytes.put(realbuf);
            return false;
        }
        //buffer必然包含一个完整的frame数据
        boolean hadresult = false;
        PgClientRequest request = null;
        Iterator<ClientFuture> respIt = responseQueue().iterator();
        boolean update_1_to_2 = false;
        while (buffer.hasRemaining()) {
            while (buffer.hasRemaining()) {
                if (request == null) request = respIt.hasNext() ? (PgClientRequest) respIt.next().getRequest() : null;
                final char cmd = (char) buffer.get();
                if (buffer.remaining() < 4) {
                    if (halfFrameBytes == null) {
                        halfFrameBytes = pollArray();
                    } else {
                        halfFrameBytes.clear();
                    }
                    halfFrameBytes.putByte(cmd);
                    halfFrameBytes.put(buffer);
                    return hadresult;
                }
                final int bufpos = buffer.position();
                final int length = buffer.getInt();
                if (PgsqlDataSource.debug && conn.isAuthenticated()) logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", cmd=" + cmd + ", length=" + ((length - 4) > 9 ? "" : " ") + (length - 4) + ", remains=" + buffer.remaining());
                if (buffer.remaining() < length - 4) {
                    if (halfFrameBytes == null) {
                        halfFrameBytes = pollArray();
                    } else {
                        halfFrameBytes.clear();
                    }
                    halfFrameCmd = cmd;
                    halfFrameLength = length;
                    halfFrameBytes.putByte(cmd);
                    halfFrameBytes.putInt(length);
                    halfFrameBytes.put(buffer);
                    return hadresult;
                }
                if (cmd == 'R') {  //登录鉴权
                    int type = buffer.getInt();
                    byte[] salt = null;
                    boolean authOK = false;
                    List<String> mechanisms = null;
                    PgReqAuthScramSaslContinueResult saslResult = null;
                    if (type == 0) {
                        //认证通过     
                        authOK = true;
                    } else if (type == 3 || type == 5) {//3:需要密码; 5:需要salt密码
                        if (type == 5) {
                            salt = new byte[4];
                            buffer.get(salt);
                        }
                    } else if (type == 10) { //sasl
                        mechanisms = new ArrayList<>();
                        do {
                            mechanisms.add(readUTF8String(buffer, array));
                        } while (buffer.get() != 0);
                        if (!mechanisms.contains("SCRAM-SHA-256")) {
                            throw new UnsupportedOperationException("SASL Authentication : only SCRAM-SHA-256 is currently supported, server wants " + mechanisms);
                        }
                    } else if (type == 11) { //sasl-continue
                        String saslmsg = readUTF8String(buffer, array);
                        saslResult = new PgReqAuthScramSaslContinueResult((PgReqAuthScramPassword) request, saslmsg);
                    } else if (type == 12) { //sasl-final
                        String saslmsg = readUTF8Base64String(buffer, array);
                        ((PgReqAuthScramSaslFinal) request).checkFinal(saslmsg, buffer);
                        //while (buffer.hasRemaining()) buffer.get();
                        authOK = true;
                    } else {
                        throw new UnsupportedOperationException("illegal type: " + type);
                    }
                    if (lastResult == null) lastResult = new PgRespAuthResultSet();
                    PgRespAuthResultSet result = (PgRespAuthResultSet) lastResult;
                    result.setAuthOK(authOK);
                    result.setAuthSalt(salt);
                    result.setAuthMechanisms(mechanisms);
                    result.setAuthSaslContinueResult(saslResult);
                    if (buffer.position() != bufpos + length) {
                        logger.log(Level.SEVERE, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", R buffer-currpos : " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                    }
                    if (!buffer.hasRemaining()) {
                        halfFrameBytes = null;
                        halfFrameCmd = 0;
                        halfFrameLength = 0;
                        addResult(lastResult);
                        lastResult = null;
                        lastExc = null;
                        lastCount = 0;
                        hadresult = true;
                        request = null;
                        break;
                    }
                } else if (cmd == 'C') {
                    if (lastResult == null) lastResult = conn.pollResultSet(request == null ? null : request.info);
                    if (request != null && (request.getType() != PgClientRequest.REQ_TYPE_QUERY)) {
                        int count = PgRespCountDecoder.instance.read(conn, buffer, length, array, request, lastResult);
                        if (count != Integer.MIN_VALUE) {
                            if (request.getType() == PgClientRequest.REQ_TYPE_BATCH) {
                                lastResult.increBatchEffectCount(((PgReqBatch) request).sqls.length, count);
                            } else {
                                lastResult.increUpdateEffectCount(count);
                            }
                            if (count == 0 && request.getType() == PgClientRequest.REQ_TYPE_EXTEND_QUERY && ((PgReqExtended) request).finds) {
                                lastResult.addRowData(null);
                            }
                        }
                        if (buffer.position() != bufpos + length) {
                            logger.log(Level.SEVERE, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", C buffer-currpos : " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                            buffer.position(bufpos + length);
                        }
                    } else {
                        buffer.position(bufpos + length);
                    }
                } else if (cmd == 'T') {  //RowDesc
                    if (request != null) {
                        if (lastResult == null) lastResult = conn.pollResultSet(request.info);
                        PgRowDesc rowDesc = PgRespRowDescDecoder.instance.read(conn, buffer, length, array, request, lastResult);
                        lastResult.setRowDesc(rowDesc);
                        if (request.isExtendType()) {
                            conn.putPrepareDesc(((PgReqExtended) request).sql, rowDesc);
                        }
                        if (buffer.position() != bufpos + length) {
                            logger.log(Level.SEVERE, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", T buffer-currpos : " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                            buffer.position(bufpos + length);
                        }
                    } else {
                        buffer.position(bufpos + length);
                    }
                } else if (cmd == 'D') {
                    if (request != null) {
                        if (lastResult == null) lastResult = conn.pollResultSet(request.info);
                        PgRowData rowData = PgRespRowDataDecoder.instance.read(conn, buffer, length, array, request, lastResult);
                        lastResult.addRowData(rowData);
                        if (buffer.position() != bufpos + length) {
                            logger.log(Level.SEVERE, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", D buffer-currpos : " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                            buffer.position(bufpos + length);
                        }
                    } else {
                        buffer.position(bufpos + length);
                    }
                } else if (cmd == 'E') {
                    if (request != null) {
                        Exception exc = PgRespErrorDecoder.instance.read(conn, buffer, length, array, request, lastResult);
                        if (lastExc == null) lastExc = exc; //以第一个异常为准，例如insert多个，当表不存在prepare异常时，bind的异常不是表不存在异常。
                        if (buffer.position() != bufpos + length) {
                            logger.log(Level.SEVERE, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", E buffer-currpos : " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                        }
                        if (request.isExtendType() && ((PgReqExtended) request).sendPrepare) {
                            conn.getPrepareFlag(((PgReqExtended) request).sql).set(false);
                        }
                    }
                    buffer.position(bufpos + length);
                    if (!conn.isAuthenticated()) { //登录失败的异常不会有ReadyForQuery消息体
                        halfFrameBytes = null;
                        halfFrameCmd = 0;
                        halfFrameLength = 0;
                        addResult(lastExc);
                        lastResult = null;
                        lastExc = null;
                        lastCount = 0;
                        hadresult = true;
                        request = null;
                        break;
                    }
                } else if (cmd == 'Z') { //ReadyForQuery
                    lastCount++;
                    buffer.get(); //'I' TransactionState.IDLE、'T' TransactionState.OPEN、'E' TransactionState.FAILED
                    //if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + " , " + request + "----------------------cmd:" + cmd);
                    if (lastResult != null) {
                        if (request.isExtendType()) { //PgReqExtended
                            PgReqExtended reqext = (PgReqExtended) request;
                            int mergeCount = reqext.getMergeCount();
                            if (mergeCount > 0) {
                                if (reqext.mode == PgReqExtended.PgReqExtendMode.LIST_ALL) {
                                    int page = lastResult.rowData.size() / (1 + mergeCount);
                                    if (page > 0) {
                                        lastResult.page = page;
                                        lastResult.limit = page;
                                    }
                                } else if (reqext.mode == PgReqExtended.PgReqExtendMode.FINDS) {
                                    int[] pages = reqext.findsCount;
                                    if (pages != null) {
                                        lastResult.pages = pages;
                                        lastResult.limit = pages[0];
                                    }
                                }
                                for (int i = 0; i < mergeCount; i++) {
                                    if (respIt.hasNext()) respIt.next();
                                }
                            }
                            if (lastResult.rowDesc == null) {
                                PgRowDesc rowDesc = conn.getPrepareDesc(reqext.sql);
                                if (rowDesc != null) lastResult.setRowDesc(rowDesc);
                            }
                        }
                        lastResult.effectRespCount++;
                    }
                    if (buffer.position() != bufpos + length) {
                        logger.log(Level.SEVERE, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", Z buffer-currpos : " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                    }
                    buffer.position(bufpos + length);

                    if (request != null && lastCount < request.getSyncedCount()) continue;

                    halfFrameBytes = null;
                    halfFrameCmd = 0;
                    halfFrameLength = 0;
                    if (update_1_to_2) return hadresult;
                    if (lastExc != null) {
                        addResult(lastExc);
                    } else if (lastResult != null) {
                        addResult(lastResult);
                        lastResult = null;
                    }
                    lastExc = null;
                    lastCount = 0;
                    hadresult = true;
                    request = null;
                    update_1_to_2 = false;
                    break;
                } else if (cmd == '1') { //UPDATE命令会先发个1、t、n、Z再发2、C、Z,  也可能是1、t、n再发2、E、Z,  也可能是1、t、T、Z再发2、D...C/E、Z
                    //if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + " , " + request + "----------------------cmd:" + cmd);
                    update_1_to_2 = true;
                    buffer.position(bufpos + length);
                } else if (cmd == '2') { //UPDATE命令会先发个1、t、n、Z再发2、C、Z,  也可能是1、t、n再发2、E、Z,  也可能是1、t、T、Z再发2、D...C/E、Z
                    //if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + " , " + request + "----------------------cmd:" + cmd);
                    update_1_to_2 = false;
                    buffer.position(bufpos + length);
                } else {
                    //if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + " , " + request + "----------------------cmd:" + cmd);
                    buffer.position(bufpos + length);
                }
            }
            buffer = realbuf;
        }
        //解析了完整的frame，但还是没解析到Z消息
        return hadresult;
    }

    protected static String readUTF8String(ByteBuffer buffer, ByteArray array) {
        int i = 0;
        array.clear();
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            array.put(c);
            if (!buffer.hasRemaining()) break;
        }
        return array.toString(StandardCharsets.UTF_8);
    }

    protected static String readUTF8Base64String(ByteBuffer buffer, ByteArray array) {
        int i = 0;
        array.clear();
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            array.put(c);
            if (array.length() > 3 && c == '=' && buffer.hasRemaining() && buffer.get(buffer.position()) == 'R') break;
            if (!buffer.hasRemaining()) break;
        }
        return array.toString(StandardCharsets.UTF_8);
    }

    @Override
    public void reset() {
        super.reset();
    }
}
