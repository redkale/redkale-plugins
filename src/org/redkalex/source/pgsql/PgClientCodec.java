/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
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

    protected ByteArray pollArray() {
        if (recyclableArray == null) {
            recyclableArray = new ByteArray();
            return recyclableArray;
        }
        recyclableArray.clear();
        return recyclableArray;
    }

    @Override //解析完成返回true，还需要继续读取返回false; 返回true后array会clear
    public boolean codecResult(ClientConnection conn, List<PgClientRequest> requests, final ByteBuffer realbuf, ByteArray array) {
        if (!realbuf.hasRemaining()) return false;
        ByteBuffer buffer = realbuf;
        //System.out.println("realbuf读到的长度: " + realbuf.remaining() + ", halfFrameBytes=" + (halfFrameBytes == null ? -1 : halfFrameBytes.length()));
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
        int reqindex = -1;
        PgClientRequest request = null;
        while (buffer.hasRemaining()) {
            while (buffer.hasRemaining()) {
                if (request == null) request = requests.get(++reqindex);
                final char cmd = (char) buffer.get();
                if (buffer.remaining() < 4) {
                    if (halfFrameBytes == null) {
                        halfFrameBytes = pollArray();
                    } else {
                        halfFrameBytes.clear();
                    }
                    halfFrameBytes.put((byte) cmd);
                    halfFrameBytes.put(buffer);
                    return false;
                }
                final int length = buffer.getInt();
                //System.out.println("cmd=" + cmd + ", length=" + (length - 4) + ", remains=" + buffer.remaining());
                if (buffer.remaining() < length - 4) {
                    if (halfFrameBytes == null) {
                        halfFrameBytes = pollArray();
                    } else {
                        halfFrameBytes.clear();
                    }
                    halfFrameCmd = cmd;
                    halfFrameLength = length;
                    halfFrameBytes.put((byte) cmd);
                    halfFrameBytes.putInt(length);
                    halfFrameBytes.put(buffer);
                    return false;
                }
                if (cmd == 'R') {  //登录鉴权
                    int type = buffer.getInt();
                    byte[] salt = null;
                    boolean authOK = false;
                    if (type == 0) {
                        //认证通过     
                        authOK = true;
                    } else if (type == 3 || type == 5) {//3:需要密码; 5:需要salt密码
                        if (type == 5) {
                            salt = new byte[4];
                            buffer.get(salt);
                        }
                    } else {
                        throw new RuntimeException("错误的type: " + type);
                    }
                    if (lastResult == null) lastResult = new PgResultSet();
                    lastResult.setAuthOK(authOK);
                    lastResult.setAuthSalt(salt);
                    if (!buffer.hasRemaining()) {
                        halfFrameBytes = null;
                        halfFrameCmd = 0;
                        halfFrameLength = 0;
                        addResult(lastResult);
                        lastResult = null;
                        lastExc = null;
                        hadresult = true;
                        request = null;
                        break;
                    }
                } else if (cmd == 'C') {
                    if (request != null && (request.getType() != PgClientRequest.REQ_TYPE_QUERY
                        && request.getType() != PgClientRequest.REQ_TYPE_EXTEND_QUERY)) {
                        String val = readUTF8String(buffer, array);
                        //if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) System.out.println(conn + " , " + request + "----------------------cmd:" + cmd + ", val=" + val);
                        int pos = val.lastIndexOf(' ');
                        if (pos > 0) {
                            if (lastResult == null) lastResult = new PgResultSet();
                            lastResult.increUpdateEffectCount(Integer.parseInt(val.substring(pos + 1)));
                        }
                    } else {
                        buffer.position(buffer.position() + length - 4);
                    }
                } else if (cmd == 'T') {
                    if (request == null) {
                        buffer.position(buffer.position() + length - 4);
                    } else {
                        if (lastResult == null) lastResult = new PgResultSet();
                        lastResult.setRowDesc(new PgRespRowDescDecoder().read(buffer, length, array));
                        PgExtendedCommandNode node = ((PgClientConnection) conn).currExtendedCacheNode;
                        if (node != null && node.rowDesc == null) node.rowDesc = lastResult.rowDesc;
                    }
                } else if (cmd == 'D') {
                    if (request == null) {
                        buffer.position(buffer.position() + length - 4);
                    } else {
                        if (lastResult == null) lastResult = new PgResultSet();
                        PgRowData rowData = new PgRespRowDataDecoder().read(buffer, length, array);
                        lastResult.addRowData(rowData);
                    }
                } else if (cmd == 'E') {
                    if (request == null) {
                        buffer.position(buffer.position() + length - 4);
                    } else {
                        String level = null;
                        String code = null;
                        String message = null;
                        for (byte type = buffer.get(); type != 0; type = buffer.get()) {
                            String value = readUTF8String(buffer, array);
                            if (type == (byte) 'S') {
                                level = value;
                            } else if (type == 'C') {
                                code = value;
                            } else if (type == 'M') {
                                message = value;
                            }
                        }
                        lastExc = new SQLException(message, code, 0);
                    }
                    if (!conn.isAuthenticated()) { //登录失败的异常不会有ReadyForQuery消息体
                        halfFrameBytes = null;
                        halfFrameCmd = 0;
                        halfFrameLength = 0;
                        addResult(lastExc);
                        lastResult = null;
                        lastExc = null;
                        hadresult = true;
                        request = null;
                        break;
                    }
                } else if (cmd == 'Z') { //ReadyForQuery
                    buffer.get(); //'I' TransactionState.IDLE、'T' TransactionState.OPEN、'E' TransactionState.FAILED
                    //if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) System.out.println(conn + " , " + request + "----------------------cmd:" + cmd);
                    if (lastResult != null) {
                        if (lastResult.rowDesc == null) {
                            if (request != null && request.currExtendedRowDescSupplier != null && request.currExtendedRowDescSupplier.get() != null) {
                                lastResult.setRowDesc(request.currExtendedRowDescSupplier.get());
                            } else {
                                PgExtendedCommandNode node = ((PgClientConnection) conn).currExtendedCacheNode;
                                if (node != null && node.rowDesc != null) {
                                    lastResult.setRowDesc(node.rowDesc);
                                } else if (request == null || request.getType() == PgClientRequest.REQ_TYPE_QUERY || request.getType() == PgClientRequest.REQ_TYPE_EXTEND_QUERY) {
                                    logger.log(Level.SEVERE, "result not found rowDesc, request=" + request);
                                }
                            }
                        }
                    }
                    halfFrameBytes = null;
                    halfFrameCmd = 0;
                    halfFrameLength = 0;
                    if (lastResult != null) {
                        addResult(lastResult);
                        lastResult = null;
                    }
                    lastExc = null;
                    hadresult = true;
                    request = null;
                    break;
                } else if (cmd == 'n') { //UPDATE命令会先发个1、t、n、Z再发2、C、Z
                    if (lastResult == null) lastResult = new PgResultSet();
                    //if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) System.out.println(conn + " , " + request + "----------------------cmd:" + cmd);
                    buffer.position(buffer.position() + length - 4);
                } else {
                    //if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) System.out.println(conn + " , " + request + "----------------------cmd:" + cmd);
                    buffer.position(buffer.position() + length - 4);
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
        }
        return array.toString(StandardCharsets.UTF_8);
    }

    @Override
    public void reset() {
        super.reset();
    }
}
