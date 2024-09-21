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
import org.redkalex.source.pgsql.PgPrepareDesc.PgExtendMode;

/** @author zhangjx */
public class PgClientCodec extends ClientCodec<PgClientRequest, PgResultSet> {

    public static final byte MESSAGE_TYPE_BACKEND_KEY_DATA = 'K';

    public static final byte MESSAGE_TYPE_AUTHENTICATION = 'R';

    public static final byte MESSAGE_TYPE_ERROR_RESPONSE = 'E';

    public static final byte MESSAGE_TYPE_NOTICE_RESPONSE = 'N';

    public static final byte MESSAGE_TYPE_NOTIFICATION_RESPONSE = 'A';

    public static final byte MESSAGE_TYPE_COMMAND_COMPLETE = 'C';

    public static final byte MESSAGE_TYPE_PARAMETER_STATUS = 'S';

    public static final byte MESSAGE_TYPE_READY_FOR_QUERY = 'Z';

    public static final byte MESSAGE_TYPE_PARAMETER_DESCRIPTION = 't';

    public static final byte MESSAGE_TYPE_ROW_DESCRIPTION = 'T';

    public static final byte MESSAGE_TYPE_DATA_ROW = 'D';

    public static final byte MESSAGE_TYPE_PORTAL_SUSPENDED = 's';

    public static final byte MESSAGE_TYPE_NO_DATA = 'n';

    public static final byte MESSAGE_TYPE_EMPTY_QUERY_RESPONSE = 'I';

    public static final byte MESSAGE_TYPE_PARSE_COMPLETE = '1';

    public static final byte MESSAGE_TYPE_BIND_COMPLETE = '2';

    public static final byte MESSAGE_TYPE_CLOSE_COMPLETE = '3';

    public static final byte MESSAGE_TYPE_FUNCTION_RESULT = 'V';

    public static final byte MESSAGE_TYPE_SSL_YES = 'S';

    public static final byte MESSAGE_TYPE_SSL_NO = 'N';

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

    protected PgResultSet pollResultSet(PgClientRequest request) {
        PgResultSet rs = new PgResultSet();
        rs.request = request;
        rs.info = request == null ? null : request.info;
        return rs;
    }

    protected ByteArray pollArray() {
        if (recyclableArray == null) {
            recyclableArray = new ByteArray();
            return recyclableArray;
        }
        recyclableArray.clear();
        return recyclableArray;
    }

    @Override
    public void decodeMessages(final ByteBuffer realBuf, ByteArray array) {
        PgClientConnection conn = (PgClientConnection) connection;
        if (!realBuf.hasRemaining()) {
            return;
        }
        ByteBuffer buffer = realBuf;
        // logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn +
        // ", realbuf读到的长度: " + realBuf.remaining() + ", halfFrameBytes=" + (halfFrameBytes == null ? -1 :
        // halfFrameBytes.length()));
        if (halfFrameBytes != null) { // 存在半包
            int remain = realBuf.remaining();
            if (halfFrameCmd == 0) {
                int cha = 5 - halfFrameBytes.length();
                if (remain < cha) { // 还是不够5字节
                    halfFrameBytes.put(realBuf);
                    return;
                }
                halfFrameBytes.put(realBuf, cha);
                halfFrameCmd = (char) halfFrameBytes.get(0);
                halfFrameLength = halfFrameBytes.getInt(1);
            }
            // 此时必然有halfFrameCmd、halfFrameLength
            int bulen = halfFrameLength + 1 - halfFrameBytes.length(); // 还差多少字节
            if (bulen > remain) { // 不够，继续读取
                halfFrameBytes.put(realBuf);
                return;
            }
            halfFrameBytes.put(realBuf, bulen);
            // 此时halfFrameBytes是完整的frame数据
            buffer = ByteBuffer.wrap(halfFrameBytes.content(), 0, halfFrameBytes.length());
            halfFrameBytes = null;
            halfFrameCmd = 0;
            halfFrameLength = 0;
        } else if (realBuf.remaining() < 5) { // 第一次就只有几个字节buffer
            halfFrameBytes = pollArray();
            halfFrameBytes.put(realBuf);
            return;
        }
        // buffer必然包含一个完整的frame数据
        PgClientRequest request = null;
        boolean update1To2 = false;
        while (buffer.hasRemaining()) {
            while (buffer.hasRemaining()) {
                if (request == null) {
                    request = nextRequest();
                }
                final char cmd = (char) buffer.get();
                if (buffer.remaining() < 4) {
                    if (halfFrameBytes == null) {
                        halfFrameBytes = pollArray();
                    } else {
                        halfFrameBytes.clear();
                    }
                    halfFrameBytes.putByte(cmd);
                    halfFrameBytes.put(buffer);
                    if (PgsqlDataSource.debug) {
                        logger.log(
                                Level.FINEST,
                                "[" + Times.nowMillis() + "] ["
                                        + Thread.currentThread().getName() + "]: " + conn
                                        + ", half data continue read.");
                    }
                    return;
                }
                final int bufpos = buffer.position();
                final int length = buffer.getInt();
                if (PgsqlDataSource.debug && conn.isAuthenticated()) {
                    logger.log(
                            Level.FINEST,
                            "[" + Times.nowMillis() + "] ["
                                    + Thread.currentThread().getName() + "]: " + conn + ", cmd=" + cmd + ", length="
                                    + ((length - 4) > 9 ? "" : " ") + (length - 4) + ", remains=" + buffer.remaining());
                }
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
                    if (PgsqlDataSource.debug) {
                        logger.log(
                                Level.FINEST,
                                "[" + Times.nowMillis() + "] ["
                                        + Thread.currentThread().getName() + "]: " + conn
                                        + ", half data continue read.");
                    }
                    return;
                }
                if (cmd == MESSAGE_TYPE_AUTHENTICATION) { // 'R' 登录鉴权
                    int type = buffer.getInt();
                    byte[] salt = null;
                    boolean authOK = false;
                    List<String> mechanisms = null;
                    PgReqAuthScramSaslContinueResult saslResult = null;
                    if (type == 0) {
                        // 认证通过
                        authOK = true;
                    } else if (type == 3 || type == 5) { // 3:需要密码; 5:需要salt密码
                        if (type == 5) {
                            salt = new byte[4];
                            buffer.get(salt);
                        }
                    } else if (type == 10) { // sasl
                        mechanisms = new ArrayList<>();
                        do {
                            mechanisms.add(readUTF8String(buffer, array));
                        } while (buffer.get() != 0);
                        if (!mechanisms.contains("SCRAM-SHA-256")) {
                            throw new UnsupportedOperationException(
                                    "SASL Authentication : only SCRAM-SHA-256 is currently supported, server wants "
                                            + mechanisms);
                        }
                    } else if (type == 11) { // sasl-continue
                        String saslmsg = readUTF8String(buffer, array);
                        saslResult = new PgReqAuthScramSaslContinueResult((PgReqAuthScramPassword) request, saslmsg);
                    } else if (type == 12) { // sasl-final
                        String saslmsg = readUTF8Base64String(buffer, array);
                        ((PgReqAuthScramSaslFinal) request).checkFinal(saslmsg, buffer);
                        // while (buffer.hasRemaining()) buffer.get();
                        authOK = true;
                    } else {
                        throw new UnsupportedOperationException("illegal type: " + type);
                    }
                    if (lastResult == null) {
                        lastResult = new PgRespAuthResultSet();
                    }
                    PgRespAuthResultSet result = (PgRespAuthResultSet) lastResult;
                    result.setAuthOK(authOK);
                    result.setAuthSalt(salt);
                    result.setAuthMechanisms(mechanisms);
                    result.setAuthSaslContinueResult(saslResult);
                    if (buffer.position() != bufpos + length) {
                        logger.log(
                                Level.SEVERE,
                                "[" + Times.nowMillis() + "] ["
                                        + Thread.currentThread().getName() + "]: " + conn + ", R buffer-currpos : "
                                        + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                    }
                    if (!buffer.hasRemaining()) {
                        halfFrameBytes = null;
                        halfFrameCmd = 0;
                        halfFrameLength = 0;
                        addMessage(request, lastResult);
                        lastResult = null;
                        lastExc = null;
                        lastCount = 0;
                        request = null;
                        break;
                    }
                } else if (cmd == MESSAGE_TYPE_COMMAND_COMPLETE) { // 'C' Count
                    if (lastResult == null) {
                        lastResult = pollResultSet(request);
                    }
                    if (request != null
                            && request.isExtendType()
                            && (((PgReqExtended) request).mode == PgExtendMode.FIND_ENTITY
                                    || ((PgReqExtended) request).mode == PgExtendMode.LISTALL_ENTITY)) {
                        buffer.position(bufpos + length);
                    } else if (request != null && (request.getType() != PgClientRequest.REQ_TYPE_QUERY)) {
                        int count = PgRespCountDecoder.instance.read(conn, buffer, length, array, request, lastResult);
                        if (PgsqlDataSource.debug) {
                            logger.log(
                                    Level.FINEST,
                                    "[" + Times.nowMillis() + "] ["
                                            + Thread.currentThread().getName() + "]: " + conn + ", count="
                                            + array.toString());
                        }
                        if (count > -1) {
                            if (request.getType() == PgClientRequest.REQ_TYPE_BATCH) {
                                lastResult.increBatchEffectCount(((PgReqBatch) request).sqls.length, count);
                            } else {
                                lastResult.increUpdateEffectCount(count);
                            }
                            if (count == 0 && request.getType() == PgClientRequest.REQ_TYPE_EXTEND_QUERY) {
                                if (((PgReqExtended) request).mode == PgExtendMode.FINDS_ENTITY) {
                                    lastResult.addEntity(null);
                                } else {
                                    lastResult.addRowData(null);
                                }
                            }
                        }
                        if (buffer.position() != bufpos + length) {
                            logger.log(
                                    Level.SEVERE,
                                    "[" + Times.nowMillis() + "] ["
                                            + Thread.currentThread().getName() + "]: " + conn + ", C buffer-currpos : "
                                            + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                            buffer.position(bufpos + length);
                        }
                    } else {
                        buffer.position(bufpos + length);
                    }
                } else if (cmd == MESSAGE_TYPE_PARAMETER_DESCRIPTION) { // 't' ParamDesc
                    if (request != null) {
                        if (lastResult == null) {
                            lastResult = pollResultSet(request);
                        }
                        PgPrepareDesc prepareDesc =
                                request.isExtendType() ? conn.getPgPrepareDesc(((PgReqExtended) request).sql) : null;
                        PgColumnFormat[] oldParamFormats = prepareDesc == null ? null : prepareDesc.paramFormats();
                        if (prepareDesc != null && oldParamFormats == null) {
                            PgColumnFormat[] paramFormats = PgRespParamDescDecoder.instance.read(
                                    conn, buffer, length, array, request, lastResult);
                            prepareDesc.updateParamFormats(paramFormats);
                            if (buffer.position() != bufpos + length) {
                                logger.log(
                                        Level.SEVERE,
                                        "[" + Times.nowMillis() + "] ["
                                                + Thread.currentThread().getName() + "]: " + conn
                                                + ", t buffer-currpos : " + buffer.position() + ", startpos=" + bufpos
                                                + ", length=" + length);
                                buffer.position(bufpos + length);
                            }
                        } else {
                            buffer.position(bufpos + length);
                        }
                    } else {
                        buffer.position(bufpos + length);
                    }
                } else if (cmd == MESSAGE_TYPE_ROW_DESCRIPTION) { // 'T' RowDesc
                    if (request != null) {
                        if (lastResult == null) {
                            lastResult = pollResultSet(request);
                        }
                        PgPrepareDesc prepareDesc =
                                request.isExtendType() ? conn.getPgPrepareDesc(((PgReqExtended) request).sql) : null;
                        PgRowDesc oldrowDesc = prepareDesc == null ? null : prepareDesc.getRowDesc();
                        if (oldrowDesc == null) {
                            PgRowDesc rowDesc = PgRespRowDescDecoder.instance.read(
                                    conn, buffer, length, array, request, lastResult);
                            lastResult.setRowDesc(rowDesc);
                            if (prepareDesc != null) {
                                prepareDesc.updateRowDesc(rowDesc);
                            }
                            if (buffer.position() != bufpos + length) {
                                logger.log(
                                        Level.SEVERE,
                                        "[" + Times.nowMillis() + "] ["
                                                + Thread.currentThread().getName() + "]: " + conn
                                                + ", T buffer-currpos : " + buffer.position() + ", startpos=" + bufpos
                                                + ", length=" + length);
                                buffer.position(bufpos + length);
                            }
                        } else {
                            lastResult.setRowDesc(oldrowDesc);
                            buffer.position(bufpos + length);
                        }
                    } else {
                        buffer.position(bufpos + length);
                    }
                } else if (cmd == MESSAGE_TYPE_DATA_ROW) { // 'D' RowData 一行数据
                    if (request != null) {
                        if (lastResult == null) {
                            lastResult = pollResultSet(request);
                        }
                        PgRowData rowData =
                                PgRespRowDataDecoder.instance.read(conn, buffer, length, array, request, lastResult);
                        if (rowData != PgRowData.NIL) {
                            lastResult.addRowData(rowData);
                        }
                        if (buffer.position() != bufpos + length) {
                            logger.log(
                                    Level.SEVERE,
                                    "[" + Times.nowMillis() + "] ["
                                            + Thread.currentThread().getName() + "]: " + conn + ", D buffer-currpos : "
                                            + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                            buffer.position(bufpos + length);
                        }
                    } else {
                        buffer.position(bufpos + length);
                    }
                } else if (cmd == MESSAGE_TYPE_ERROR_RESPONSE) { // 'E' Error
                    if (request != null) {
                        Exception exc =
                                PgRespErrorDecoder.instance.read(conn, buffer, length, array, request, lastResult);
                        if (lastExc == null) {
                            lastExc = exc; // 以第一个异常为准，例如insert多个，当表不存在prepare异常时，bind的异常不是表不存在异常。
                        }
                        if (buffer.position() != bufpos + length) {
                            logger.log(
                                    Level.SEVERE,
                                    "[" + Times.nowMillis() + "] ["
                                            + Thread.currentThread().getName() + "]: " + conn + ", E buffer-currpos : "
                                            + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                        }
                        if (request.isExtendType() && ((PgReqExtended) request).sendPrepare) {
                            conn.getPgPrepareDesc(((PgReqExtended) request).sql).uncomplete();
                        }
                        if (PgsqlDataSource.debug) {
                            logger.log(
                                    Level.FINEST,
                                    "[" + Times.nowMillis() + "] ["
                                            + Thread.currentThread().getName() + "]: " + conn + ", error=" + exc
                                            + ", request=" + request,
                                    exc);
                        }
                    }
                    buffer.position(bufpos + length);
                    if (!conn.isAuthenticated()) { // 登录失败的异常不会有ReadyForQuery消息体
                        halfFrameBytes = null;
                        halfFrameCmd = 0;
                        halfFrameLength = 0;
                        addMessage(request, lastExc);
                        lastResult = null;
                        lastExc = null;
                        lastCount = 0;
                        request = null;
                        break;
                    }
                } else if (cmd == MESSAGE_TYPE_READY_FOR_QUERY) { // 'Z' ReadyForQuery
                    lastCount++;
                    buffer.get(); // 'I' TransactionState.IDLE、'T' TransactionState.OPEN、'E' TransactionState.FAILED
                    // if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) logger.log(Level.FINEST, "[" +
                    // Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + " , " + request +
                    // "----------------------cmd:" + cmd);
                    if (lastResult != null) {
                        if (request.isExtendType()) { // PgReqExtended
                            PgReqExtended reqext = (PgReqExtended) request;
                            if (lastResult.rowDesc == null) {
                                PgPrepareDesc prepareDesc = conn.getPgPrepareDesc(reqext.sql);
                                if (prepareDesc != null) {
                                    lastResult.setRowDesc(prepareDesc.getRowDesc());
                                }
                            }
                        }
                        lastResult.effectRespCount++;
                    }
                    if (buffer.position() != bufpos + length) {
                        logger.log(
                                Level.SEVERE,
                                "[" + Times.nowMillis() + "] ["
                                        + Thread.currentThread().getName() + "]: " + conn + ", Z buffer-currpos : "
                                        + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                    }
                    buffer.position(bufpos + length);

                    if (request != null && lastCount < request.getSyncCount()) {
                        if (PgsqlDataSource.debug) {
                            logger.log(
                                    Level.FINEST,
                                    "[" + Times.nowMillis() + "] ["
                                            + Thread.currentThread().getName() + "]: " + conn
                                            + ", continue parse, lastCount=" + lastCount + ", syncCount="
                                            + request.getSyncCount());
                        }
                        continue;
                    }

                    halfFrameBytes = null;
                    halfFrameCmd = 0;
                    halfFrameLength = 0;
                    if (update1To2) {
                        return;
                    }
                    if (lastExc != null) {
                        addMessage(request, lastExc);
                    } else if (lastResult != null) {
                        addMessage(request, lastResult);
                        lastResult = null;
                    }
                    lastExc = null;
                    lastCount = 0;
                    request = null;
                    update1To2 = false;
                    break;
                } else if (cmd == MESSAGE_TYPE_PARSE_COMPLETE) { // '1' UPDATE命令会先发个1、t、n、Z再发2、C、Z,  也可能是1、t、n再发2、E、Z,
                    // 也可能是1、t、T、Z再发2、D...C/E、Z
                    // if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) logger.log(Level.FINEST, "[" +
                    // Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + " , " + request +
                    // "----------------------cmd:" + cmd);
                    update1To2 = true;
                    buffer.position(bufpos + length);
                } else if (cmd == MESSAGE_TYPE_BIND_COMPLETE) { // '2' UPDATE命令会先发个1、t、n、Z再发2、C、Z,  也可能是1、t、n再发2、E、Z,
                    // 也可能是1、t、T、Z再发2、D...C/E、Z
                    // if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) logger.log(Level.FINEST, "[" +
                    // Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + " , " + request +
                    // "----------------------cmd:" + cmd);
                    update1To2 = false;
                    buffer.position(bufpos + length);
                } else {
                    // if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_UPDATE) logger.log(Level.FINEST, "[" +
                    // Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + " , " + request +
                    // "----------------------cmd:" + cmd);
                    buffer.position(bufpos + length);
                }
            }
            buffer = realBuf;
        }
    }

    protected static String readUTF8String(ByteBuffer buffer, ByteArray array) {
        array.clear();
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            array.put(c);
            if (!buffer.hasRemaining()) {
                break;
            }
        }
        return array.toString(StandardCharsets.UTF_8);
    }

    protected static String readUTF8Base64String(ByteBuffer buffer, ByteArray array) {
        array.clear();
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            array.put(c);
            if (array.length() > 3 && c == '=' && buffer.hasRemaining() && buffer.get(buffer.position()) == 'R') {
                break;
            }
            if (!buffer.hasRemaining()) {
                break;
            }
        }
        return array.toString(StandardCharsets.UTF_8);
    }
}
