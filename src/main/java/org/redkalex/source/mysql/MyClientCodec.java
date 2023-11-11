/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.logging.*;
import org.redkale.net.client.*;
import org.redkale.util.*;
import static org.redkalex.source.mysql.MyClientRequest.*;
import static org.redkalex.source.mysql.MyRespDecoder.readOKPacket;
import static org.redkalex.source.mysql.MyResultSet.*;
import static org.redkalex.source.mysql.Mysqls.*;

/**
 *
 * @author zhangjx
 */
public class MyClientCodec extends ClientCodec<MyClientRequest, MyResultSet> {

    protected static final Logger logger = Logger.getLogger(MyClientCodec.class.getSimpleName());

    protected int halfFrameLength;

    protected ByteArray halfFrameBytes;

    private ByteArray recyclableArray;

    MyResultSet lastResult = null;

    Throwable lastExc = null;

    public MyClientCodec(ClientConnection connection) {
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

    @Override
    public void decodeMessages(final ByteBuffer realbuf, ByteArray array) {
        MyClientConnection conn = (MyClientConnection) connection;
        if (!realbuf.hasRemaining()) {
            return;
        }
        ByteBuffer buffer = realbuf;
        //logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", realbuf读到的长度: " + realbuf.remaining() + ", halfFrameBytes=" + (halfFrameBytes == null ? -1 : halfFrameBytes.length()));
        if (halfFrameBytes != null) {  //存在半包
            int remain = realbuf.remaining();
            if (halfFrameLength == 0) {
                if (remain + halfFrameBytes.length() < 3) {//还是不足以读取length
                    halfFrameBytes.put(realbuf);
                    return;
                }
                while (halfFrameBytes.length() < 3) halfFrameBytes.put(realbuf.get());
                halfFrameLength = Mysqls.readUB3(halfFrameBytes, 0) + 4;
            }
            //此时必然有halfFrameLength
            int bulen = halfFrameLength - halfFrameBytes.length(); //还差多少字节
            if (bulen > remain) { //不够，继续读取
                halfFrameBytes.put(realbuf);
                return;
            }
            halfFrameBytes.put(realbuf, bulen);
            //此时halfFrameBytes是完整的frame数据            
            buffer = ByteBuffer.wrap(halfFrameBytes.content(), 0, halfFrameBytes.length());
            halfFrameBytes = null;
            halfFrameLength = 0;
        } else if (realbuf.remaining() < 4) { //第一次就只有几个字节buffer
            halfFrameBytes = pollArray();
            halfFrameBytes.put(realbuf);
            return;
        }
        //buffer必然包含一个完整的frame数据
        MyClientRequest request = null;
        while (buffer.hasRemaining()) {
            while (buffer.hasRemaining()) {
                if (request == null) {
                    request = nextRequest();
                }
                if (buffer.remaining() < 3) { //不足以读取length
                    halfFrameBytes = pollArray();
                    halfFrameBytes.put(buffer);
                    break;
                }
                final int length = Mysqls.readUB3(buffer); //all:78, length:74, remain:75(index:1,body:74)
                if (buffer.remaining() < length + 1) {
                    if (halfFrameBytes == null) {
                        halfFrameBytes = pollArray();
                    } else {
                        halfFrameBytes.clear();
                    }
                    halfFrameLength = length + 4;
                    Mysqls.writeUB3(halfFrameBytes, length);
                    halfFrameBytes.put(buffer);
                    return;
                }
                final byte index = buffer.get();
                final int bufpos = buffer.position();
                if ((request == null || request.isVirtualType()) && !conn.isAuthenticated()) {  //首次通讯
                    //logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", length=" + length + ", index=" + index + ", remains=" + buffer.remaining());
                    try {
                        conn.handshake = MyRespHandshakeDecoder.instance.read(conn, buffer, length, index, array, request, null);
                        lastResult = conn.handshake;
                    } catch (Exception e) {
                        lastExc = e;
                    }
                    halfFrameBytes = null;
                    halfFrameLength = 0;
                    if (lastExc != null) {
                        addMessage(request, lastExc);
                    } else if (lastResult != null) {
                        addMessage(request, lastResult);
                        lastResult = null;
                    }
                    lastExc = null;
                    request = null;
                    break;
                }
                final int typeid = lastResult != null && lastResult.status != 0 ? 0xfff : buffer.get() & 0xff;
                final int reqType = request == null ? 0 : request.getType();
                if (MysqlDataSource.debug && conn.isAuthenticated()) {
                    logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", length=" + length + ", index=" + index + ", typeid=0x" + Integer.toHexString(typeid) + ", status=" + (lastResult == null ? -1 : lastResult.status) + ", position=" + buffer.position() + ", remains=" + buffer.remaining());
                }

                if (typeid == TYPE_ID_ERROR) {
                    if ((reqType & 0x1) > 0 && ((MyReqExtended) request).sendPrepare) {
                        conn.getPrepareFlag(((MyReqExtended) request).sql).set(false);
                        //conn.resumeWrite();
                    }
                    lastExc = MyRespDecoder.readErrorPacket(conn, buffer, length, index, array);
                    //if (!conn.autoddl()) logger.log(Level.WARNING, "mysql.request = " + request, lastExc);
                } else {
                    if (lastResult == null && reqType != REQ_TYPE_AUTH) {
                        lastResult = conn.pollResultSet(request == null ? null : request.info);
                    }
                    if (reqType == REQ_TYPE_AUTH) { //登录
                        MyRespAuthResultSet authResult = new MyRespAuthResultSet();
                        if (length < 3) {
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", buffernot enough, continue read, remain=" + buffer.remaining());
                            }
                            buffer.position(bufpos + length);
                            continue;
                        }
                        if (typeid == 0xFE) {  //AUTH_SWITCH_REQUEST_STATUS_FLAG
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", length=" + length + ", index=" + index + ", typeid=0x" + Integer.toHexString(typeid) + ", position=" + buffer.position() + ", remains=" + buffer.remaining());
                            }
                            String pluginName = Mysqls.readASCIIString(buffer, array);
                            byte[] nonce = new byte[20]; //NONCE_LENGTH
                            buffer.get(nonce);
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", length=" + length + ", index=" + index + ", typeid=0x" + Integer.toHexString(typeid) + ", pluginName=" + pluginName + ", position=" + buffer.position() + ", remains=" + buffer.remaining());
                            }
                            authResult.authSwitch = new MyReqAuthentication.MyReqAuthSwitch(pluginName, nonce, ((MyReqAuthentication) request).password);
                        } else if (typeid == 0x1) { //AUTH_MORE_DATA_STATUS_FLAG
                            //SSL暂未实现
                            lastExc = new SQLException("Not support auth method, typeid: " + typeid);
                        } else if (typeid == 0x0) {
                            authResult.authOK = true;
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", auth OK! remain = " + buffer.remaining());
                            }
                        } else {
                            lastExc = new SQLException("Not support auth method, typeid: " + typeid);
                        }
                        if (lastResult == null && lastExc == null) {
                            lastResult = authResult;
                        }
                        buffer.position(bufpos + length);
                        if (MysqlDataSource.debug && conn.isAuthenticated()) {
                            logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", authResult=" + authResult);
                        }
                    } else if (reqType == REQ_TYPE_UPDATE || reqType == REQ_TYPE_INSERT || reqType == REQ_TYPE_DELETE || reqType == REQ_TYPE_BATCH) {
                        //简单更新 开始
                        MyRespOK ok = readOKPacket(conn, buffer, length, index, array);
                        if (reqType == REQ_TYPE_BATCH) {
                            lastResult.increBatchEffectCount(((MyReqBatch) request).sqls.length, (int) ok.affectedRows);
                        } else {
                            lastResult.increUpdateEffectCount((int) ok.affectedRows);
                        }
                        if (MysqlDataSource.debug && buffer.position() != bufpos + length) {
                            logger.log(Level.FINER, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", request=" + request + ", buffer-currpos: " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                        }
                        buffer.position(bufpos + length);
                        if (MysqlDataSource.debug && conn.isAuthenticated()) {
                            logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", affectedRows=" + ok.affectedRows + ", statusFlags=" + ok.serverStatusFlags + ", warningCount=" + ok.warningCount);
                        }
                        if (reqType == REQ_TYPE_BATCH) {
                            lastResult.effectRespCount++;
                            if (lastResult.effectRespCount < ((MyReqBatch) request).sqls.length) {
                                continue;
                            }
                        }
                        //简单更新 结束
                    } else if (reqType == REQ_TYPE_QUERY) {
                        //简单查询 开始
                        if (lastResult.status == STATUS_QUERY_ROWDESC) {
                            lastResult.rowDesc.columns[lastResult.rowColumnDecodeIndex] = MyRespRowColumnDecoder.instance.read(conn, buffer, length, index, array, request, lastResult);
                            buffer.position(bufpos + length);
                            //logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", column = " + lastResult.rowDesc.columns[lastResult.rowColumnDecodeIndex]);
                            if (++lastResult.rowColumnDecodeIndex == lastResult.rowDesc.length()) {
                                lastResult.status = STATUS_QUERY_ROWDATA;
                                lastResult.rowColumnDecodeIndex = -1;
                            }
                            continue;
                        } else if (lastResult.status == STATUS_QUERY_ROWDATA) {
                            MyRowData row = MyRespRowDataRowDecoder.instance.read(conn, buffer, length, index, array, request, lastResult);
                            //row = null 表示数据读取完毕
                            if (row != null) {
                                lastResult.addRowData(row);
                            }
                            if (buffer.position() != bufpos + length) {
                                logger.log(Level.SEVERE, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", rowdata buffer-currpos : " + buffer.position() + ", unread-byte-len=" + (bufpos + length - buffer.position()) + ", startpos=" + bufpos + ", length=" + length);
                            }
                            buffer.position(bufpos + length);
                            if (row != null) {
                                continue;
                            }
                            lastResult.status = 0; //row全部读取完毕
                        } else {
                            if (typeid == TYPE_ID_LOCAL_INFILE) {
                                buffer.position(bufpos + length);
                                lastExc = new SQLException("Not support LOCAL INFILE");
                            } else if (typeid == TYPE_ID_OK) {
                                MyRespOK ok = readOKPacket(conn, buffer, length, index, array);
                                lastResult.increUpdateEffectCount((int) ok.affectedRows);
                                if (buffer.position() != bufpos + length) {
                                    logger.log(Level.SEVERE, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", buffer-currpos : " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                                }
                                buffer.position(bufpos + length);
                            } else {
                                int columnCount = (int) Mysqls.readLength(typeid, buffer);
                                lastResult.rowDesc = new MyRowDesc((new MyRowColumn[columnCount]));
                                lastResult.status = STATUS_QUERY_ROWDESC;
                                lastResult.rowColumnDecodeIndex = 0;
                                if (buffer.position() != bufpos + length) {
                                    logger.log(Level.SEVERE, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", buffer-currpos : " + buffer.position() + ", startpos=" + bufpos + ", length=" + length);
                                }
                                buffer.position(bufpos + length);
                                continue;
                            }
                        }
                        //简单查询 结束
                    } else if ((reqType & 0x1) > 0) {
                        //REQ_TYPE_EXTEND_XXX 开始
                        final MyReqExtended reqext = (MyReqExtended) request;
                        if (lastResult.status == STATUS_PREPARE_PARAM) {
                            MyRespPrepare prepare = lastResult.prepare;
                            MyRowColumn column = MyRespRowColumnDecoder.instance.read(conn, buffer, length, index, array, request, lastResult);
                            buffer.position(bufpos + length);
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", paramColumn = " + column);
                            }

                            prepare.paramDescs.columns[prepare.paramDecodeIndex++] = column;
                            if (prepare.paramDecodeIndex < prepare.paramDescs.columns.length) {
                                continue;
                            }
                            if (prepare.columnDescs != null) {
                                lastResult.status = STATUS_PREPARE_COLUMN;
                                prepare.columnDecodeIndex = 0;
                                if (MysqlDataSource.debug) {
                                    logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 进入 STATUS_PREPARE_COLUMN");
                                }
                                continue;
                            }
                            //prepare结束
                            lastResult.status = 0;
                            conn.putPrepareDesc(reqext.sql, new MyPrepareDesc(prepare));
                            conn.putStatementIndex(reqext.sql, prepare.statementId);
                            //conn.resumeWrite();
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", sendBindCount = " + reqext.getBindCount() + ", 解析完paramColumn后缓存MyPrepareDesc = " + prepare);
                            }
                            if (reqext.getBindCount() > 0) {
                                continue;
                            }
                        } else if (lastResult.status == STATUS_PREPARE_COLUMN) {
                            MyRespPrepare prepare = lastResult.prepare;
                            MyRowColumn column = MyRespRowColumnDecoder.instance.read(conn, buffer, length, index, array, request, lastResult);
                            buffer.position(bufpos + length);
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", columnColumn = " + column);
                            }

                            prepare.columnDescs.columns[prepare.columnDecodeIndex++] = column;
                            if (prepare.columnDecodeIndex < prepare.columnDescs.columns.length) {
                                continue;
                            }
                            //prepare结束
                            lastResult.status = 0;
                            conn.putPrepareDesc(reqext.sql, new MyPrepareDesc(prepare));
                            conn.putStatementIndex(reqext.sql, prepare.statementId);
                            //conn.resumeWrite();
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 解析完columnColumn后缓存MyPrepareDesc = " + prepare + "\r\n");
                            }
                            if (reqext.getBindCount() > 0) {
                                continue;
                            }
                        } else if (lastResult.status == STATUS_EXTEND_COLUMN) {
                            MyRowColumn column = MyRespRowColumnDecoder.instance.read(conn, buffer, length, index, array, request, lastResult);
                            buffer.position(bufpos + length);
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", extendColumn = " + column);
                            }
                            MyRowColumn[] columns = lastResult.rowDesc.columns;
                            columns[lastResult.rowColumnDecodeIndex++] = column;
                            if (lastResult.rowColumnDecodeIndex >= columns.length) {
                                lastResult.status = STATUS_EXTEND_ROWDATA;  //extend-column 结束
                            }
                            continue;
                        } else if (lastResult.status == STATUS_EXTEND_ROWDATA) {
                            MyRowData row = MyRespRowDataRowDecoder.instance.read(conn, buffer, length, index, array, request, lastResult);
                            //row = null 表示数据读取完毕
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 读取到一条RowData: " + row);
                            }
                            if (row != null) {
                                lastResult.addRowData(row);
                            }
                            if (buffer.position() != bufpos + length) {
                                logger.log(Level.SEVERE, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", rowdata buffer-currpos : " + buffer.position() + ", unread-byte-len=" + (bufpos + length - buffer.position()) + ", startpos=" + bufpos + ", length=" + length);
                            }
                            buffer.position(bufpos + length);
                            if (row != null) {
                                continue;
                            }
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", rowData 读取完毕. ");
                            }
                            lastResult.status = 0; //row全部读取完毕
                            lastResult.effectRespCount++;
                            if (reqext.finds && lastResult.rowData.size() < lastResult.effectRespCount) {
                                lastResult.addRowData(null);
                            }
                            if (lastResult.effectRespCount < reqext.getBindCount()) {
                                continue;
                            }
                        } else if (reqext.sendPrepare && lastResult.status == 0) { //Prepare第一个响应包
                            if (length == 7) {
                                MyRespOK ok = MyRespDecoder.readOKPacket(conn, buffer, length, index, array);
                                if (buffer.position() != bufpos + length) {
                                    logger.log(Level.SEVERE, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", prepareok buffer-currpos: " + buffer.position() + ", unread-byte-len=" + (bufpos + length - buffer.position()) + ", startpos=" + bufpos + ", length=" + length);
                                }
                                buffer.position(bufpos + length);
                                if (MysqlDataSource.debug) {
                                    logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 读到Prepare第一个响应包");
                                }

                                continue;
                            }
                            MyRespPrepare prepare = MyRespPrepareDecoder.instance.read(conn, buffer, length, index, array, request, lastResult);
                            lastResult.prepare = prepare;
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", request.sql =" + reqext.sql + ", prepare = " + prepare);
                            }
                            if (prepare.paramDecodeIndex >= 0) {
                                lastResult.status = STATUS_PREPARE_PARAM;
                            } else if (prepare.columnDecodeIndex >= 0) {
                                lastResult.status = STATUS_PREPARE_COLUMN;
                            }
                            if (buffer.position() != bufpos + length) {
                                logger.log(Level.SEVERE, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", sendPrepare buffer-currpos : " + buffer.position() + ", unread-byte-len=" + (bufpos + length - buffer.position()) + ", startpos=" + bufpos + ", length=" + length);
                            }
                            buffer.position(bufpos + length);
                            if (lastResult.status > 0) {
                                continue;
                            }
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 居然没有 lastResult.status");
                            }
                        } else if (lastResult.status == 0 && reqType == REQ_TYPE_EXTEND_QUERY) { //Extended-Query第一个响应包
                            buffer.position(buffer.position() - 1); //回退typeid
                            int columnCount = (int) Mysqls.readLength(buffer);
                            if (MysqlDataSource.debug) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", RowDesc预编译查询column数量: " + columnCount);
                            }
                            lastResult.rowDesc = new MyRowDesc(new MyRowColumn[columnCount]);
                            lastResult.rowColumnDecodeIndex = 0;
                            lastResult.status = columnCount > 0 ? STATUS_EXTEND_COLUMN : STATUS_EXTEND_ROWDATA;
                            continue;
                        } else { //REQ_TYPE_EXTEND_INSERT/UPDATE/DELETE的RowData
                            MyRespOK ok = MyRespDecoder.readOKPacket(conn, buffer, length, index, array);
                            lastResult.updateEffectCount += (int) ok.affectedRows;
                            lastResult.effectRespCount++;
                            if (reqType == REQ_TYPE_EXTEND_UPDATE && buffer.position() != bufpos + length) {
                                logger.log(Level.SEVERE, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", rowData buffer-currpos : " + buffer.position() + ", unread-byte-len=" + (bufpos + length - buffer.position()) + ", startpos=" + bufpos + ", length=" + length);
                            }
                            buffer.position(bufpos + length);
                            if (MysqlDataSource.debug && conn.isAuthenticated()) {
                                logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 增删改结果: affectedRows=" + ok.affectedRows + ", statusFlags=" + ok.serverStatusFlags + ", warningCount=" + ok.warningCount);
                            }
                            if (lastResult.effectRespCount < reqext.getBindCount()) {
                                continue;
                            }
                        }
                        //REQ_TYPE_EXTEND_XXX 结束
                    } else {
                        if (MysqlDataSource.debug) {
                            logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 未能识别的请求类型: request = " + request);
                        }
                        buffer.position(bufpos + length);
                    }
                }
                if (buffer.position() != bufpos + length) {
                    logger.log(Level.SEVERE, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", buffer-currpos : " + buffer.position() + ", unread-byte-len=" + (bufpos + length - buffer.position()) + ", startpos=" + bufpos + ", length=" + length);
                }
                halfFrameBytes = null;
                halfFrameLength = 0;
                if (lastExc != null) {
                    addMessage(request, lastExc);
                    if (MysqlDataSource.debug) {
                        logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 返回结果异常 = " + lastExc);
                    }
                } else if (lastResult != null) {
                    addMessage(request, lastResult);
                    if (MysqlDataSource.debug) {
                        logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 返回结果 = " + lastResult + ", request = " + (request == null ? null : request.toSimpleString()));
                    }
                    lastResult = null;
                } else {
                    if (MysqlDataSource.debug) {
                        logger.log(Level.FINEST, "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", 返回无结果");
                    }
                }
                lastExc = null;
                request = null;
                break;
            }
            buffer = realbuf;
        }
    }

}
