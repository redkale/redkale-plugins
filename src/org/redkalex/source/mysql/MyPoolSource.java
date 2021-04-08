/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Logger;
import org.redkale.net.*;
import org.redkale.source.PoolTcpSource;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class MyPoolSource extends PoolTcpSource {

    protected static final String CONN_ATTR_CURR_DBNAME = "CURR_DBNAME";

    protected static final byte[] CURRDBNAME_BYTES = "SELECT DATABASE()".getBytes();

    protected static final byte[] PING_BYTES = "SELECT 1".getBytes();

    public MyPoolSource(AsyncGroup asyncGroup, String rwtype, ArrayBlockingQueue queue, Semaphore semaphore, Properties prop, Logger logger) {
        super(asyncGroup, rwtype, queue, semaphore, prop, logger);
        if (this.encoding == null || this.encoding.isEmpty()) this.encoding = "UTF8MB4";
    }

    @Override
    protected ByteArray reqConnectBuffer(AsyncConnection conn) {
        return null;
    }

    protected static final String CONN_ATTR_PROTOCOL_VERSION = "PROTOCOL_VERSION";

    @Override
    protected void respConnectBuffer(final ByteBuffer buffer, CompletableFuture<AsyncConnection> future, AsyncConnection conn) {
        final ByteArray array = conn.getSubobject();
        //MySQLIO.doHandshake
        MyHandshakePacket handshakePacket = null;
        try {
            handshakePacket = new MyHandshakePacket(buffer, array);
        } catch (Exception ex) {
            conn.offerBuffer(buffer);
            conn.dispose();
            future.completeExceptionally(ex);
            return;
        }

        MyAuthPacket authPacket = new MyAuthPacket(handshakePacket, this.username, this.password, this.database);
        buffer.clear();
        authPacket.writeTo(buffer);
        buffer.flip();
        conn.write(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment1) {
                if (result < 0) {
                    failed(new RuntimeException("Write Buffer Error"), attachment1);
                    return;
                }
                buffer.clear();
                conn.setReadBuffer(buffer);
                conn.read(new CompletionHandler<Integer, ByteBuffer>() {
                    @Override
                    public void completed(Integer result, ByteBuffer attachment2) {
                        if (result < 0) {
                            failed(new RuntimeException("Read Buffer Error"), attachment2);
                            return;
                        }
                        attachment2.flip();
                        MyOKPacket okPacket = new MyOKPacket(-1, ByteBufferReader.create(buffer), array);
                        if (!okPacket.isOK()) {
                            conn.offerBuffer(buffer);
                            future.completeExceptionally(new SQLException(okPacket.toMessageString("MySQLOKPacket statusCode not success"), okPacket.sqlState));
                            conn.dispose();
                            return;
                        }
                        buffer.clear();
                        new MyQueryPacket(("SET NAMES " + encoding).getBytes()).writeTo(buffer);
                        buffer.flip();
                        conn.write(buffer, buffer, new CompletionHandler<Integer, ByteBuffer>() {
                            @Override
                            public void completed(Integer result, ByteBuffer attachment3) {
                                if (result < 0) {
                                    failed(new RuntimeException("Write Buffer Error"), attachment3);
                                    return;
                                }
                                buffer.clear();
                                conn.setReadBuffer(buffer);
                                conn.read(new CompletionHandler<Integer, ByteBuffer>() {
                                    @Override
                                    public void completed(Integer result, ByteBuffer attachment4) {
                                        if (result < 0) {
                                            failed(new SQLException("Read Buffer Error"), attachment4);
                                            return;
                                        }
                                        attachment4.flip();
                                        MyOKPacket okPacket = new MyOKPacket(-1, ByteBufferReader.create(attachment4), array);
                                        if (!okPacket.isOK()) {
                                            conn.offerBuffer(buffer);
                                            future.completeExceptionally(new SQLException(okPacket.toMessageString("MySQLOKPacket statusCode not success"), okPacket.sqlState));
                                            conn.dispose();
                                            return;
                                        }
                                        //完成了
                                        conn.offerBuffer(buffer);
                                        future.complete(conn);
                                    }

                                    @Override
                                    public void failed(Throwable exc, ByteBuffer attachment4) {
                                        conn.offerBuffer(attachment4);
                                        future.completeExceptionally(exc);
                                        conn.dispose();
                                    }
                                });
                            }

                            @Override
                            public void failed(Throwable exc, ByteBuffer attachment3) {
                                conn.offerBuffer(attachment3);
                                future.completeExceptionally(exc);
                                conn.dispose();
                            }

                        });

                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment2) {
                        conn.offerBuffer(attachment2);
                        future.completeExceptionally(exc);
                        conn.dispose();
                    }
                });
            }

            @Override
            public void failed(Throwable exc, Void attachment1) {
                conn.offerBuffer(buffer);
                conn.dispose();
                future.completeExceptionally(exc);
            }
        });
    }

    @Override
    protected int getDefaultPort() {
        return 3306;
    }

    @Override
    protected CompletableFuture<AsyncConnection> sendPingCommand(AsyncConnection conn) {
        final ByteBuffer buffer = ByteBuffer.allocate(8192);  //临时
        Mysqls.writeUB3(buffer, 1 + PING_BYTES.length);
        buffer.put((byte) 0x0);
        buffer.put(MyPacket.COM_QUERY);
        buffer.put(PING_BYTES);
        buffer.flip();
        CompletableFuture<AsyncConnection> future = new CompletableFuture<>();
        conn.write(buffer, buffer, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer result, ByteBuffer attachment) {
                if (result < 0) {
                    failed(new RuntimeException("Write Buffer Error"), attachment);
                    return;
                }
                buffer.clear();
                conn.setReadBuffer(buffer);
                conn.read(new CompletionHandler<Integer, ByteBuffer>() {
                    @Override
                    public void completed(Integer result, ByteBuffer attachment) {
                        if (result < 0) {
                            failed(new SQLException("Read Buffer Error"), attachment);
                            return;
                        }
//                        buffer.flip();
//                        final ByteBufferReader buffers = ByteBufferReader.create(buffer);
//                        int packetLength = Mysqls.readUB3(buffers);
//                        final byte[] array = conn.getAttribute(CONN_ATTR_BYTES_NAME);
//                        if (packetLength < 4) {
//                            MyColumnCountPacket countPacket = new MyColumnCountPacket(packetLength, buffers, array);
//                            System.out.println("PING, 字段数： " + countPacket.columnCount);
//                            System.out.println("--------- column desc start  -------------");
//                            MyColumnDescPacket[] colDescs = new MyColumnDescPacket[countPacket.columnCount];
//                            for (int i = 0; i < colDescs.length; i++) {
//                                colDescs[i] = new MyColumnDescPacket(buffers, array);
//                            }
//                            MyEOFPacket eofPacket = new MyEOFPacket(-1, buffers, array);
//                            System.out.println("字段描述EOF包： " + eofPacket);
//                        } else {
//                            MyOKPacket okPacket = new MyOKPacket(packetLength, buffers, array);
//                            System.out.println("PING的结果 ： " + okPacket);
//                        }
                        conn.offerBuffer(buffer);
                        future.complete(conn);
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment) {
                        conn.offerBuffer(buffer);
                        conn.dispose();
                        future.completeExceptionally(exc);
                    }

                });
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                conn.offerBuffer(buffer);
                conn.dispose();
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    @Override
    protected CompletableFuture<AsyncConnection> sendCloseCommand(AsyncConnection conn) {
        final ByteBuffer buffer = ByteBuffer.allocate(8192);  //临时
        Mysqls.writeUB3(buffer, 1);
        buffer.put((byte) 0x0);
        buffer.put(MyPacket.COM_QUIT);
        buffer.flip();
        CompletableFuture<AsyncConnection> future = new CompletableFuture<>();
        conn.write(buffer, buffer, new CompletionHandler<Integer, ByteBuffer>() {
            @Override
            public void completed(Integer result, ByteBuffer attachment) {
                if (result < 0) {
                    failed(new RuntimeException("Write Buffer Error"), attachment);
                    return;
                }
                buffer.clear();
                conn.offerBuffer(buffer);
                future.complete(conn);
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                conn.offerBuffer(buffer);
                conn.dispose();
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

}
