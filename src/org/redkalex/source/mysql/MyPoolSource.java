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
import org.redkale.net.AsyncConnection;
import org.redkale.source.PoolTcpSource;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class MyPoolSource extends PoolTcpSource {

    protected static final String CONN_ATTR_BYTES_NAME = "BYTES_NAME";

    protected static final byte[] PING_BYTES = "SELECT 1".getBytes();

    public MyPoolSource(String rwtype, ArrayBlockingQueue queue, Semaphore semaphore, Properties prop,
        Logger logger, ObjectPool<ByteBuffer> bufferPool, ThreadPoolExecutor executor) {
        super(rwtype, queue, semaphore, prop, logger, bufferPool, executor);
        if (this.encoding == null || this.encoding.isEmpty()) this.encoding = "UTF8MB4";
    }

    @Override
    protected ByteBuffer reqConnectBuffer(AsyncConnection conn) {
        if (conn.getAttribute(CONN_ATTR_BYTES_NAME) == null) conn.setAttribute(CONN_ATTR_BYTES_NAME, new byte[1024]);
        return null;
    }

    protected static final String CONN_ATTR_PROTOCOL_VERSION = "PROTOCOL_VERSION";

    @Override
    protected void respConnectBuffer(final ByteBuffer buffer, CompletableFuture<AsyncConnection> future, AsyncConnection conn) {
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTES_NAME);

        //MySQLIO.doHandshake
        MyHandshakePacket handshakePacket = null;
        try {
            handshakePacket = new MyHandshakePacket(buffer, bytes);
        } catch (Exception ex) {
            bufferPool.accept(buffer);
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
                if (buffer.hasRemaining()) {
                    conn.write(buffer, attachment1, this);
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
                        MyOKPacket okPacket = new MyOKPacket(-1, ByteBufferReader.create(buffer), bytes);
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
                                if (buffer.hasRemaining()) {
                                    conn.write(buffer, attachment3, this);
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
                                        MyOKPacket okPacket = new MyOKPacket(-1, ByteBufferReader.create(attachment4), bytes);
                                        if (!okPacket.isOK()) {
                                            conn.offerBuffer(buffer);
                                            future.completeExceptionally(new SQLException(okPacket.toMessageString("MySQLOKPacket statusCode not success"), okPacket.sqlState));
                                            conn.dispose();
                                            return;
                                        }
                                        //完成了
                                        bufferPool.accept(buffer);
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
                bufferPool.accept(buffer);
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
        final ByteBuffer buffer = bufferPool.get();
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
                if (buffer.hasRemaining()) {
                    conn.write(buffer, attachment, this);
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
                        bufferPool.accept(buffer);
                        future.complete(conn);
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment) {
                        bufferPool.accept(buffer);
                        conn.dispose();
                        future.completeExceptionally(exc);
                    }

                });
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                bufferPool.accept(buffer);
                conn.dispose();
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    @Override
    protected CompletableFuture<AsyncConnection> sendCloseCommand(AsyncConnection conn) {
        final ByteBuffer buffer = bufferPool.get();
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
                if (buffer.hasRemaining()) {
                    conn.write(buffer, attachment, this);
                    return;
                }
                buffer.clear();
                bufferPool.accept(buffer);
                future.complete(conn);
            }

            @Override
            public void failed(Throwable exc, ByteBuffer attachment) {
                bufferPool.accept(buffer);
                conn.dispose();
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

}
