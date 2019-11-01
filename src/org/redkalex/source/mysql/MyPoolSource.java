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

    protected static final String CONN_ATTR_BYTESBAME = "BYTESBAME";

    public MyPoolSource(String rwtype, ArrayBlockingQueue queue, Semaphore semaphore, Properties prop,
        Logger logger, ObjectPool<ByteBuffer> bufferPool, ThreadPoolExecutor executor) {
        super(rwtype, queue, semaphore, prop, logger, bufferPool, executor);
    }

    @Override
    protected ByteBuffer reqConnectBuffer(AsyncConnection conn) {
        if (conn.getAttribute(CONN_ATTR_BYTESBAME) == null) conn.setAttribute(CONN_ATTR_BYTESBAME, new byte[1024]);
        return null;
    }

    protected static final String CONN_ATTR_PROTOCOL_VERSION = "PROTOCOL_VERSION";

    @Override
    protected void respConnectBuffer(final ByteBuffer buffer, CompletableFuture<AsyncConnection> future, AsyncConnection conn) {
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);

        //MySQLIO.doHandshake
        MySQLHandshakePacket handshakePacket = null;
        try {
            handshakePacket = new MySQLHandshakePacket(buffer, bytes);
        } catch (Exception ex) {
            bufferPool.accept(buffer);
            conn.dispose();
            future.completeExceptionally(ex);
            return;
        }

        MySQLAuthPacket authPacket = new MySQLAuthPacket(handshakePacket, this.username, this.password, this.database);
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
                        MySQLOKPacket okPacket = new MySQLOKPacket(buffer, bytes);
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
    protected CompletableFuture<AsyncConnection> sendCloseCommand(AsyncConnection conn) {
        return null;
    }

}
