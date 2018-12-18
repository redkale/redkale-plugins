/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Logger;
import org.redkale.net.AsyncConnection;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkalex.source.pgsql.PgSQLDataSource.*;

/**
 *
 * @author zhangjx
 */
public class PgPoolSource extends PoolTcpSource {

    protected static final String CONN_ATTR_BYTESBAME = "BYTESBAME";

    public PgPoolSource(String rwtype, ArrayBlockingQueue queue, Semaphore semaphore, Properties prop, Logger logger, ObjectPool<ByteBuffer> bufferPool, ThreadPoolExecutor executor) {
        super(rwtype, queue, semaphore, prop, logger, bufferPool, executor);
    }

    @Override
    protected ByteBuffer reqConnectBuffer(AsyncConnection conn) {
        if (conn.getAttribute(CONN_ATTR_BYTESBAME) == null) conn.setAttribute(CONN_ATTR_BYTESBAME, new byte[255]);
        final ByteBuffer buffer = bufferPool.get();
        {
            buffer.putInt(0);
            buffer.putInt(196608);
            writeUTF8String(writeUTF8String(buffer, "user"), username);
            writeUTF8String(writeUTF8String(buffer, "database"), database);
            writeUTF8String(writeUTF8String(buffer, "client_encoding"), "UTF8");

            buffer.put((byte) 0);
            buffer.putInt(0, buffer.position());
        }
        buffer.flip();
        return buffer;
    }

    @Override
    protected void respConnectBuffer(final ByteBuffer buffer, CompletableFuture<AsyncConnection> future, AsyncConnection conn) {
        char cmd = (char) buffer.get();
        int length = buffer.getInt();
        if (cmd == 'R') {
            int type = buffer.getInt();
            if (type == 0) {
                //认证通过     
            } else if (type == 3 || type == 5) {//3:需要密码; 5:需要salt密码
                byte[] salt = null;
                if (type == 5) {
                    salt = new byte[4];
                    buffer.get(salt);
                }
                buffer.clear();
                { //密码验证
                    buffer.put((byte) 'p');
                    buffer.putInt(0);
                    if (salt == null) {
                        buffer.put(password.getBytes(StandardCharsets.UTF_8));
                    } else {
                        MessageDigest md5;
                        try {
                            md5 = MessageDigest.getInstance("MD5");
                        } catch (NoSuchAlgorithmException e) {
                            bufferPool.accept(buffer);
                            future.completeExceptionally(e);
                            return;
                        }
                        md5.update(password.getBytes(StandardCharsets.UTF_8));
                        md5.update(username.getBytes(StandardCharsets.UTF_8));
                        md5.update(Utility.binToHexString(md5.digest()).getBytes(StandardCharsets.UTF_8));
                        md5.update(salt);
                        buffer.put((byte) 'm');
                        buffer.put((byte) 'd');
                        buffer.put((byte) '5');
                        buffer.put(Utility.binToHexString(md5.digest()).getBytes(StandardCharsets.UTF_8));
                    }
                    buffer.put((byte) 0);
                    buffer.putInt(1, buffer.position() - 1);
                }
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
                                respConnectBuffer(attachment2, future, conn);
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
                        future.completeExceptionally(exc);
                        conn.dispose();
                    }
                });
                return;
            } else {
                bufferPool.accept(buffer);
                future.completeExceptionally(new SQLException("postgres connect error"));
                conn.dispose();
                return;
            }
            cmd = (char) buffer.get();
            length = buffer.getInt();
        }
        while (cmd != 'E' && cmd != 'Z') {
            buffer.position(buffer.position() + length - 4);
            cmd = (char) buffer.get();
            length = buffer.getInt();
        }
        if (cmd == 'E') { //异常了
            byte[] field = new byte[255];
            String level = null, code = null, message = null;
            for (byte type = buffer.get(); type != 0; type = buffer.get()) {
                String value = readUTF8String(buffer, field);
                if (type == (byte) 'S') {
                    level = value;
                } else if (type == 'C') {
                    code = value;
                } else if (type == 'M') {
                    message = value;
                }
            }
            bufferPool.accept(buffer);
            future.completeExceptionally(new SQLException(message, code, 0));
            conn.dispose();
            return;
        }
        if (cmd == 'Z') { //ReadyForQuery
            bufferPool.accept(buffer);
            future.complete(conn);
            return;
        }
        bufferPool.accept(buffer);
        future.completeExceptionally(new SQLException("postgres connect resp error"));
        conn.dispose();
    }

    @Override
    protected int getDefaultPort() {
        return 5432;
    }

    @Override
    protected CompletableFuture<AsyncConnection> sendCloseCommand(AsyncConnection conn) {
        return null;
//        { // CLOSE
//            buffer.put((byte) 'C');
//            buffer.putInt(4 + 1 + 1);
//            buffer.put((byte) 'S');
//            buffer.put((byte) 0);
//        }
    }

}
