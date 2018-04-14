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

/**
 *
 * @author zhangjx
 */
public class PgPoolSource extends PoolTcpSource {

    public PgPoolSource(String rwtype, Properties prop, Logger logger, ObjectPool<ByteBuffer> bufferPool, ThreadPoolExecutor executor) {
        super(rwtype, prop, logger, bufferPool, executor);
    }

    @Override
    public void change(Properties property) {
    }

    @Override
    protected ByteBuffer reqConnectBuffer() {
        final ByteBuffer buffer = bufferPool.get();
        { //StartupMessage
            buffer.putInt(0);
            buffer.putInt(196608); //benchmarkdbuser  benchmarkdbpass hello_world

            putCString(putCString(buffer, "user"), user);
            putCString(putCString(buffer, "database"), defdb);
            putCString(putCString(buffer, "client_encoding"), "UTF8");

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
        System.out.println("---------------cmd----------" + cmd);
        if (cmd == 'R') {
            int type = buffer.getInt();
            if (type == 0) { //认证通过     
                System.out.println("---------------通过认证了----------");

            } else if (type == 3 || type == 5) {//3:需要密码; 5:需要salt密码
                System.out.println("---------------需要密码认证----------" + type);
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
                        md5.update(user.getBytes(StandardCharsets.UTF_8));
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
                        conn.read(buffer, null, new CompletionHandler<Integer, Void>() {
                            @Override
                            public void completed(Integer result, Void attachment2) {
                                if (result < 0) {
                                    failed(new RuntimeException("Read Buffer Error"), attachment2);
                                    return;
                                }
                                buffer.flip();
                                respConnectBuffer(buffer, future, conn);
                            }

                            @Override
                            public void failed(Throwable exc, Void attachment2) {
                                bufferPool.accept(buffer);
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
            }
        }
        cmd = (char) buffer.get();
        length = buffer.getInt();
        System.out.println("xxxxxx55555xxxxxxxxxx " + cmd);
        while (cmd != 'E' && cmd != 'Z') {
            buffer.position(buffer.position() + length - 4);
            cmd = (char) buffer.get();
            System.out.println("xxxxxxxxxxxxxxxx " + cmd);
            length = buffer.getInt();
        }
        if (cmd == 'E') { //异常了
            byte[] field = new byte[255];
            String level = null, code = null, message = null;
            for (byte type = buffer.get(); type != 0; type = buffer.get()) {
                String value = getCString(buffer, field);
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
            System.out.println("---------------可以进行查询了----------");
            bufferPool.accept(buffer);
            future.complete(conn);
            return;
        }
        bufferPool.accept(buffer);
        future.completeExceptionally(new SQLException("postgres connect resp error"));
        conn.dispose();
    }

    protected static String getCString(ByteBuffer buffer, byte[] store) {
        int i = 0;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            store[i++] = c;
        }
        return new String(store, 0, i, StandardCharsets.UTF_8);
    }

    protected static ByteBuffer putCString(ByteBuffer buffer, String string) {
        buffer.put(string.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        return buffer;
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected int getDefaultPort() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
