/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.redkale.net.AsyncConnection;
import org.redkale.source.DataSources;
import org.redkale.util.ObjectPool;
import static org.redkalex.source.pgsql.PgPoolSource.CONN_ATTR_BYTESBAME;
import static org.redkalex.source.pgsql.Pgs.*;

/**
 *
 * @author zhangjx
 */
public class PgSQLDataSource {

    protected static final Logger logger = Logger.getLogger(PgSQLDataSource.class.getSimpleName());

    public static void main(String[] args) throws Throwable {
        final int capacity = 16 * 1024;
        final ObjectPool<ByteBuffer> bufferPool = new ObjectPool<>(new AtomicLong(), new AtomicLong(), 16,
            (Object... params) -> ByteBuffer.allocateDirect(capacity), null, (e) -> {
                if (e == null || e.isReadOnly() || e.capacity() != capacity) return false;
                e.clear();
                return true;
            });

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4, (Runnable r) -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });

        Properties prop = new Properties();
        prop.setProperty(DataSources.JDBC_URL, "jdbc:postgresql://127.0.0.1:5432/hello_world"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty(DataSources.JDBC_USER, "postgres");
        prop.setProperty(DataSources.JDBC_PWD, "1234");
        PgPoolSource poolSource = new PgPoolSource("", prop, logger, bufferPool, executor);
        System.out.println("user:" + poolSource.getUsername() + ", pass: " + poolSource.getPassword() + ", db: " + poolSource.getDatabase());
        long s, e;
        s = System.currentTimeMillis();
        poolSource.pollAsync().join().dispose();
        e = System.currentTimeMillis() - s;
        System.out.println("第一次连接耗时: " + e + "ms");
        System.out.println("--------------------------------------------开始简单查询连接--------------------------------------------");
        s = System.currentTimeMillis();
        singleQuery(bufferPool, poolSource);
        e = System.currentTimeMillis() - s;
        System.out.println("查询耗时: " + e + "ms");
        System.out.println("--------------------------------------------开始更新操作连接--------------------------------------------");
        s = System.currentTimeMillis();
        singleUpdate(bufferPool, poolSource);
        e = System.currentTimeMillis() - s;
        System.out.println("更新耗时: " + e + "ms");
        System.out.println("--------------------------------------------开始复杂查询连接--------------------------------------------");
        s = System.currentTimeMillis();
        prepareQuery(bufferPool, poolSource);
        e = System.currentTimeMillis() - s;
        System.out.println("复杂查询耗时: " + e + "ms");
    }

    private static void prepareQuery(final ObjectPool<ByteBuffer> bufferPool, final PgPoolSource poolSource) {
        final AsyncConnection conn = poolSource.pollAsync().join();
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        ByteBuffer buffer = bufferPool.get();
        {
            buffer.put((byte) 'P');
            int start = buffer.position();
            buffer.putInt(0);
            buffer.put((byte) 0); // unnamed prepared statement
            putCString(buffer, "SELECT * FROM fortune WHERE id = $1");
            buffer.putShort((short) 0); // no parameter types
            buffer.putInt(start, buffer.position() - start);
        }
        { // BIND
            buffer.put((byte) 'B');
            int start = buffer.position();
            buffer.putInt(0);
            buffer.put((byte) 0); // portal
            buffer.put((byte) 0); // prepared statement
            buffer.putShort((short) 0); // number of format codes
            byte[][] params = new byte[][]{"1".getBytes(StandardCharsets.UTF_8)};
            if (params == null) {
                buffer.putShort((short) 0); // number of parameters
            } else {
                buffer.putShort((short) params.length); // number of parameters
                for (byte[] param : params) {
                    if (param == null) {
                        buffer.putInt(-1);
                    } else {
                        buffer.putInt(param.length);
                        buffer.put(param);
                    }
                }
            }
            buffer.putShort((short) 0);
            buffer.putInt(start, buffer.position() - start);
        }
        { // DESCRIBE
            buffer.put((byte) 'D');
            buffer.putInt(4 + 1 + 1);
            buffer.put((byte) 'S');
            buffer.put((byte) 0);
        }
        { // EXECUTE
            buffer.put((byte) 'E');
            buffer.putInt(4 + 1 + 4);
            buffer.put((byte) 0);
            buffer.putInt(0);
        }
        { // CLOSE
            buffer.put((byte) 'C');
            buffer.putInt(4 + 1 + 1);
            buffer.put((byte) 'S');
            buffer.put((byte) 0);
        }
        { // SYNC
            buffer.put((byte) 'S');
            buffer.putInt(4);
        }
        buffer.flip();
        final CompletableFuture<AsyncConnection> future = new CompletableFuture();
        conn.write(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment1) {
                if (result < 0) {
                    failed(new SQLException("Write Buffer Error"), attachment1);
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
                            failed(new SQLException("Read Buffer Error"), attachment2);
                            return;
                        }
                        buffer.flip();
                        char cmd = (char) buffer.get();
                        int length = buffer.getInt();
                        System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        if (cmd == 'T') {
                            System.out.println(new RespRowDescDecoder().read(buffer, bytes));
                            cmd = (char) buffer.get();
                            length = buffer.getInt();
                            System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        }
                        while (cmd != 'E') {
                            if (cmd == 'C') {
                                System.out.println(Pgs.getCString(buffer, new byte[255]));
                            } else if (cmd == 'Z') {
                                System.out.println("连接待命中");
                                buffer.position(buffer.position() + length - 4);
                            } else {
                                buffer.position(buffer.position() + length - 4);
                            }
                            if (!buffer.hasRemaining()) break;
                            cmd = (char) buffer.get();
                            length = buffer.getInt();
                            System.out.println("---------cmd:" + cmd + "-----length:" + length);
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
                        future.complete(conn);
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
                exc.printStackTrace();
            }
        });
        future.join();
    }

    private static void singleUpdate(final ObjectPool<ByteBuffer> bufferPool, final PgPoolSource poolSource) {
        final AsyncConnection conn = poolSource.pollAsync().join();
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        ByteBuffer buffer = bufferPool.get();
        {
            buffer.put((byte) 'P');
            int start = buffer.position();
            buffer.putInt(0);
            buffer.put((byte) 0); // unnamed prepared statement
            putCString(buffer, "UPDATE fortune SET id=1 WHERE id=100");
            buffer.putShort((short) 0); // no parameter types
            buffer.putInt(start, buffer.position() - start);
        }
        { // BIND
            buffer.put((byte) 'B');
            int start = buffer.position();
            buffer.putInt(0);
            buffer.put((byte) 0); // portal
            buffer.put((byte) 0); // prepared statement
            buffer.putShort((short) 0); // number of format codes
            buffer.putShort((short) 0); // number of parameters
            buffer.putShort((short) 0);
            buffer.putInt(start, buffer.position() - start);
        }
        { // DESCRIBE
            buffer.put((byte) 'D');
            buffer.putInt(4 + 1 + 1);
            buffer.put((byte) 'S');
            buffer.put((byte) 0);
        }
        { // EXECUTE
            buffer.put((byte) 'E');
            buffer.putInt(4 + 1 + 4);
            buffer.put((byte) 0);
            buffer.putInt(0);
        }
        { // CLOSE
            buffer.put((byte) 'C');
            buffer.putInt(4 + 1 + 1);
            buffer.put((byte) 'S');
            buffer.put((byte) 0);
        }
        { // SYNC
            buffer.put((byte) 'S');
            buffer.putInt(4);
        }
        buffer.flip();
        final CompletableFuture<AsyncConnection> future = new CompletableFuture();
        conn.write(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment1) {
                if (result < 0) {
                    failed(new SQLException("Write Buffer Error"), attachment1);
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
                            failed(new SQLException("Read Buffer Error"), attachment2);
                            return;
                        }
                        buffer.flip();
                        char cmd = (char) buffer.get();
                        int length = buffer.getInt();
                        System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        if (cmd == 'T') {
                            System.out.println(new RespRowDescDecoder().read(buffer, bytes));
                            cmd = (char) buffer.get();
                            length = buffer.getInt();
                            System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        }
                        while (cmd != 'E') {
                            if (cmd == 'C') {
                                System.out.println(Pgs.getCString(buffer, new byte[255]));
                            } else if (cmd == 'Z') {
                                System.out.println("连接待命中");
                                buffer.position(buffer.position() + length - 4);
                            } else {
                                buffer.position(buffer.position() + length - 4);
                            }
                            if (!buffer.hasRemaining()) break;
                            cmd = (char) buffer.get();
                            length = buffer.getInt();
                            System.out.println("---------cmd:" + cmd + "-----length:" + length);
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
                        future.complete(conn);
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
                exc.printStackTrace();
            }
        });
        future.join();
    }

    private static void singleQuery(final ObjectPool<ByteBuffer> bufferPool, final PgPoolSource poolSource) {
        final AsyncConnection conn = poolSource.pollAsync().join();
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        ByteBuffer buffer = bufferPool.get();
        {
            buffer.put((byte) 'Q');
            int start = buffer.position();
            buffer.putInt(0);
            putCString(buffer, "SELECT * FROM fortune");
            buffer.putInt(start, buffer.position() - start);
        }
        buffer.flip();
        final CompletableFuture<AsyncConnection> future = new CompletableFuture();
        conn.write(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment1) {
                if (result < 0) {
                    failed(new SQLException("Write Buffer Error"), attachment1);
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
                            failed(new SQLException("Read Buffer Error"), attachment2);
                            return;
                        }
                        buffer.flip();
                        char cmd = (char) buffer.get();
                        int length = buffer.getInt();
                        System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        if (cmd == 'T') {
                            System.out.println(new RespRowDescDecoder().read(buffer, bytes));
                            cmd = (char) buffer.get();
                            length = buffer.getInt();
                            System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        }
                        while (cmd != 'E') {
                            if (cmd == 'C') {
                                System.out.println(Pgs.getCString(buffer, new byte[255]));
                            } else if (cmd == 'Z') {
                                System.out.println("连接待命中");
                                buffer.position(buffer.position() + length - 4);
                            } else {
                                buffer.position(buffer.position() + length - 4);
                            }
                            if (!buffer.hasRemaining()) break;
                            cmd = (char) buffer.get();
                            length = buffer.getInt();
                            System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        }
                        future.complete(conn);
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
                exc.printStackTrace();
            }
        });
        future.join();
    }
}
