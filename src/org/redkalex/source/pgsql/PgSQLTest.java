/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.persistence.Id;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.AsyncConnection;
import org.redkale.source.*;
import org.redkale.util.ObjectPool;
import static org.redkalex.source.pgsql.PgPoolSource.CONN_ATTR_BYTESBAME;
import static org.redkalex.source.pgsql.PgSQLDataSource.*;

/**
 *
 * @author zhangjx
 */
public class PgSQLTest {

    public static void main(String[] args) throws Throwable {
        final Logger logger = Logger.getLogger(PgSQLDataSource.class.getSimpleName());
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
        PgSQLDataSource source = new PgSQLDataSource("", null, prop, prop);
        PoolSource<AsyncConnection> poolSource = source.writePoolSource();
        System.out.println("user:" + poolSource.getUsername() + ", pass: " + poolSource.getPassword() + ", db: " + poolSource.getDatabase());
        long s, e;
        s = System.currentTimeMillis();
        AsyncConnection conn = poolSource.pollAsync().join();
        System.out.println("真实连接: " + conn);
        poolSource.offerConnection(conn);
        e = System.currentTimeMillis() - s;
        System.out.println("第一次连接(" + conn + ")耗时: " + e + "ms");
        System.out.println("--------------------------------------------开始singleQuery查询连接--------------------------------------------");
        s = System.currentTimeMillis();
        singleQuery(bufferPool, poolSource);
        e = System.currentTimeMillis() - s;
        System.out.println("SELECT * FROM fortune  查询耗时: " + e + "ms");

        System.out.println("--------------------------------------------PgSQLDataSource更新操作--------------------------------------------");
        Fortune bean = new Fortune();
        bean.setId(1);
        bean.setMessage("aaa");
        s = System.currentTimeMillis();
        int rows = source.updateColumn(bean, "message");
        e = System.currentTimeMillis() - s;
        System.out.println("更新结果:(" + rows + ") 耗时: " + e + "ms");

        bean = new Fortune();
        bean.setId(100);
        bean.setMessage("ccc");
        s = System.currentTimeMillis();
        source.delete(bean);
        e = System.currentTimeMillis() - s;
        System.out.println("删除结果:(" + rows + ") 耗时: " + e + "ms");
        
        bean = new Fortune();
        bean.setId(100);
        bean.setMessage("ccc");
        s = System.currentTimeMillis();
        source.insert(bean);
        e = System.currentTimeMillis() - s;
        System.out.println("插入结果:(" + rows + ") 耗时: " + e + "ms");

        
        conn = poolSource.pollAsync().join();
        System.out.println("真实连接: " + conn);
    }

    private static void singleQuery(final ObjectPool<ByteBuffer> bufferPool, final PoolSource<AsyncConnection> poolSource) {
        final AsyncConnection conn = poolSource.pollAsync().join();
        System.out.println("真实连接: " + conn);
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
                            System.out.println(new RespRowDescDecoder().read(buffer, length, bytes));
                            cmd = (char) buffer.get();
                            length = buffer.getInt();
                            System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        }
                        while (cmd != 'E') {
                            if (cmd == 'C') {
                                System.out.println(getCString(buffer, new byte[255]));
                            } else if (cmd == 'Z') {
                                System.out.println("连接待命中");
                                buffer.position(buffer.position() + length - 4);
                                poolSource.offerConnection(conn);
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
                            System.out.println("---------Exception------level:" + level + "-----code:" + code + "-----message:" + message);
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

    public static class Fortune implements Comparable<Fortune> {

        @Id
        private int id;

        private String message = "";

        public Fortune() {
        }

        public Fortune(int id, String message) {
            this.id = id;
            this.message = message;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        @Override
        public int compareTo(Fortune o) {
            return message.compareTo(o.message);
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }

    }

}
