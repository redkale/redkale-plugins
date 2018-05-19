/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import javax.persistence.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.AsyncConnection;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkalex.source.pgsql.PgPoolSource.CONN_ATTR_BYTESBAME;
import static org.redkalex.source.pgsql.PgSQLDataSource.*;

/**
 *
 * @author zhangjx
 */
public class PgSQLTest {

    public static void main(String[] args) throws Throwable {
        System.out.println(ByteBuffer.allocate(1).getClass());
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

        if (false) {
            System.out.println("--------------------------------------------PgSQLDataSource插入操作--------------------------------------------");
            //create table record(  id serial,  name character varying(128), constraint pk_record_id primary key( id) ); 
            Record r1 = new Record("ccc");
            Record r2 = new Record("eee");
            s = System.currentTimeMillis();
            source.insert(r1, r2);
            e = System.currentTimeMillis() - s;
            System.out.println("插入Record结果:(" + rows + ") 耗时: " + e + "ms " + r1 + "," + r2);
        }

        System.out.println("--------------------------------------------PgSQLDataSource查询操作--------------------------------------------");
        s = System.currentTimeMillis();
        List rs = source.queryList(Fortune.class);
        System.out.println(rs);
        e = System.currentTimeMillis() - s;
        System.out.println("查询结果:(" + rs.size() + ") 耗时: " + e + "ms ");

        System.out.println("--------------------------------------------PgSQLDataSource查改操作--------------------------------------------");
        final World[] ws = new World[500];
        s = System.currentTimeMillis();
        for (int i = 0; i < ws.length; i++) {
            ws[i] = source.find(World.class, i + 1);
            ws[i].setId(i + 1);
            ws[i].setRandomNumber(i + 1);
        }
        System.out.println("查询结果:(" + rs.size() + ") 耗时: " + e + "ms ");
        source.update(ws);
        e = System.currentTimeMillis() - s;
        System.out.println("更改结果:(" + rs.size() + ") 耗时: " + e + "ms ");

        conn = poolSource.pollAsync().join();
        System.out.println("真实连接: " + conn);

    }

    private static void singleQuery(final ObjectPool<ByteBuffer> bufferPool, final PoolSource<AsyncConnection> poolSource) {
        final AsyncConnection conn = poolSource.pollAsync().join();
        System.out.println("真实连接: " + conn);
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        final String sql = "SELECT a.* FROM fortune a";
        ByteBuffer buffer = bufferPool.get();
        {
            buffer.put((byte) 'Q');
            int start = buffer.position();
            buffer.putInt(0);
            writeUTF8String(buffer, sql);
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
                        ByteBufferReader bufReader = ByteBufferReader.create(buffer);
                        char cmd = (char) buffer.get();
                        int length = buffer.getInt();
                        System.out.println(sql + "---------cmd:" + cmd + "-----length:" + length);
                        if (cmd == 'T') {
                            System.out.println(new RespRowDescDecoder().read(bufReader, length, bytes));
                            cmd = (char) buffer.get();
                            length = buffer.getInt();
                            System.out.println("---------cmd:" + cmd + "-----length:" + length);
                        }
                        while (cmd != 'E') {
                            if (cmd == 'C') {
                                System.out.println(readUTF8String(buffer, new byte[255]));
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

    //@DistributeTable(strategy = Record.TableStrategy.class)
    public static class Record {

        public static class TableStrategy implements DistributeTableStrategy<Record> {

            private static final String format = "%1$tY%1$tm";

            @Override
            public String getTable(String table, FilterNode node) {
                int pos = table.indexOf('.');
                return table.substring(pos + 1) + "_" + String.format(format, System.currentTimeMillis());
            }

            @Override
            public String getTable(String table, Record bean) {
                int pos = table.indexOf('.');
                return table.substring(pos + 1) + "_" + String.format(format, System.currentTimeMillis());
            }

            @Override
            public String getTable(String table, Serializable primary) {
                int pos = table.indexOf('.');
                return table.substring(pos + 1) + "_" + String.format(format, System.currentTimeMillis());
            }
        }

        @Id
        @GeneratedValue
        private int id;

        private String name = "";

        public Record() {
        }

        public Record(String name) {
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }

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

    public static class World implements Comparable<World> {

        @Id
        private int id;

        private int randomNumber;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public int getRandomNumber() {
            return randomNumber;
        }

        public void setRandomNumber(int randomNumber) {
            this.randomNumber = randomNumber;
        }

        @Override
        public int compareTo(World o) {
            return Integer.compare(id, o.id);
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }

    }

}
