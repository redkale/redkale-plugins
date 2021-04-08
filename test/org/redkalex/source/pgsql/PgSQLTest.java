/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import javax.persistence.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.*;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkale.boot.Application.RESNAME_APP_GROUP;

/**
 *
 * @author zhangjx
 */
public class PgSQLTest {

    private static final Random random = new SecureRandom();

    protected static int randomId() {
        return random.nextInt(10000) + 1;
    }

    public static void main(String[] args) throws Throwable {
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        ResourceFactory.root().register(RESNAME_APP_GROUP, asyncGroup);

        Properties prop = new Properties();
        prop.setProperty(DataSources.JDBC_URL, "jdbc:postgresql://127.0.0.1:5432/hello_world"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop.put("javax.persistence.jdbc.preparecache", "true");
        prop.setProperty(DataSources.JDBC_USER, "postgres");
        prop.setProperty(DataSources.JDBC_PWD, "1234");

        Properties prop2 = new Properties();
        prop2.setProperty(DataSources.JDBC_URL, "jdbc:postgresql://127.0.0.1:5432/hello_world"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop2.put("javax.persistence.jdbc.preparecache", "true");
        prop2.setProperty(DataSources.JDBC_USER, "postgres");
        prop2.setProperty(DataSources.JDBC_PWD, "1234");

        final PgsqlLDataSource source = new PgsqlLDataSource("", null, prop, prop2);
        ResourceFactory.root().inject(source);
        source.init(null);
        {
            System.out.println("当前机器CPU核数: " + Runtime.getRuntime().availableProcessors());
            final CompletableFuture[] futures = new CompletableFuture[Runtime.getRuntime().availableProcessors()];
            final CompletableFuture[] futures2 = new CompletableFuture[Runtime.getRuntime().availableProcessors()];
            Thread.sleep(1000);
            for (int i = 0; i < futures.length; i++) {
                PgClientConnection conn = (PgClientConnection) source.readPoolSource().client.getConnArray()[i];
                futures[i] = conn.writeChannel(new PgReqQuery(null, "SELECT 2"));
                PgClientConnection conn2 = (PgClientConnection) source.writePoolSource().client.getConnArray()[i];
                futures2[i] = conn2.writeChannel(new PgReqQuery(null, "SELECT 2"));
            }

            CompletableFuture.allOf(futures).join();
            CompletableFuture.allOf(futures2).join();
            System.out.println("已连接数: " + source.readPoolSource().client.getConnArray().length);
        }
//        System.out.println(source.queryList(Fortune.class)); 
        CompletableFuture[] ffs = new CompletableFuture[2];
        ffs[0] = source.findAsync(Fortune.class, 1);
        ffs[1] = source.findAsync(Fortune.class, 2);
        CompletableFuture.allOf(ffs).join();
        System.out.println(ffs[0].join());
        System.out.println(ffs[1].join());
//        ffs = new CompletableFuture[2];
//        ffs[0] = source.findAsync(Fortune.class, 1);
//        ffs[1] = source.findAsync(Fortune.class, 2);
//        CompletableFuture.allOf(ffs).join();
//        System.out.println(ffs[0].join());
//        System.out.println(ffs[1].join());
        Thread.sleep(1000);
        System.out.println("============== 开始 ==============");
        source.readPoolSource().client.getWriteReqCounter().set(0);
        source.readPoolSource().client.getPollRespCounter().set(0);
        source.writePoolSource().client.getWriteReqCounter().set(0);
        source.writePoolSource().client.getPollRespCounter().set(0);
        final int count = 2;  //4.18秒
        final CountDownLatch cdl = new CountDownLatch(count);
        final CountDownLatch startcdl = new CountDownLatch(count);
        long s1 = System.currentTimeMillis();
        for (int j = 0; j < count; j++) {
            new Thread() {
                @Override
                public void run() {
                    try {
                        startcdl.countDown();
                        startcdl.await();

                        final World[] rs = new World[5];
                        final CompletableFuture[] futures = new CompletableFuture[rs.length];
                        for (int i = 0; i < rs.length; i++) {
                            final int index = i;
                            futures[index] = source.findAsync(World.class, randomId()).thenAccept(r -> rs[index] = r.randomNumber(randomId()));
                        }
                        CompletableFuture.allOf(futures).thenCompose(v -> {
                            //return CompletableFuture.completedFuture(null);
                            return source.updateAsync(sort(rs));
                        }).whenComplete((r, t) -> {
                            cdl.countDown();
                            if (t != null) t.printStackTrace();
                        });
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
            }.start();
        }
        cdl.await();
        long e1 = System.currentTimeMillis() - s1;
        System.out.println("一共耗时: " + e1);
        System.out.println("只读池req数: " + source.readPoolSource().client.getWriteReqCounter());
        System.out.println("只读池resp数: " + source.readPoolSource().client.getPollRespCounter());
        System.out.println("---------------------------------");

        s1 = System.currentTimeMillis();
        source.close();
        e1 = System.currentTimeMillis() - s1;
        System.out.println("关闭耗时: " + e1 + " ms");
    }

    protected static World[] sort(World[] worlds) {
        Arrays.sort(worlds);
        return worlds;
    }

    protected static ByteBuffer writeUTF8String(ByteBuffer array, String string) {
        array.put(string.getBytes(StandardCharsets.UTF_8));
        array.put((byte) 0);
        return array;
    }

    protected static String readUTF8String(ByteBuffer buffer, ByteArray array) {
        int i = 0;
        array.clear();
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            array.put(c);
        }
        return array.toString(StandardCharsets.UTF_8);
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

        public World randomNumber(int randomNumber) {
            this.randomNumber = randomNumber;
            return this;
        }

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
