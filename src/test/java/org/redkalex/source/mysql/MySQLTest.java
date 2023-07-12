/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

//import org.redkalex.source.mysql_old.MysqlDataSource;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.*;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import org.redkale.boot.LoggingFileHandler;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.*;
import org.redkale.persistence.*;
import org.redkale.source.*;
import org.redkale.util.*;
import org.redkalex.source.vertx.VertxSqlDataSource;

/**
 *
 * @author zhangjx
 */
public class MySQLTest {

    public static void main(String[] args) throws Throwable {

        LoggingFileHandler.initDebugLogConfig();
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        ResourceFactory factory = ResourceFactory.create();
        factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup);

        Properties prop = new Properties();
        prop.setProperty("redkale.datasource.default.url", "jdbc:mysql://127.0.0.1:3389/aa_test?useSSL=false&rewriteBatchedStatements=true&serverTimezone=UTC&characterEncoding=utf8"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty("redkale.datasource.default.maxconns", "10");
        prop.setProperty("redkale.datasource.default.table-autoddl", "true");
        prop.setProperty("redkale.datasource.default.user", "root");
        prop.setProperty("redkale.datasource.default.password", "");

        if (VertxSqlDataSource.class.isAssignableFrom(AbstractDataSqlSource.class)) {
            return;
        }
        ExecutorService workExecutor = WorkThread.createExecutor(20, "Thread-%s");
        MysqlDataSource source = new MysqlDataSource();
        MysqlDataSource.debug = true;
        //DataJdbcSource source = new DataJdbcSource();
        factory.inject(source);
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue("default"));
        System.out.println("---------");
        Function<DataResultSet, String> func = set -> set.next() ? ("" + set.getObject(1)) : null;
        //System.out.println("查询结果: " + source.nativeQuery("SHOW TABLES", func));
        //System.out.println("执行结果: " + source.nativeExecute("SET NAMES UTF8MB4"));
        if (false) {
            System.out.println("当前机器CPU核数: " + Utility.cpus());
            System.out.println("执行结果: " + source.nativeExecute("UPDATE World set id =0 where id =0"));
            System.out.println("随机获取World记录1: " + source.findAsync(World.class, randomId()).join());
            System.out.println("随机获取World记录2: " + source.findsListAsync(World.class, Stream.of(randomId(), -1122, randomId())).join());
            System.out.println("随机获取World记录3: " + Arrays.toString(source.findsAsync(World.class, randomId(), -1122, randomId()).join()));
            World w1 = source.findAsync(World.class, 11).join();
            World w2 = source.findAsync(World.class, 22).join();
            System.out.println("随机获取World记录4: " + w1 + ", " + w2);
            w1.setRandomNumber(w1.getRandomNumber() + 2);
            w2.setRandomNumber(w2.getRandomNumber() + 2);
            source.updateAsync(w1, w2).join();
            w1 = source.findAsync(World.class, 11).join();
            w2 = source.findAsync(World.class, 22).join();
            System.out.println("修改后World记录: " + w1 + ", " + w2);

            System.out.println("随机获取World记录5: " + source.findsListAsync(World.class, Stream.of(randomId(), randomId())).join());

            IntStream ids = IntStream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20);
            System.out.println("组合操作: " + source.findsListAsync(World.class, ids.boxed()).thenCompose(words -> source.updateAsync(words.toArray()).thenApply(v -> words)).join());
            System.out.println(source.queryList(Fortune.class));
            System.out.println();
            System.out.println();
            System.out.println();

            if (true) {
                MysqlDataSource.debug = true;
                CountDownLatch cdl = new CountDownLatch(50);
                for (int i = 0; i < cdl.getCount(); i++) {
                    workExecutor.submit(() -> {
                        int size = 20;
                        try {
                            IntStream ii = ThreadLocalRandom.current().ints(size, 1, 10001);
                            source.findsListAsync(World.class, ii.boxed()).thenCompose(words -> source.updateAsync(words.toArray()).thenApply(v -> words)).join();
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            cdl.countDown();
                        }
                    });
                }
                cdl.await();
                return;
            }

            DayRecord record1 = new DayRecord();
            record1.setCreateTime(System.currentTimeMillis());
            record1.setContent("这是内容1 " + Utility.formatTime(record1.getCreateTime()));
            record1.setRecordid("rid-" + record1.getCreateTime());

            DayRecord record2 = new DayRecord();
            record2.setCreateTime(record1.getCreateTime() - 24 * 60 * 60 * 1000L - 1);
            record2.setContent("这是内容2 " + Utility.formatTime(record2.getCreateTime()));
            record2.setRecordid("rid2-" + record2.getCreateTime());

            DayRecord record3 = new DayRecord();
            record3.setCreateTime(record1.getCreateTime() - 48 * 60 * 60 * 1000L - 2);
            record3.setContent("这是内容3 " + Utility.formatTime(record3.getCreateTime()));
            record3.setRecordid("rid3-" + record3.getCreateTime());

            DayRecord record4 = new DayRecord();
            record4.setCreateTime(record1.getCreateTime() - 72 * 60 * 60 * 1000L - 3);
            record4.setContent("这是内容4 " + Utility.formatTime(record4.getCreateTime()));
            record4.setRecordid("rid4-" + record4.getCreateTime());

            source.insert(record1, record2);
            System.out.println("-------新增成功---------");

            record1.setContent("这是内容1 xx " + Utility.formatTime(record1.getCreateTime()));
            record2.setContent("这是内容2 xx " + Utility.formatTime(record2.getCreateTime()));
            record3.setContent("这是内容3 xx " + Utility.formatTime(record3.getCreateTime()));
            record4.setContent("这是内容4 xx " + Utility.formatTime(record4.getCreateTime()));
            for (int i = 0; i < 10; i++) {
                //    record2.setContent(record2.getContent() + " ******** ");
            }

            //int rs = source.updateAsync(record1, record2, record3, record4).join();
            int rs = source.update(record1, record2, record3, record4);
            System.out.println("-------修改成功数: " + rs + "---------");

            FilterNode node = FilterNode.create("createTime", new Range.LongRange(record4.getCreateTime(), record1.getCreateTime()));
            System.out.println("查询结果: " + source.querySheet(DayRecord.class, new Flipper(), node));

            FilterNode pkFilter = FilterNode.create("recordid", (Serializable) Utility.ofList(record1.getRecordid(), record2.getRecordid(), record3.getRecordid(), record4.getRecordid()));

            rs = source.updateColumn(record1, pkFilter, SelectColumn.includes("content"));
            System.out.println("-------修改成功数: " + rs + "---------");

            //rs = source.delete(record1, record2, record3, record4);
            rs = source.delete(DayRecord.class, pkFilter);
            System.out.println("-------删除成功数: " + rs + "---------");

            rs = source.clearTable(DayRecord.class, pkFilter);
            System.out.println("-------清空成功数: " + rs + "---------");

            rs = source.dropTable(DayRecord.class, pkFilter);
            System.out.println("-------删表成功数: " + rs + "---------");
            return;
        }

        System.out.println("清空表: " + source.clearTable(World.class));
        //System.out.println("----------新增记录----------");

        //EntityInfo info = ((MysqlDataSource)source).loadEntityInfo(SmsRecord.class);
        //System.out.println(Arrays.toString(info.getQueryAttributes()));
        System.out.println("查询List结果: " + source.queryList(World.class));
        System.out.println("查询List结束========");

        World w1 = new World();
        w1.id = 1;
        w1.randomNumber = 10;
        World w2 = new World();
        w2.id = 2;
        w2.randomNumber = 20;
        System.out.println("新增结果: " + source.insert(w1, w2));

        System.out.println("\r\n\r\n开始更新\r\n\r\n");
        w1.randomNumber = 11;
        w2.randomNumber = 22;
        System.out.println("修改结果: " + source.update(w1, w2));

        System.out.println("\r\n\r\n开始删除\r\n\r\n");
        System.out.println("删除结果: " + source.delete(w1));

        System.out.println("\r\n\r\n开始finds\r\n\r\n");
        List<Integer> list = List.of(1, -1, 2);
        System.out.println(Arrays.toString(source.finds(World.class, list.stream())));

        System.out.println("\r\n\r\n再次finds\r\n\r\n");
        List<Integer> list2 = List.of(3);
        System.out.println(Arrays.toString(source.finds(World.class, list2.stream())));

        int[] cs = source.nativeExecute("update world set randomNumber =11 where id =2", "update world set randomNumber =11 where id =-1");
        System.out.println("批量处理结果: " + Arrays.toString(cs));

        //if (true) return;
        final SmsRecord record = new SmsRecord((short) 2, "12345678901", "这是内容");
        record.setSmsid("sms1-" + record.getCreateTime());
        SmsRecord record2 = new SmsRecord((short) 2, "12345678901", "这是内容");
        record2.setSmsid("sms2-" + record.getCreateTime());
        System.out.println("新增结果: " + source.insert(record, record2));
        SmsRecord record3 = new SmsRecord((short) 2, "12345678901", "这是内容");
        record3.setSmsid("sms3-" + record.getCreateTime());
        System.out.println("新增结果: " + source.insert(record3));
        SmsRecord record4 = new SmsRecord((short) 2, "12345678901", "这是内容");
        record4.setSmsid("sms4-" + record.getCreateTime());
        System.out.println("新增结果: " + source.insert(record4));
        if (source.find(SmsRecord.class, record.getSmsid()) == null) {
            source.insert(record);
        }

        //if (true) return;
        System.out.println(source.find(SmsRecord.class, "sms1-1632282662741"));
        System.out.println("--------------继续查询单个记录------------------");
        SmsRecord sms = source.find(SmsRecord.class, FilterNode.create("smsid", record.getSmsid()));
        System.out.println(sms);
        sms.setCreateTime(System.currentTimeMillis());
        sms.setStatus((short) 3);
        System.out.println("----------------修改记录----------------");
        System.out.println("修改结果: " + source.update(sms));
        System.out.println("修改结果: " + source.update(sms));
        sms.setSmsid("33");
        System.out.println("修改结果: " + source.update(sms));
        System.out.println(source.find(SmsRecord.class, "sms1-1632282662741"));

        System.out.println("\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n\r\n");
        source.close();
        System.out.println(" -------------------------------- 压测开始 --------------------------------");
        MysqlDataSource source2 = new MysqlDataSource();
        MysqlDataSource.debug = false;
        AsyncIOGroup asyncGroup2 = new AsyncIOGroup(8192, 16);
        asyncGroup2.start();
        ResourceFactory factory2 = ResourceFactory.create();
        factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup2);
        factory2.inject(source2);
        source2.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue(""));
        
        final String json = JsonConvert.root().convertTo(record);
        int count = 200;
        CountDownLatch cdl = new CountDownLatch(count);
        long s = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            final int b = i;
            new Thread() {
                public void run() {
                    try {
                        if (b % 3 == 0) {
                            String smsid = record.getSmsid().replace("sms1", "sms" + ((b + 1) % 5 + 100));
                            source2.delete(SmsRecord.class, smsid);
                        } else if (b % 2 == 0) {
                            source2.findAsync(SmsRecord.class, record.getSmsid()).thenCompose(v -> source2.updateAsync(v)).join();
                        } else {
                            String smsid = record.getSmsid().replace("sms1", "sms" + (b + 1) % 5);
                            String content = "这是内容," + ((b + 1) % 5);
                            SmsRecord s = JsonConvert.root().convertFrom(SmsRecord.class, json);
                            s.setSmsid(smsid);
                            s.setContent(content);
                            source2.update(s);
                        }
                    } finally {
                        cdl.countDown();
                    }
                }
            }.start();
        }
        cdl.await();
        long e = System.currentTimeMillis() - s;
        System.out.println("并发 " + count + ", 一共耗时: " + e + "ms");
        System.out.println("---------------- 准备关闭DataSource ----------------");
        source2.close();
        System.out.println("---------------- 全部执行完毕 ----------------");
    }

    protected static int randomId() {
        return ThreadLocalRandom.current().nextInt(10000) + 1;
    }

    @Entity
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
