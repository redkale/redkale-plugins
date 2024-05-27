/*
 *
 */
package org.redkalex.source.mysql;

import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.redkale.boot.LoggingFileHandler;
import org.redkale.convert.json.*;
import org.redkale.inject.ResourceFactory;
import org.redkale.net.AsyncIOGroup;
import org.redkale.persistence.*;
import org.redkale.source.*;
import org.redkale.util.*;
import org.redkalex.source.base.IncreWorld;
import org.redkalex.source.parser.DataNativeJsqlParser;
import org.redkalex.source.vertx.TestRecord;

/** @author zhangjx */
public class MySQLJdbcTest {

    public static void main(String[] args) throws Throwable {

        LoggingFileHandler.initDebugLogConfig();
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        ResourceFactory factory = ResourceFactory.create();
        factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup);
        factory.register("", new DataNativeJsqlParser());

        Properties prop = new Properties();
        prop.setProperty(
                "redkale.datasource.default.url",
                "jdbc:mysql://127.0.0.1:3389/aa_test?useSSL=false&rewriteBatchedStatements=true&serverTimezone=UTC&characterEncoding=utf8"); // 192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty("redkale.datasource.default.maxconns", "1");
        prop.setProperty("redkale.datasource.default.table-autoddl", "true");
        prop.setProperty("redkale.datasource.default.user", "root");
        prop.setProperty("redkale.datasource.default.password", "");

        Connection conn = DriverManager.getConnection(
                prop.getProperty("redkale.datasource.default.url"),
                prop.getProperty("redkale.datasource.default.user"),
                prop.getProperty("redkale.datasource.default.password"));
        System.out.println(conn);
        conn.close();

        DataJdbcSource source = new DataJdbcSource();
        factory.inject(source);
        source.init(AnyValue.loadFromProperties(prop)
                .getAnyValue("redkale")
                .getAnyValue("datasource")
                .getAnyValue("default"));
        System.out.println("---------");

        System.out.println("当前机器CPU核数: " + Utility.cpus());
        System.out.println("清空DayRecord表: " + source.clearTable(OneRecord.class));
        {
            // source.dropTable(TestRecord.class);
            TestRecord entity = new TestRecord();
            entity.setRecordid("r223" + System.currentTimeMillis());
            entity.setScore(200);
            entity.setStatus((short) 10);
            entity.setName("myname2");
            entity.setCreateTime(System.currentTimeMillis());
            source.insert(entity);

            Map<String, Object> params = Utility.ofMap("name", "%", "ids", Utility.ofList(entity.getRecordid()));
            String sql = "SELECT * FROM TestRecord WHERE name LIKE :name OR recordid IN :ids";
            TestRecord one = source.nativeQueryOne(TestRecord.class, sql, params);
            System.out.println(one);

            String upsql = "UPDATE TestRecord SET name='aa' WHERE name LIKE :name OR recordid IN :ids";
            int rs = source.nativeUpdate(upsql, params);
            System.out.println("修改结果数: " + rs);
            System.out.println(source.find(TestRecord.class, entity.getRecordid()));

            String sheetSql = "SELECT * FROM TestRecord WHERE name LIKE :name OR recordid IN :ids";
            Flipper flipper = new Flipper(2);
            Sheet<TestRecord> sheet = source.nativeQuerySheet(TestRecord.class, sheetSql, flipper, params);
            System.out.println(sheet);
            System.out.println("获得总数: " + sheet.getTotal());

            sheetSql = "SELECT * FROM TestRecord WHERE recordid IN :ids";
            sheet = source.nativeQuerySheet(TestRecord.class, sheetSql, flipper, params);
            System.out.println(sheet);
            System.out.println("获得总数: " + sheet.getTotal());
        }

        OneRecord record1 = new OneRecord();
        record1.setCreateTime(11);
        record1.setContent("这是内容1");
        record1.setRecordid("rid-1");

        OneRecord record2 = new OneRecord();
        record2.setCreateTime(22);
        record2.setContent("这是内容2");
        record2.setRecordid("rid-2");

        OneRecord record3 = new OneRecord();
        record3.setCreateTime(33);
        record3.setContent("这是内容3");
        record3.setRecordid("rid-3");

        source.insert(record1, record2, record3);

        DataBatch batch = DataBatch.create();
        batch.update(OneRecord.class, record2.getRecordid(), ColumnValue.set("content", "这是内容2XX"));
        batch.insert(record3);
        Exception exception = null;
        try {
            System.out.println("执行批量操作结果: " + source.batch(batch));
        } catch (Exception e) {
            exception = e;
        }
        Assertions.assertTrue(exception != null);
        OneRecord r2 = source.find(OneRecord.class, record2.getRecordid());
        Assertions.assertTrue("这是内容2".equals(r2.getContent()));

        batch = DataBatch.create();
        batch.update(OneRecord.class, record2.getRecordid(), ColumnValue.set("content", "这是内容2XX"));
        batch.update(OneRecord.class, record3.getRecordid(), ColumnValue.set("content", "这是内容3XX"));
        batch.delete(record3);
        exception = null;
        try {
            System.out.println("执行批量操作结果: " + source.batch(batch));
        } catch (Exception e) {
            e.printStackTrace();
            exception = e;
        }
        Assertions.assertTrue(exception == null);
        r2 = source.find(OneRecord.class, record2.getRecordid());
        Assertions.assertTrue("这是内容2XX".equals(r2.getContent()));
        r2 = source.findAsync(OneRecord.class, record2.getRecordid()).join();
        Assertions.assertTrue("这是内容2XX".equals(r2.getContent()));
        System.out.println(source.queryList(OneRecord.class));
        System.out.println(source.findsListAsync(OneRecord.class, Stream.of("11", record2.getRecordid()))
                .join());
        System.out.println(source.findsList(OneRecord.class, Stream.of("11", record2.getRecordid())));
        System.out.println(source.nativeQueryOne(OneRecord.class, "select * from onerecord where recordid = 'rid-1' "));
        System.out.println(
                source.nativeQueryList(OneRecord.class, "select * from onerecord where recordid = 'rid-1' "));
        System.out.println(
                "Map-List: " + source.nativeQueryList(Map.class, "select * from onerecord where recordid = 'rid-1' "));
        System.out.println("JsonObject-List: "
                + source.nativeQueryList(JsonObject.class, "select * from onerecord where recordid = 'rid-1' "));

        TwoIntRecord t1 = new TwoIntRecord();
        t1.setId(1);
        t1.setRandomNumber(1);
        TwoIntRecord t2 = new TwoIntRecord();
        t2.setId(2);
        t2.setRandomNumber(2);
        TwoIntRecord t3 = new TwoIntRecord();
        t3.setId(3);
        t3.setRandomNumber(3);
        source.clearTable(TwoIntRecord.class);
        source.insert(t1, t2, t3);

        t1.setRandomNumber(11);
        t2.setRandomNumber(22);
        t3.setRandomNumber(33);
        source.update(t1, t2, t3);

        source.dropTable(IncreWorld.class);
        IncreWorld in1 = new IncreWorld();
        in1.setRandomNumber(11);
        IncreWorld in2 = new IncreWorld();
        in2.setRandomNumber(22);
        source.insert(in1, in2);
        System.out.println("IncreWorld记录: " + in1);
        System.out.println("IncreWorld记录: " + in2);

        if (true) { // 压测
            int count = 500;
            CountDownLatch sr = new CountDownLatch(count);
            CountDownLatch cdl = new CountDownLatch(count);
            long s = System.currentTimeMillis();
            AtomicInteger rand = new AtomicInteger();
            for (int i = 0; i < count; i++) {
                new Thread(() -> {
                            sr.countDown();
                            try {
                                sr.await();
                                source.find(OneRecord.class, record2.getRecordid() + rand.incrementAndGet());
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                cdl.countDown();
                            }
                        })
                        .start();
            }
            cdl.await();
            long e = System.currentTimeMillis() - s;
            System.out.println("并发 " + count + ", 参考值：0.330秒，耗时: " + e / 1000.0);
        }

        System.out.println("---------------- 准备关闭DataSource ----------------");
        source.close();
        System.out.println("---------------- 全部执行完毕 ----------------");
    }

    @Entity
    public static class TwoIntRecord {

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
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    @Entity
    public static class OneRecord {

        @Id
        @Column(length = 64, comment = "主键")
        private String recordid = "";

        @Column(length = 60, comment = "内容")
        private String content = "";

        @Column(updatable = false, comment = "生成时间，单位毫秒")
        private long createTime;

        public String getRecordid() {
            return recordid;
        }

        public void setRecordid(String recordid) {
            this.recordid = recordid;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }
}
