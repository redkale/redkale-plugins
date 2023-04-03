/*
 *
 */
package org.redkalex.source.mysql;

import java.util.Properties;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import org.redkale.boot.LoggingFileHandler;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.AsyncIOGroup;
import org.redkale.persistence.*;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class MySQLJdbcTest {

    public static void main(String[] args) throws Throwable {

        LoggingFileHandler.initDebugLogConfig();
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        ResourceFactory factory = ResourceFactory.create();
        factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup);

        Properties prop = new Properties();
        prop.setProperty("redkale.datasource[].url", "jdbc:mysql://127.0.0.1:3389/aa_test?useSSL=false&rewriteBatchedStatements=true&serverTimezone=UTC&characterEncoding=utf8"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty("redkale.datasource[].maxconns", "10");
        prop.setProperty("redkale.datasource[].table-autoddl", "true");
        prop.setProperty("redkale.datasource[].user", "root");
        prop.setProperty("redkale.datasource[].password", "");

        DataJdbcSource source = new DataJdbcSource();
        factory.inject(source);
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue(""));
        System.out.println("---------");

        System.out.println("当前机器CPU核数: " + Utility.cpus());
        System.out.println("清空DayRecord表: " + source.clearTable(OneRecord.class));

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
        batch.update(OneRecord.class, record2.getRecordid(), ColumnValue.mov("content", "这是内容2XX"));
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
        batch.update(OneRecord.class, record2.getRecordid(), ColumnValue.mov("content", "这是内容2XX"));
        batch.update(OneRecord.class, record3.getRecordid(), ColumnValue.mov("content", "这是内容3XX"));
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
        System.out.println(source.findsListAsync(OneRecord.class, Stream.of("11", record2.getRecordid())).join());
        System.out.println(source.findsList(OneRecord.class, Stream.of("11", record2.getRecordid())));

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
