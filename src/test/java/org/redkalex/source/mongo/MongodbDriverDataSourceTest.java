/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mongo;

import org.junit.jupiter.api.Assertions;
import com.mongodb.reactivestreams.client.MongoClient;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.redkale.convert.json.JsonConvert;
import org.redkale.source.*;
import static org.redkale.source.AbstractDataSource.*;
import org.redkale.util.*;
import org.redkale.util.AnyValue.DefaultAnyValue;

/**
 *
 * @author zhangjx
 */
public class MongodbDriverDataSourceTest {

    //需要本地装MongoDB，故不用Junit
    public static void main(String[] args) throws Throwable {
        DefaultAnyValue conf = DefaultAnyValue.create();
        conf.addValue(DATA_SOURCE_URL, "mongodb://localhost/admin");
        conf.addValue(DATA_SOURCE_MAXCONNS, "2");
        final MongodbDriverDataSource source = new MongodbDriverDataSource();
        source.init(conf);
        MongoClient mongoClient = source.readMongoClient;
        MongodbDriverDataSource.ReatorListFuture<String> future = new MongodbDriverDataSource.ReatorListFuture<>();
        mongoClient.listDatabaseNames().subscribe(future);
        System.out.println("数据库: " + future.get());

        TestRecord entity = new TestRecord();
        entity.setRecordid("r111");
        entity.setScore(100);
        entity.setStatus((short) 10);
        entity.setName("myname1");
        entity.setCreateTime(System.currentTimeMillis());

        TestRecord entity2 = new TestRecord();
        entity2.setRecordid("r222");
        entity2.setScore(122);
        entity2.setStatus((short) 20);
        entity2.setName("myname2");
        entity2.setCreateTime(System.currentTimeMillis());

        TestRecord entity3 = new TestRecord();
        entity3.setRecordid("r333");
        entity3.setScore(111);
        entity3.setStatus((short) 30);
        entity3.setName("myname3");
        entity3.setCreateTime(System.currentTimeMillis());

        if (source.exists(TestRecord.class, entity.getRecordid())) {
            System.out.println("删除结果: " + source.delete(TestRecord.class, entity.getRecordid(), entity2.getRecordid() + "3"));
        }
        if (source.exists(TestRecord.class, entity2.getRecordid())) {
            System.out.println("删除结果: " + source.delete(TestRecord.class, entity2.getRecordid()));
        }
        if (source.exists(TestRecord.class, entity3.getRecordid())) {
            System.out.println("删除结果: " + source.delete(TestRecord.class, entity3.getRecordid()));
        }
        Assertions.assertFalse(source.exists(TestRecord.class, entity.getRecordid()));
        source.insert(entity, entity2, entity3);
        int count = source.getNumberResult(TestRecord.class, FilterFunc.COUNT, 0, "score").intValue();
        Assertions.assertEquals(3, count);
        System.out.println("全部个数: " + count);
        System.out.println("去重个数: " + source.getNumberResult(TestRecord.class, FilterFunc.DISTINCTCOUNT, 0, "createTime"));
        System.out.println("求平均值: " + source.getNumberResult(TestRecord.class, FilterFunc.AVG, 0, "score"));
        System.out.println("求最大值: " + source.getNumberResult(TestRecord.class, FilterFunc.MAX, 0, "score"));
        System.out.println("求最小值: " + source.getNumberResult(TestRecord.class, FilterFunc.MIN, 0, "score"));
        System.out.println("求总和值: " + source.getNumberResult(TestRecord.class, FilterFunc.SUM, 0, "score"));
        System.out.println("应该存在: " + source.exists(TestRecord.class, entity.getRecordid()));
        System.out.println("数据更新前内容: " + source.find(TestRecord.class, entity.getRecordid()));
        source.updateColumn(TestRecord.class, entity.getRecordid(), ColumnValue.mov("name", "mynewname1"), ColumnValue.inc("score", 11), ColumnValue.mul("status", 4), ColumnValue.and("createTime", 7));
        System.out.println("数据更新后内容: " + source.find(TestRecord.class, entity.getRecordid() + "3"));
        //source.updateColumn(TestRecord.class, entity.getRecordid(), ColumnValue.div("status", 10), new ColumnValue("createTime", ColumnExpress.MOD, 3));
        //System.out.println("数据更新后内容: " + source.find(TestRecord.class, entity.getRecordid()));
        System.out.println("单个状态: " + source.findColumn(TestRecord.class, "status", FilterNode.create("createTime", entity.getCreateTime())));
        System.out.println("单个状时间: " + source.findColumn(TestRecord.class, "createTime", entity.getRecordid()));
        System.out.println("查询所有: " + source.querySheet(TestRecord.class, SelectColumn.includes("recordid", "score"), new Flipper(2, 1, "score ASC"), (FilterNode) null));

        ColumnNode[] cns = Utility.ofArray(ColumnFuncNode.count("recordid"), ColumnFuncNode.sum("score"));
        System.out.println("部分统计: " + JsonConvert.root().convertTo(source.queryColumnMap(TestRecord.class, cns, Utility.ofArray("createTime"), FilterNode.create("createTime", FilterExpress.GREATERTHAN, 1))));
        System.out.println("部分统计: " + JsonConvert.root().convertTo(source.queryColumnMap(TestRecord.class, "createTime", FilterFunc.SUM, "score", FilterNode.create("createTime", FilterExpress.GREATERTHAN, 1))));
        System.out.println("部分统计: " + JsonConvert.root().convertTo(source.getNumberMap(TestRecord.class, FilterFuncColumn.create(FilterFunc.COUNT, "recordid"), FilterFuncColumn.create(FilterFunc.SUM, "score"))));

        entity.setScore(entity.getScore() + 1);
        entity.setCreateTime(entity.getCreateTime() + 1);
        entity2.setScore(entity2.getScore() + 1);
        entity2.setCreateTime(entity2.getCreateTime() + 1);
        entity3.setScore(entity3.getScore() + 1);
        entity3.setCreateTime(entity3.getCreateTime() + 1);
        System.out.println("批量更新: " + source.update(entity, entity2, entity3));
        System.out.println("数据更新后内容: " + source.find(TestRecord.class, entity.getRecordid()));
        System.out.println("数据更新后内容: " + source.find(TestRecord.class, entity2.getRecordid()));
        System.out.println("数据更新后内容: " + source.find(TestRecord.class, entity3.getRecordid()));

        int threads = 200;
        long s = System.currentTimeMillis();
        CountDownLatch stc = new CountDownLatch(threads);
        CountDownLatch cdl = new CountDownLatch(threads);
        AtomicInteger a = new AtomicInteger();
        for (int i = 0; i < threads; i++) {
            new Thread() {
                public void run() {
                    try {
                        stc.countDown();
                        stc.await();
                        source.queryColumnList("score", TestRecord.class, (FilterNode) null);
                        source.updateColumn(TestRecord.class, entity.getRecordid(), ColumnValue.mov("name", "mynewname1" + a.incrementAndGet()));
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        cdl.countDown();
                    }
                }

            }.start();
        }
        cdl.await();
        long e = System.currentTimeMillis() - s;
        System.out.println("批量请求:  耗时: " + e + " ms");
    }

}
