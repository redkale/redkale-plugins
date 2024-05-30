/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.impl.PoolBase;
import java.lang.reflect.Field;
import java.util.*;
import org.redkale.boot.LoggingBaseHandler;
import org.redkale.inject.ResourceFactory;
import org.redkale.source.*;
import org.redkale.util.*;
import org.redkalex.source.base.IncreWorld;
import org.redkalex.source.parser.DataNativeJsqlParser;

/** @author zhangjx */
public class VertxMysqlTest {

    public static void main(String[] args) throws Throwable {
        Properties prop = new Properties();
        prop.setProperty("redkale.datasource.default.maxconns", "2");

        prop.setProperty(
                "redkale.datasource.default.url",
                "jdbc:mysql://127.0.0.1:3389/aa_test?useSSL=false&amp;rewriteBatchedStatements=true&amp;serverTimezone=UTC&amp;characterEncoding=utf8"); // 192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty("redkale.datasource.default.table-autoddl", "true");
        prop.setProperty("redkale.datasource.default.user", "root");
        prop.setProperty("redkale.datasource.default.password", "");
        if (false) {
            prop.setProperty("redkale.datasource.default.url", "jdbc:postgresql://127.0.0.1:5432/hello_world");
            prop.setProperty("redkale.datasource.default.table-autoddl", "true");
            prop.setProperty("redkale.datasource.default.user", "postgres");
            prop.setProperty("redkale.datasource.default.password", "1234");
        }
        LoggingBaseHandler.initDebugLogConfig();
        ResourceFactory factory = ResourceFactory.create();
        factory.register("", new DataNativeJsqlParser());
        final VertxSqlDataSource source = new VertxSqlDataSource();
        factory.inject(source);
        source.init(AnyValue.loadFromProperties(prop)
                .getAnyValue("redkale")
                .getAnyValue("datasource")
                .getAnyValue("default"));
        Pool pool = source.readThreadPool;
        Field f = PoolBase.class.getDeclaredField("delegate");
        f.setAccessible(true);
        Object delegate = f.get(pool);
        System.out.println("client信息: " + delegate.getClass());
        source.dropTable(IncreWorld.class);
        IncreWorld in1 = new IncreWorld();
        in1.setRandomNumber(11);
        IncreWorld in2 = new IncreWorld();
        in2.setRandomNumber(22);
        source.insert(in1, in2);
        System.out.println(in1);
        System.out.println(in2);

        source.dropTable(TestRecord.class);
        TestRecord entity = new TestRecord();
        entity.setRecordid("r223");
        entity.setScore(200);
        entity.setStatus((short) 10);
        entity.setName("myname2");
        entity.setCreateTime(System.currentTimeMillis());
        source.insert(entity);
        source.delete(TestRecord.class, (FilterNode) null);
        source.insert(entity);
        source.delete(TestRecord.class, (FilterNode) null);
        System.out.println(source.insert(entity));

        System.out.println(source.queryList(TestRecord.class));
        entity.setName(entity.getName() + "-new");
        source.update(entity);
        System.out.println(source.queryList(TestRecord.class));
        entity.setName(entity.getName() + "-new2");
        source.updateColumn(entity, "name");
        System.out.println(source.queryList(TestRecord.class));
        System.out.println(source.find(TestRecord.class, entity.getRecordid()));

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

        System.out.println("运行完成");
        source.destroy(null);
    }
}
