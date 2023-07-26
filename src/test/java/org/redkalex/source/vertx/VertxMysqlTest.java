/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import java.util.Properties;
import org.redkale.source.FilterNode;
import org.redkale.util.AnyValue;
import org.redkalex.source.base.IncreWorld;

/**
 *
 * @author zhangjx
 */
public class VertxMysqlTest {

    public static void main(String[] args) throws Throwable {
        Properties prop = new Properties();
        prop.setProperty("redkale.datasource.default.maxconns", "2");

        prop.setProperty("redkale.datasource.default.url", "jdbc:mysql://127.0.0.1:3389/aa_test?useSSL=false&amp;rewriteBatchedStatements=true&amp;serverTimezone=UTC&amp;characterEncoding=utf8"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty("redkale.datasource.default.table-autoddl", "true");
        prop.setProperty("redkale.datasource.default.user", "root");
        prop.setProperty("redkale.datasource.default.password", "");
        if (false) {
            prop.setProperty("redkale.datasource.default.url", "jdbc:postgresql://127.0.0.1:5432/hello_world");
            prop.setProperty("redkale.datasource.default.table-autoddl", "true");
            prop.setProperty("redkale.datasource.default.user", "postgres");
            prop.setProperty("redkale.datasource.default.password", "1234");
        }
        final VertxSqlDataSource source = new VertxSqlDataSource();
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue("default"));

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

        System.out.println("运行完成");
        source.destroy(null);
    }
}
