/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import java.util.Properties;
import org.redkale.source.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class VertxMysqlTest {

    public static void main(String[] args) throws Throwable {
        Properties prop = new Properties();
        prop.setProperty("redkale.datasource[].url", "jdbc:mysql://127.0.0.1:3389/redsns_platf?useSSL=false&amp;rewriteBatchedStatements=true&amp;serverTimezone=UTC&amp;characterEncoding=utf8"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty("redkale.datasource[].maxconns", "2");
        prop.setProperty("redkale.datasource[].table-autoddl", "true");
        prop.setProperty("redkale.datasource[].user", "root");
        prop.setProperty("redkale.datasource[].password", "");

        final VertxSqlDataSource source = new VertxSqlDataSource();
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue(""));

        TestRecord entity = new TestRecord();
        entity.setRecordid("r223");
        entity.setScore(200);
        entity.setStatus((short) 10);
        entity.setName("myname2");
        entity.setCreateTime(System.currentTimeMillis());
        source.delete(TestRecord.class, (FilterNode) null);
        System.out.println(source.insert(entity));

        System.out.println(source.queryList(TestRecord.class));
        entity.setName(entity.getName()+"-new");
        source.update(entity);
        System.out.println(source.queryList(TestRecord.class));
        entity.setName(entity.getName()+"-new2");
        source.updateColumn(entity, "name");
        System.out.println(source.queryList(TestRecord.class));
        System.out.println(source.find(TestRecord.class, entity.getRecordid()));
        System.out.println("运行完成");
        source.destroy(null);
    }
}
