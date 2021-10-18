/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import java.util.Properties;
import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
public class VertxMysqlTest {

    public static void main(String[] args) throws Throwable {
        Properties conf = new Properties();
        conf.put(DataSources.JDBC_URL, "jdbc:mysql://127.0.0.1:3306/redsns_platf?useSSL=false&rewriteBatchedStatements=true&serverTimezone=UTC&characterEncoding=utf8");
        conf.put(DataSources.JDBC_CONNECTIONS_LIMIT, "2");
        conf.put(DataSources.JDBC_USER, "root");
        conf.put(DataSources.JDBC_PWD, "12345678");

        final VertxSqlDataSource source = new VertxSqlDataSource("", null, "mysql", conf, conf);
        source.init(null);

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
