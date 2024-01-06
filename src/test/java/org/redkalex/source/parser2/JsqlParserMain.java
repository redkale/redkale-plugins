/*
 *
 */
package org.redkalex.source.parser2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import org.redkale.boot.LoggingFileHandler;
import org.redkale.inject.ResourceFactory;
import org.redkale.net.AsyncIOGroup;
import org.redkale.source.DataJdbcSource;
import org.redkale.util.AnyValue;
import org.redkalex.source.parser.DataNativeJsqlParser;

/**
 *
 * @author zhangjx
 */
public class JsqlParserMain {

    public static void main(String[] args) throws Throwable {

        LoggingFileHandler.initDebugLogConfig();
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        ResourceFactory factory = ResourceFactory.create();
        factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup);
        factory.register("", new DataNativeJsqlParser());

        Properties prop = new Properties();
        prop.setProperty("redkale.datasource.url", "jdbc:mysql://127.0.0.1:3389/aa_test?serverTimezone=UTC&characterEncoding=utf8"); //192.168.175.1  127.0.0.1 192.168.1.103
        prop.setProperty("redkale.datasource.maxconns", "1");
        prop.setProperty("redkale.datasource.table-autoddl", "true");
        prop.setProperty("redkale.datasource.user", "root");
        prop.setProperty("redkale.datasource.password", "");

        Connection conn = DriverManager.getConnection(prop.getProperty("redkale.datasource.url"),
            prop.getProperty("redkale.datasource.user"), prop.getProperty("redkale.datasource.password"));
        System.out.println(conn);
        conn.close();

        DataJdbcSource source = new DataJdbcSource();
        factory.inject(source);
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource"));
        System.out.println("---------");

        ForumInfo forum = source.nativeQueryOne(ForumInfo.class, "SELECT forum_groupid FROM forum_info WHERE forumid='ggmk'");
        System.out.println("ggmk对象: " + forum);
    }
}
