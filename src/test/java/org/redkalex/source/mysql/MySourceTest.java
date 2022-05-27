/*
 */
package org.redkalex.source.mysql;

import java.util.Properties;
import static org.redkale.boot.Application.RESNAME_APP_ASYNCGROUP;
import org.redkale.boot.LoggingFileHandler;
import org.redkale.net.AsyncIOGroup;
import org.redkale.util.*;
import org.redkalex.source.base.*;

/**
 *
 * @author zhangjx
 */
public class MySourceTest {

    public static void main(String[] args) throws Throwable {

        LoggingFileHandler.initDebugLogConfig();
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        ResourceFactory factory = ResourceFactory.create();
        factory.register(RESNAME_APP_ASYNCGROUP, asyncGroup);

        Properties prop = new Properties();
        prop.setProperty("redkale.datasource[].url", "jdbc:mysql://127.0.0.1:3389/are_hello");
        prop.setProperty("redkale.datasource[].table-autoddl", "true");
        prop.setProperty("redkale.datasource[].user", "root");
        prop.setProperty("redkale.datasource[].password", "");

        MysqlDataSource source = new MysqlDataSource();
        factory.inject(source);
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue(""));
        System.out.println("---------");
        SourceTest.run(source);
        source.close();
    }
}
