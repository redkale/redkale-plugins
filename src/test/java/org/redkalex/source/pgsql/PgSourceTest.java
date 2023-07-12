/*
 */
package org.redkalex.source.pgsql;

import java.util.Properties;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import org.redkale.boot.LoggingFileHandler;
import org.redkale.net.AsyncIOGroup;
import org.redkale.util.*;
import org.redkalex.source.base.SourceTest;

/**
 *
 * @author zhangjx
 */
public class PgSourceTest {

    public static void main(String[] args) throws Throwable {

        LoggingFileHandler.initDebugLogConfig();
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        ResourceFactory factory = ResourceFactory.create();
        factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup);

        Properties prop = new Properties();
        prop.setProperty("redkale.datasource.default.url", "jdbc:postgresql://127.0.0.1:5432/hello_world");
        prop.setProperty("redkale.datasource.default.table-autoddl", "true");
        prop.setProperty("redkale.datasource.default.user", "postgres");
        prop.setProperty("redkale.datasource.default.password", "1234");

        PgsqlDataSource source = new PgsqlDataSource();
        factory.inject(source);
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue("default"));
        System.out.println("---------");
        SourceTest.run(source);
        source.close();
    }
}
