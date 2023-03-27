/*
 */
package org.redkalex.source.mysql;

import java.util.Properties;
import org.redkale.boot.LoggingFileHandler;
import org.redkale.net.AsyncIOGroup;
import org.redkale.util.*;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_IOGROUP;

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
        factory.register(RESNAME_APP_CLIENT_IOGROUP, asyncGroup);

        Properties prop = new Properties();
        prop.setProperty("redkale.datasource[].url", "jdbc:mysql://127.0.0.1:3389/aa_test?useSSL=false&rewriteBatchedStatements=true&serverTimezone=UTC&characterEncoding=utf8");
        prop.setProperty("redkale.datasource[].table-autoddl", "true");
        prop.setProperty("redkale.datasource[].user", "root");
        prop.setProperty("redkale.datasource[].password", "");

        MysqlDataSource source = new MysqlDataSource();
        factory.inject(source);
        source.init(AnyValue.loadFromProperties(prop).getAnyValue("redkale").getAnyValue("datasource").getAnyValue(""));
        System.out.println("---------");
        World[] words = new World[10000];
        for (int i = 0; i < words.length; i++) {
            words[i] = new World();
            words[i].id = i + 1;
            words[i].randomNumber = i + 1;
        }
        source.insert(words);
        source.close();
    }
}
