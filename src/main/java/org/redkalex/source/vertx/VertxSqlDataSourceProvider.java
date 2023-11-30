/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import org.redkale.annotation.Priority;
import org.redkale.source.*;
import static org.redkale.source.DataSources.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Priority(-300)
public class VertxSqlDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        String dbtype = config.getValue("dbtype");
        if (dbtype == null) {
            AnyValue read = config.getAnyValue("read");
            AnyValue node = read == null ? config : read;
            dbtype = parseDbtype(node.getValue(DATA_SOURCE_URL));
        }
        try {
            boolean pgsql = "postgresql".equalsIgnoreCase(dbtype);
            if (pgsql) {
                io.vertx.sqlclient.spi.Driver.class.isAssignableFrom(io.vertx.pgclient.spi.PgDriver.class); //试图加载PgClient相关类
                Class clazz = Thread.currentThread().getContextClassLoader().loadClass("io.vertx.pgclient.PgConnectOptions");
                RedkaleClassLoader.putReflectionClass(clazz.getName());
                RedkaleClassLoader.putReflectionPublicConstructors(clazz, clazz.getName());
                return pgsql;
            }
        } catch (Throwable t) {
            return false;
        }
        try {
            boolean mysql = "mysql".equalsIgnoreCase(dbtype);
            if (mysql) {
                io.vertx.sqlclient.spi.Driver.class.isAssignableFrom(io.vertx.mysqlclient.spi.MySQLDriver.class); //试图加载MySQLClient相关类
                Class clazz = Thread.currentThread().getContextClassLoader().loadClass("io.vertx.mysqlclient.MySQLConnectOptions");
                RedkaleClassLoader.putReflectionClass(clazz.getName());
                RedkaleClassLoader.putReflectionPublicConstructors(clazz, clazz.getName());
                return mysql;
            }
            return false;
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public DataSource createInstance() {
        return new VertxSqlDataSource();
    }

}
