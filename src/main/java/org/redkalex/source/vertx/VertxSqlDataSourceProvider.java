/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import javax.annotation.Priority;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Priority(-100)
public class VertxSqlDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        final String dbtype = config.getValue("dbtype");
        try {
            boolean pgsql = "postgresql".equalsIgnoreCase(dbtype);
            if (pgsql) {
                io.vertx.sqlclient.Pool.class.isAssignableFrom(io.vertx.pgclient.PgPool.class); //试图加载PgClient相关类
                VertxSqlDataSource.class.getDeclaredConstructor().newInstance();
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
                io.vertx.sqlclient.Pool.class.isAssignableFrom(io.vertx.mysqlclient.MySQLPool.class); //试图加载MySQLClient相关类
                VertxSqlDataSource.class.getDeclaredConstructor().newInstance();
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
    public Class<? extends DataSource> sourceClass() {
        return VertxSqlDataSource.class;
    }

}
