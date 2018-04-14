/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.redkale.net.AsyncConnection;
import org.redkale.source.DataSources;
import org.redkale.util.ObjectPool;

/**
 *
 * @author zhangjx
 */
public class PgSQLDataSource {

    protected static final Logger logger = Logger.getLogger(PgSQLDataSource.class.getSimpleName());

    public static void main(String[] args) throws Throwable {
        final int capacity = 16 * 1024;
        final ObjectPool<ByteBuffer> bufferPool = new ObjectPool<>(new AtomicLong(), new AtomicLong(), 16,
            (Object... params) -> ByteBuffer.allocateDirect(capacity), null, (e) -> {
                if (e == null || e.isReadOnly() || e.capacity() != capacity) return false;
                e.clear();
                return true;
            });

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4, (Runnable r) -> {
            Thread t = new Thread(r);
            return t;
        });

        Properties prop = new Properties();
        prop.setProperty(DataSources.JDBC_URL, "jdbc:postgresql://tfb-database:5432/hello_world"); //192.168.175.1  127.0.0.1
        prop.setProperty(DataSources.JDBC_USER, "benchmarkdbuser");
        prop.setProperty(DataSources.JDBC_PWD, "benchmarkdbpass");
        PgPoolSource poolSource = new PgPoolSource("", prop, logger, bufferPool, executor);
        System.out.println("user:" + poolSource.getUser() + ", pass: " + poolSource.getPassword() + ", db: " + poolSource.getDefdb());

        AsyncConnection conn = poolSource.pollAsync().join();

    }
}
