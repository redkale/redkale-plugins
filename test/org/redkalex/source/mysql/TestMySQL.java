/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.redkale.source.DataSources;
import org.redkale.util.ObjectPool;

/**
 *
 * @author zhangjx
 */
public class TestMySQL {

    public static void main(String[] args) throws Throwable {
        final Logger logger = Logger.getLogger(TestMySQL.class.getSimpleName());
        final int capacity = 16 * 1024;
        final ObjectPool<ByteBuffer> bufferPool = new ObjectPool<>(new AtomicLong(), new AtomicLong(), 16,
            (Object... params) -> ByteBuffer.allocateDirect(capacity), null, (e) -> {
                if (e == null || e.isReadOnly() || e.capacity() != capacity) return false;
                e.clear();
                return true;
            });

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4, (Runnable r) -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        Properties prop = new Properties();
        prop.setProperty(DataSources.JDBC_URL, "jdbc:mysql://localhost:3306/platf_core?characterEncoding=utf8"); 
        prop.setProperty(DataSources.JDBC_USER, "root");
        prop.setProperty(DataSources.JDBC_PWD, "");
        MySQLDataSource source = new MySQLDataSource("", null, prop, prop);  
        source.getReadPoolSource().poll();
        source.directExecute("UPDATE almsrecord SET createtime = 0");
    }
}
