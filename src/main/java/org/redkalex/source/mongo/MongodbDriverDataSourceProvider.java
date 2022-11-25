/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mongo;

import javax.annotation.Priority;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Priority(-300)
public class MongodbDriverDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            Object.class.isAssignableFrom(com.mongodb.reactivestreams.client.MongoClient.class); //试图加载MongoClient相关类
            RedkaleClassLoader.putReflectionPublicConstructors(MongodbDriverDataSource.class, MongodbDriverDataSource.class.getName());
            MongodbDriverDataSource source = MongodbDriverDataSource.class.getConstructor().newInstance();
            String dbtype = config.getValue("dbtype");
            if (dbtype == null) {
                AnyValue read = config.getAnyValue("read");
                AnyValue node = read == null ? config : read;
                dbtype = AbstractDataSource.parseDbtype(node.getValue(AbstractDataSource.DATA_SOURCE_URL));
            }
            return "mongodb".equalsIgnoreCase(dbtype);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public DataSource createInstance() {
        return new MongodbDriverDataSource();
    }
}
