/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mongo;

import javax.annotation.Priority;
import org.redkale.source.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
@Priority(-300)
public class MongodbDriverDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            MongodbDriverDataSource source = MongodbDriverDataSource.class.getConstructor().newInstance();
            return "mongodb".equalsIgnoreCase(config.getValue("dbtype"));
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public Class<? extends DataSource> sourceClass() {
        return MongodbDriverDataSource.class;
    }
}
