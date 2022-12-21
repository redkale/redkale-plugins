/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.annotation.Priority;
import org.redkale.source.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
@Priority(-900)
public class MysqlDataSourceProvider implements DataSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        String dbtype = config.getValue("dbtype");
        if (dbtype == null) {
            AnyValue read = config.getAnyValue("read");
            AnyValue node = read == null ? config : read;
            dbtype = AbstractDataSource.parseDbtype(node.getValue(AbstractDataSource.DATA_SOURCE_URL));
        }
        return "mysql".equalsIgnoreCase(dbtype);
    }

    @Override
    public DataSource createInstance() {
        return new MysqlDataSource();
    }

}
