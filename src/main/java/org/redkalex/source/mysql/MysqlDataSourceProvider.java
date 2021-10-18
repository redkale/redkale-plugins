/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import javax.annotation.Priority;
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
        return "mysql".equalsIgnoreCase(config.getValue("dbtype"));
    }

    @Override
    public Class<? extends DataSource> sourceClass() {
        return MysqlDataSource.class;
    }

}
