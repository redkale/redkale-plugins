/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
public class MySQLSourceLoader implements SourceLoader {

    @Override
    public String dbtype() {
        return "mysql";
    }

    @Override
    public Class<? extends DataSource> dataSourceClass() {
        return MySQLDataSource.class;
    }

}
