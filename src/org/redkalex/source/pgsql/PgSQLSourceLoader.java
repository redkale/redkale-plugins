/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
public class PgSQLSourceLoader implements SourceLoader {

    @Override
    public String dbtype() {
        return "postgresql";
    }

    @Override
    public Class<? extends DataSource> dataSourceClass() {
        return PgSQLDataSource.class;
    }

}
