/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mongodb;

import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
public class MongodbSourceLoader implements SourceLoader {

    @Override
    public String dbtype() {
        return "mongodb";
    }

    @Override
    public Class<? extends DataSource> dataSourceClass() {
        return MongodbDataSource.class;
    }
}
