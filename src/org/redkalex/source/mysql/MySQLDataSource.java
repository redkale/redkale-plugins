/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.net.URL;
import java.util.Properties;
import org.redkale.net.AsyncConnection;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 * 尚未实现
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(DataSource.class)
public abstract class MySQLDataSource extends DataSqlSource<AsyncConnection> {

    public MySQLDataSource(String unitName, URL persistxml, Properties readprop, Properties writeprop) {
        super(unitName, persistxml, readprop, writeprop);
    }
}
