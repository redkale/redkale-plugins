/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import org.redkale.source.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class OpenSearchSourceLoader implements DataSourceLoader {

    @Override
    public boolean match(AnyValue config) {
        return "search".equalsIgnoreCase(config.getValue("dbtype"));
    }

    @Override
    public Class<? extends DataSource> sourceClass() {
        return OpenSearchSource.class;
    }
}
