/*
 *
 */
package org.redkalex.source.parser;

import net.sf.jsqlparser.parser.*;
import org.redkale.annotation.Priority;
import org.redkale.source.*;
import org.redkale.source.spi.DataNativeSqlParserProvider;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
@Priority(-800)
public class DataNativeJsqlParserProvider implements DataNativeSqlParserProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            //加载jsqlparser类
            AbstractJSqlParser.class.isAssignableFrom(CCJSqlParser.class);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public DataNativeSqlParser createInstance() {
        return new DataNativeJsqlParser();
    }

}
