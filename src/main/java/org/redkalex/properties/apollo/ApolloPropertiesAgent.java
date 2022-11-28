package org.redkalex.properties.apollo;

import org.redkale.boot.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class ApolloPropertiesAgent extends PropertiesAgent {

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        return (System.getProperty("apollo.meta") != null
            || config.getValue("apollo.meta") != null
            || config.getValue("apollo-meta") != null
            || config.getValue("apollo_meta") != null)
            && (System.getProperty("apollo.appid") != null
            || System.getProperty("app.id") != null
            || config.getValue("apollo.appid") != null
            || config.getValue("app.id") != null
            || config.getValue("apollo-appid") != null
            || config.getValue("apollo_appid") != null);
    }

    @Override
    public void init(final Application application, final AnyValue propertiesConf) {

    }

    @Override
    public void destroy(AnyValue propertiesConf) {
    }

}
