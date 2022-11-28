package org.redkalex.properties.apollo;

import java.util.Properties;
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
        //可系统变量:  apollo.appid、apollo.meta、apollo.cluster、apollo.label、apollo.access-key.secret、apollo.namespace
        Properties agentConf = new Properties();
        propertiesConf.forEach((k, v) -> {
            String key = k.replace('-', '.').replace('_', '.');
            if (!key.startsWith("apollo.")) return;
            if (key.equals("apollo.app.id")) {
                key = "apollo.appid";
            } else if (key.equals("apollo.access.key.secret")) {
                key = "apollo.access-key.secret";
            }
            agentConf.put(key, v);
        });
        System.getProperties().forEach((k, v) -> {
            //支持 app.id、apollo.appid、apollo.meta、apollo.cluster、apollo.label、apollo.access-key.secret、apollo.namespace
            if (k.toString().startsWith("apollo") || k.toString().equals("app.id")) {
                String key = k.toString().replace('-', '.').replace('_', '.');
                if (key.equals("apollo.app.id") || key.equals("app.id")) {
                    key = "apollo.appid";
                } else if (key.equals("apollo.access.key.secret")) {
                    key = "apollo.access-key.secret";
                }
                if (!key.startsWith("apollo.")) return;
                agentConf.put(key, v);
            }
        });
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
    }

}
