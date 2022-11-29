/*
 */
package org.redkalex.properties.apollo;

import com.ctrip.framework.apollo.*;
import com.ctrip.framework.apollo.core.ConfigConsts;
import java.util.*;
import java.util.logging.Level;
import org.redkale.boot.*;
import org.redkale.util.*;

/**
 * 依赖于apollo-client实现的Apollo配置 https://github.com/apolloconfig/apollo
 *
 *
 * @author zhangjx
 * @since 2.8.0
 */
public class ApolloClientPropertiesAgent extends PropertiesAgent {

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
            agentConf.put(key, v);
            if (key.equals("apollo.appid")) {
                key = "apollo.app.id";
            } else if (key.equals("apollo.access.key.secret")) {
                agentConf.remove(key);
                key = "apollo.access-key.secret";
                agentConf.put(key, v);
            }
            if (key.startsWith("apollo.") && System.getProperty(key) == null) {
                if (key.startsWith("apollo.app.")) {
                    key = key.substring("apollo.".length());
                }
                System.setProperty(key, v);
            }
        });
        //远程请求具体类: com.ctrip.framework.apollo.internals.RemoteConfigRepository
        //String cluster = System.getProperty(ConfigConsts.APOLLO_CLUSTER_KEY, ConfigConsts.CLUSTER_NAME_DEFAULT);
        String namespaces = agentConf.getProperty("apollo.namespace", System.getProperty("apollo.namespace", ConfigConsts.NAMESPACE_APPLICATION)); //多个用,分隔
        for (String namespace : namespaces.split(";|,")) {
            if (namespace.trim().isEmpty()) continue;
            Config config = ConfigService.getConfig(namespace.trim());
            logger.log(Level.INFO, "apollo config (namespace=" + namespace + ") size: " + config.getPropertyNames().size());
            config.addChangeListener(changeEvent -> {
                Properties props = new Properties();
                changeEvent.changedKeys().forEach(k -> {
                    String val = changeEvent.getChange(k).getNewValue();
                    props.put(k, val);
                });
                //更新全局配置项
                putEnvironmentProperties(application, props);
            });
            //初始化配置项
            config.getPropertyNames().forEach(k -> {
                String val = config.getProperty(k, null);
                //更新全局配置项
                putEnvironmentProperties(application, k, val);
            });
        }
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
    }

}
