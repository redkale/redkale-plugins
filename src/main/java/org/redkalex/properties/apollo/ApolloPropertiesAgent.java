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
 * Apollo 配置实现 https://github.com/apolloconfig/apollo
 *
 *
 * @author zhangjx
 * @since 2.8.0
 */
public class ApolloPropertiesAgent extends PropertiesAgent {

    protected ResourceFactory factory;

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        return System.getProperty(ConfigConsts.APOLLO_META_KEY) != null
            || config.getValue(ConfigConsts.APOLLO_META_KEY) != null
            || config.getValue(ConfigConsts.APOLLO_META_KEY.replace('.', '-')) != null
            || config.getValue(ConfigConsts.APOLLO_META_KEY.replace('.', '_')) != null;
    }

    @Override
    public void init(final Application application, final AnyValue propertiesConf) {
        //可系统变量:  apollo.appid、apollo.meta、apollo.cluster、apollo.label、apollo.namespace
        Properties agentConf = new Properties();
        propertiesConf.forEach((k, v) -> {
            String key = k.replace('-', '.').replace('_', '.');
            agentConf.put(key, v);
            if (key.equals("apollo.appid")) key = "apollo.app.id";
            if (key.startsWith("apollo.") && System.getProperty(key) == null) {
                if (key.startsWith("apollo.app.")) {
                    key = key.substring("apollo.".length());
                }
                System.setProperty(key, v);
            }
        });
        //远程请求具体类: com.ctrip.framework.apollo.internals.RemoteConfigRepository
        //String cluster = System.getProperty(ConfigConsts.APOLLO_CLUSTER_KEY, ConfigConsts.CLUSTER_NAME_DEFAULT);
        String namespaces = agentConf.getProperty("apollo.namespace", ConfigConsts.NAMESPACE_APPLICATION); //多个用,分隔
        for (String namespace : namespaces.split(";|,")) {
            if (namespace.trim().isEmpty()) continue;
            Config config = ConfigService.getConfig(namespace.trim());
            logger.log(Level.FINER, "apollo config size: " + config.getPropertyNames().size());
            config.addChangeListener(changeEvent -> {
                Properties props = new Properties();
                changeEvent.changedKeys().forEach(k -> {
                    String val = changeEvent.getChange(k).getNewValue();
                    props.put(k, val);
                });
                //更新全局配置项
                putResourceProperties(application, props);
            });
            //初始化配置项
            config.getPropertyNames().forEach(k -> {
                String val = config.getProperty(k, null);
                //更新全局配置项
                putResourceProperties(application, k, val);
            });
        }
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
    }

}
