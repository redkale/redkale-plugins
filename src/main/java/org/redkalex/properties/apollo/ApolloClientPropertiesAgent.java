/*
 */
package org.redkalex.properties.apollo;

import com.ctrip.framework.apollo.*;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.model.ConfigChange;
import java.util.*;
import java.util.logging.Level;
import org.redkale.boot.*;
import org.redkale.inject.ResourceEvent;
import org.redkale.props.spi.PropertiesAgent;
import org.redkale.util.*;

/**
 * 依赖于apollo-client实现的Apollo配置 https://github.com/apolloconfig/apollo
 *
 * @author zhangjx
 * @since 2.8.0
 */
public class ApolloClientPropertiesAgent extends PropertiesAgent {

    @Override
    public void compile(final AnyValue propertiesConf) {
        // do nothing
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        return ApolloPropertiesAgent.acceptsConf0(config);
    }

    @Override
    public Map<String, Properties> init(final Application application, final AnyValue propertiesConf) {
        // 可系统变量:  apollo.appid、apollo.meta、apollo.cluster、apollo.label、apollo.access-key.secret、apollo.namespace
        Properties agentConf = new Properties();
        propertiesConf.forEach((k, v) -> {
            String key = k.contains(".") && k.contains("-") ? k : k.replace('-', '.');
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
        // 远程请求具体类: com.ctrip.framework.apollo.internals.RemoteConfigRepository
        // String cluster = System.getProperty(ConfigConsts.APOLLO_CLUSTER_KEY, ConfigConsts.CLUSTER_NAME_DEFAULT);
        String namespaces = agentConf.getProperty(
                "apollo.namespace",
                System.getProperty("apollo.namespace", ConfigConsts.NAMESPACE_APPLICATION)); // 多个用,分隔
        Map<String, Properties> result = new LinkedHashMap<>();
        for (String namespace0 : namespaces.split("[;,]")) {
            if (namespace0.trim().isEmpty()) {
                continue;
            }
            String namespace = namespace0.trim();
            Config config = ConfigService.getConfig(namespace);
            logger.log(
                    Level.FINER,
                    "Apollo config (namespace=" + namespace + ") size: "
                            + config.getPropertyNames().size());
            config.addChangeListener(changeEvent -> {
                List<ResourceEvent> events = new ArrayList<>();
                changeEvent.changedKeys().forEach(k -> {
                    ConfigChange cc = changeEvent.getChange(k);
                    if (cc != null) {
                        events.add(ResourceEvent.create(k, cc.getNewValue(), cc.getOldValue()));
                    }
                });
                // 更新全局配置项
                onEnvironmentUpdated(application, namespace, events);
            });
            // 初始化配置项
            Properties props = new Properties();
            config.getPropertyNames().forEach(k -> {
                String val = config.getProperty(k, null);
                props.put(k, val);
            });
            result.put(namespace.trim(), props);
        }
        return result;
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
        // do nothing
    }
}
