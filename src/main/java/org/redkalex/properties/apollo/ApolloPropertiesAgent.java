/*
 */
package org.redkalex.properties.apollo;

import com.ctrip.framework.apollo.*;
import com.ctrip.framework.apollo.core.ConfigConsts;
import java.util.Properties;
import org.redkale.boot.PropertiesAgent;
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
    public void init(final ResourceFactory factory, final Properties globalProperties, final AnyValue propertiesConf) {
        this.factory = factory;
        propertiesConf.forEach((k, v) -> {
            if (k.startsWith("apollo.")) {
                System.setProperty(k, v);
            } else if (k.startsWith("apollo_")) {
                System.setProperty(k.replace('_', '.'), v);
            }
        });
        String url = propertiesConf.get(PROP_KEY_URL);
        String meta = System.getProperty(ConfigConsts.APOLLO_META_KEY);
        if (url == null && meta == null) {
            throw new IllegalArgumentException("not found " + ConfigConsts.APOLLO_META_KEY + " config value");
        }
        if (meta == null && url != null) {
            System.setProperty(ConfigConsts.APOLLO_META_KEY, meta);
        }
        String namespace = propertiesConf.getOrDefault(PROP_KEY_NAMESPACE, PROP_NAMESPACE_APPLICATION);
        Config config = ConfigService.getConfig(namespace);
        config.addChangeListener(changeEvent -> {
            changeEvent.changedKeys().forEach(k -> {
                factory.register(true, getKeyResourceName(k), changeEvent.getChange(k).getNewValue());
            });
        });
        config.getPropertyNames().forEach(k -> {
            factory.register(true, getKeyResourceName(k), config.getProperty(k, null));
        });
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
    }

}
