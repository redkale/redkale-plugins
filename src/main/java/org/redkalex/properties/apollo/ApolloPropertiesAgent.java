/*
 */
package org.redkalex.properties.apollo;

import com.ctrip.framework.apollo.*;
import com.ctrip.framework.apollo.core.ConfigConsts;
import java.util.Properties;
import java.util.logging.Level;
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
    public boolean acceptsConf(AnyValue config) {
        return System.getProperty(ConfigConsts.APOLLO_META_KEY) != null
            || config.getValue(ConfigConsts.APOLLO_META_KEY) != null
            || config.getValue(ConfigConsts.APOLLO_META_KEY.replace('.', '-')) != null
            || config.getValue(ConfigConsts.APOLLO_META_KEY.replace('.', '_')) != null;
    }

    @Override
    public void init(final ResourceFactory factory, final Properties appProperties, Properties sourceProperties, final AnyValue propertiesConf) {
        this.factory = factory;
        boolean finer = logger.isLoggable(Level.FINER);
        propertiesConf.forEach((k, v) -> {
            String key = k.replace('-', '.').replace('_', '.');
            if (key.equals("apollo.appid")) key = "apollo.app.id";
            if (key.startsWith("apollo.") && System.getProperty(key) == null) {
                if (key.startsWith("apollo.app.")) {
                    key = key.substring("apollo.".length());
                }
                System.setProperty(key, v);
            }
        });

        String namespace = propertiesConf.getOrDefault("namespace", ConfigConsts.NAMESPACE_APPLICATION);
        Config config = ConfigService.getConfig(namespace);
        if (finer) logger.log(Level.FINER, "apollo config size: " + config.getPropertyNames().size());
        config.addChangeListener(changeEvent -> {
            Properties props = new Properties();
            changeEvent.changedKeys().forEach(k -> {
                String key = getKeyResourceName(k);
                String val = changeEvent.getChange(k).getNewValue();
                props.put(key, val);
            });
            //更新全局配置项
            putProperties(appProperties, sourceProperties, props);
            //需要一次性提交所有变更的配置项
            factory.register(props);
        });
        //初始化配置项
        config.getPropertyNames().forEach(k -> {
            String key = getKeyResourceName(k);
            String val = config.getProperty(k, null);
            //更新全局配置项
            putProperties(appProperties, sourceProperties, key, val);
            //依赖注入配置项
            factory.register(false, key, val);
        });
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
    }

}
