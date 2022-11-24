/*
 */
package org.redkalex.properties.nacos;

import com.alibaba.nacos.api.*;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import java.util.Properties;
import org.redkale.boot.PropertiesAgent;
import org.redkale.util.*;

/**
 * Nacos 配置实现 https://github.com/alibaba/nacos
 *
 *
 * @author zhangjx
 * @since 2.8.0
 */
public class NacosPropertiesAgent extends PropertiesAgent {

    protected ResourceFactory factory;

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public void init(final ResourceFactory factory, final Properties globalProperties, final AnyValue propertiesConf) {
        this.factory = factory;
        try {
            Properties properties = new Properties();
            properties.put(PropertyKeyConst.SERVER_ADDR, "");
            ConfigService configService = NacosFactory.createConfigService(properties);
        } catch (NacosException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
    }

}
