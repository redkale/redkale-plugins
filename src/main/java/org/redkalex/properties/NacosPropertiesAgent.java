/*
 */
package org.redkalex.properties;

import java.util.Properties;
import org.redkale.boot.PropertiesAgent;
import org.redkale.util.*;

/**
 * Nacos 配置实现 https://github.com/alibaba/nacos
 *
 * TODO: 待实现
 *
 * @author zhangjx
 * @since 2.7.0
 */
public class NacosPropertiesAgent extends PropertiesAgent {

    protected ResourceFactory factory;

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public void init(final ResourceFactory factory, final Properties globalProperties, final AnyValue propertiesConf) {
        this.factory = factory;
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
    }

}
