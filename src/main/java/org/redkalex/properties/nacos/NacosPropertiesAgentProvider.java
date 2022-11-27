/*
 */
package org.redkalex.properties.nacos;

import javax.annotation.Priority;
import org.redkale.boot.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Priority(-800)
public class NacosPropertiesAgentProvider implements PropertiesAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            Object.class.isAssignableFrom(com.alibaba.nacos.api.config.ConfigService.class); //试图加载相关类
            return new NacosPropertiesAgent().acceptsConf(config);
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public PropertiesAgent createInstance() {
        return new NacosPropertiesAgent();
    }

}
