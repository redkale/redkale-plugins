/*
 */
package org.redkalex.properties.apollo;

import org.redkale.annotation.Priority;
import org.redkale.props.spi.PropertiesAgent;
import org.redkale.props.spi.PropertiesAgentProvider;
import org.redkale.util.*;

/** @author zhangjx */
@Priority(-800)
public class ApolloClientPropertiesAgentProvider implements PropertiesAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            Object.class.isAssignableFrom(com.ctrip.framework.apollo.core.ConfigConsts.class); // 试图加载相关类
            return new ApolloClientPropertiesAgent().acceptsConf(config);
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public PropertiesAgent createInstance() {
        return new ApolloClientPropertiesAgent();
    }
}
