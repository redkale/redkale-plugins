/*
 */
package org.redkalex.properties.apollo;

import javax.annotation.Priority;
import org.redkale.boot.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Priority(-800)
public class ApolloPropertiesAgentProvider implements PropertiesAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            Object.class.isAssignableFrom(com.ctrip.framework.apollo.core.ConfigConsts.class); //试图加载相关类
            return new ApolloPropertiesAgent().acceptsConf(config);
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public PropertiesAgent createInstance() {
        return new ApolloPropertiesAgent();
    }

}
