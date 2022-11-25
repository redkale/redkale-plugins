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
@Priority(-900)
public class NacosPropertiesAgentProvider implements PropertiesAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        return new NacosPropertiesAgent().acceptsConf(config);
    }

    @Override
    public PropertiesAgent createInstance() {
        return new NacosPropertiesAgent();
    }

}
