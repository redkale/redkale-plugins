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
@Priority(-900)
public class ApolloPropertiesAgentProvider implements PropertiesAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        return new ApolloPropertiesAgent().acceptsConf(config);
    }

    @Override
    public PropertiesAgent createInstance() {
        return new ApolloPropertiesAgent();
    }

}
