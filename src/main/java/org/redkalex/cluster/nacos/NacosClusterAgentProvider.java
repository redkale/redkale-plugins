/*
 */
package org.redkalex.cluster.nacos;

import org.redkale.annotation.Priority;
import org.redkale.cluster.spi.ClusterAgent;
import org.redkale.cluster.spi.ClusterAgentProvider;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Priority(-800)
public class NacosClusterAgentProvider implements ClusterAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        return new NacosClusterAgent().acceptsConf(config);
    }

    @Override
    public ClusterAgent createInstance() {
        return new NacosClusterAgent();
    }

}
