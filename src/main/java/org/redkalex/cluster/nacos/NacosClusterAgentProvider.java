/*
 */
package org.redkalex.cluster.nacos;

import javax.annotation.Priority;
import org.redkale.cluster.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Priority(-700)
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
