/*
 */
package org.redkalex.cluster.nacos;

import javax.annotation.Priority;
import org.redkale.cluster.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
@Priority(-700)
public class NacosClusterAgentProvider implements ClusterAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            NacosClusterAgent source = NacosClusterAgent.class.getConstructor().newInstance();
            return source.acceptsConf(config);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public Class<? extends ClusterAgent> agentClass() {
        return NacosClusterAgent.class;
    }

}
