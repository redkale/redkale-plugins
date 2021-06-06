/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cluster.consul;

import org.redkale.cluster.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class ConsulClusterAgentLoader implements ClusterAgentLoader {

    @Override
    public boolean match(AnyValue config) {
        try {
            ConsulClusterAgent source = ConsulClusterAgent.class.getConstructor().newInstance();
            return source.match(config);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public Class<? extends ClusterAgent> agentClass() {
        return ConsulClusterAgent.class;
    }

}
