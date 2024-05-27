/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cluster.consul;

import org.redkale.annotation.Priority;
import org.redkale.cluster.spi.*;
import org.redkale.util.*;

/** @author zhangjx */
@Priority(-800)
public class ConsulClusterAgentProvider implements ClusterAgentProvider {

	@Override
	public boolean acceptsConf(AnyValue config) {
		return new ConsulClusterAgent().acceptsConf(config);
	}

	@Override
	public ClusterAgent createInstance() {
		return new ConsulClusterAgent();
	}
}
