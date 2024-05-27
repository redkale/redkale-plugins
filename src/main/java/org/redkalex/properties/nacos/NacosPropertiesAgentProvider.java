/*
 */
package org.redkalex.properties.nacos;

import org.redkale.annotation.Priority;
import org.redkale.props.spi.PropertiesAgent;
import org.redkale.props.spi.PropertiesAgentProvider;
import org.redkale.util.AnyValue;

/** @author zhangjx */
@Priority(-900)
public class NacosPropertiesAgentProvider implements PropertiesAgentProvider {

	@Override
	public boolean acceptsConf(AnyValue config) {
		try {
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
