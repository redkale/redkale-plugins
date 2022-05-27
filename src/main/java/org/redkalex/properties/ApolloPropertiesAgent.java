/*
 */
package org.redkalex.properties;

import java.util.Properties;
import org.redkale.boot.PropertiesAgent;
import org.redkale.util.*;

/**
 * Apollo 配置实现 https://github.com/apolloconfig/apollo
 *
 * TODO: 待实现
 *
 * @author zhangjx
 * @since 2.7.0
 */
public class ApolloPropertiesAgent extends PropertiesAgent {

    protected ResourceFactory factory;

    @Override
    public void compile(AnyValue conf) {
    }

    @Override
    public void init(ResourceFactory factory, Properties globalProperties, AnyValue conf) {
        this.factory = factory;
    }

    @Override
    public void destroy(AnyValue conf) {
    }

}
