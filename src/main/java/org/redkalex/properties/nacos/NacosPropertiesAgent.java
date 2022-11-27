/*
 */
package org.redkalex.properties.nacos;

import org.redkale.boot.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class NacosPropertiesAgent extends PropertiesAgent {

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
        boolean rs = config.getValue("nacos.serverAddr") != null
            || config.getValue("nacos-serverAddr") != null
            || config.getValue("nacos_serverAddr") != null
            || System.getProperty("nacos.serverAddr") != null
            || System.getProperty("nacos-serverAddr") != null
            || System.getProperty("nacos_serverAddr") != null;
        if (!rs) return rs;
        //nacos.data.group值的数据格式为: dataId1:group1,dataId2:group2  
        //多组数据用,分隔
        return config.getValue("nacos.data.group") != null
            || config.getValue("nacos-data-group") != null
            || config.getValue("nacos_data_group") != null
            || System.getProperty("nacos.data.group") != null
            || System.getProperty("nacos-data-group") != null
            || System.getProperty("nacos_data_group") != null;
    }

    @Override
    public void init(final Application application, final AnyValue propertiesConf) {

    }

    @Override
    public void destroy(AnyValue propertiesConf) {

    }
}
