/*
 */
package org.redkalex.properties.nacos;

import com.alibaba.nacos.api.*;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;
import java.util.Properties;
import java.util.logging.Level;
import org.redkale.boot.PropertiesAgent;
import org.redkale.util.*;

/**
 * Nacos 配置实现 https://github.com/alibaba/nacos
 *
 *
 * @author zhangjx
 * @since 2.8.0
 */
public class NacosPropertiesAgent extends PropertiesAgent {

    protected ResourceFactory factory;

    protected ConfigService configService;

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
        return config.getValue("nacos." + PropertyKeyConst.SERVER_ADDR) != null
            || config.getValue("nacos-" + PropertyKeyConst.SERVER_ADDR) != null
            || config.getValue("nacos_" + PropertyKeyConst.SERVER_ADDR) != null
            || System.getProperty("nacos." + PropertyKeyConst.SERVER_ADDR) != null
            || System.getProperty("nacos-" + PropertyKeyConst.SERVER_ADDR) != null
            || System.getProperty("nacos_" + PropertyKeyConst.SERVER_ADDR) != null;
    }

    @Override
    public void init(final ResourceFactory factory, final Properties globalProperties, final AnyValue propertiesConf) {
        this.factory = factory;
        try {
            Properties properties = new Properties();
            propertiesConf.forEach((k, v) -> {
                if (k.startsWith("nacos")) {
                    properties.put(k.substring("nacos".length() + 1), v); //+1指. - _
                }
            });
            System.getProperties().forEach((k, v) -> {
                //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
                if (k.toString().startsWith("nacos.") || k.toString().startsWith("nacos-") || k.toString().startsWith("nacos_")) {
                    properties.put(k.toString().substring("nacos".length() + 1), v); //+1指. - _
                }
            });
            this.configService = NacosFactory.createConfigService(properties);
        } catch (NacosException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
        if (this.configService != null) {
            try {
                this.configService.shutDown();
            } catch (NacosException e) {
                logger.log(Level.WARNING, "shutDown nacos client error", e);
            }
        }
    }

}
