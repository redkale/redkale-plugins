/*
 */
package org.redkalex.properties.nacos;

import com.alibaba.nacos.api.*;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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

    protected ExecutorService configExecutor;

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
        boolean rs = config.getValue("nacos." + PropertyKeyConst.SERVER_ADDR) != null
            || config.getValue("nacos-" + PropertyKeyConst.SERVER_ADDR) != null
            || config.getValue("nacos_" + PropertyKeyConst.SERVER_ADDR) != null
            || System.getProperty("nacos." + PropertyKeyConst.SERVER_ADDR) != null
            || System.getProperty("nacos-" + PropertyKeyConst.SERVER_ADDR) != null
            || System.getProperty("nacos_" + PropertyKeyConst.SERVER_ADDR) != null;
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
    public void init(final ResourceFactory factory, final Properties appProperties, Properties sourceProperties, final AnyValue propertiesConf) {
        this.factory = factory;
        try {
            Properties properties = new Properties();
            StringWrapper dataGroups = new StringWrapper();
            propertiesConf.forEach((k, v) -> {
                String key = k.replace('-', '.').replace('_', '.');
                if (key.equals("nacos.data.group")) {
                    dataGroups.setValue(v);
                } else if (key.startsWith("nacos.")) {
                    properties.put(key.substring("nacos.".length()), v);
                }
            });
            System.getProperties().forEach((k, v) -> {
                //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
                if (k.toString().startsWith("nacos")) {
                    String key = k.toString().replace('-', '.').replace('_', '.');
                    if (key.equals("nacos.data.group")) {
                        dataGroups.setValue(v.toString());
                    } else if (key.startsWith("nacos.")) {
                        properties.put(key.substring("nacos.".length()), v);
                    }
                }
            });
            final AtomicInteger counter = new AtomicInteger();
            this.configExecutor = Executors.newFixedThreadPool(2, (Runnable r) -> {
                int c = counter.incrementAndGet();
                String threadname = "Redkale-NacosListenerThread-" + (c > 9 ? c : ("0" + c));
                Thread t = new Thread(r, threadname);
                return t;
            });
            this.configService = NacosFactory.createConfigService(properties);
            for (String dataGroup : dataGroups.getValue().split(",")) {
                String dataId0 = null;
                String group0 = null;
                int pos = dataGroup.indexOf(':');
                if (pos > 0) {
                    dataId0 = dataGroup.substring(0, pos).trim();
                    group0 = dataGroup.substring(pos + 1).trim();
                } else {
                    dataId0 = dataGroup.trim();
                    group0 = "";
                }
                if (dataId0.isEmpty()) continue;
                if (group0.isEmpty()) group0 = "DEFAULT_GROUP";
                final String dataId = dataId0;
                final String group = group0;
                final String content = configService.getConfigAndSignListener(dataId, group, 3_000, new Listener() {
                    @Override
                    public Executor getExecutor() {
                        return configExecutor;
                    }

                    @Override
                    public void receiveConfigInfo(String configInfo) {
                        updateConent(factory, appProperties, sourceProperties, dataId, configInfo);
                    }
                });
                updateConent(factory, appProperties, sourceProperties, dataId, content);
            }
        } catch (NacosException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateConent(final ResourceFactory factory, final Properties appProperties, Properties sourceProperties, String dataId, String content) {
        Properties props = new Properties();
        try {
            props.load(new StringReader(content));
        } catch (IOException e) {
            logger.log(Level.SEVERE, "load nacos content (dataId = " + dataId + ") error", e);
            return;
        }
        if (props.isEmpty()) return;
        Properties newProps = new Properties();
        props.forEach((k, v) -> {
            newProps.put(getKeyResourceName(k.toString()), v);
        });
        //更新全局配置项
        putProperties(appProperties, sourceProperties, newProps);
        //需要一次性提交所有变更的配置项
        factory.register(newProps);
        logger.log(Level.FINER, "nacos config(" + dataId + ") size: " + newProps.size());
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
