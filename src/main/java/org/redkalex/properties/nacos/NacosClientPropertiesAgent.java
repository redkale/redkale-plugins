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
import org.redkale.boot.*;
import org.redkale.util.*;

/**
 * 依赖于nacos-client实现的Nacos配置 https://github.com/alibaba/nacos
 *
 *
 * @author zhangjx
 * @since 2.8.0
 */
public class NacosClientPropertiesAgent extends PropertiesAgent {

    protected ConfigService configService;

    protected ExecutorService listenExecutor;

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
        //nacos.data.group值的数据格式为: dataId1:group1:tenant1,dataId2:group2:tenant2 
        //多组数据用,分隔
        return false && (config.getValue("nacos.serverAddr") != null
            || config.getValue("nacos-serverAddr") != null
            || config.getValue("nacos_serverAddr") != null
            || System.getProperty("nacos.serverAddr") != null
            || System.getProperty("nacos-serverAddr") != null
            || System.getProperty("nacos_serverAddr") != null)
            && (config.getValue("nacos.data.group") != null
            || config.getValue("nacos-data-group") != null
            || config.getValue("nacos_data_group") != null
            || System.getProperty("nacos.data.group") != null
            || System.getProperty("nacos-data-group") != null
            || System.getProperty("nacos_data_group") != null);
    }

    @Override
    public void init(final Application application, final AnyValue propertiesConf) {
        try {
            Properties agentConf = new Properties();
            StringWrapper dataWrapper = new StringWrapper();
            propertiesConf.forEach((k, v) -> {
                String key = k.replace('-', '.').replace('_', '.');
                if (key.equals("nacos.data.group")) {
                    dataWrapper.setValue(v);
                } else if (key.startsWith("nacos.")) {
                    agentConf.put(key.substring("nacos.".length()), v);
                }
            });
            System.getProperties().forEach((k, v) -> {
                //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
                if (k.toString().startsWith("nacos")) {
                    String key = k.toString().replace('-', '.').replace('_', '.');
                    if (key.equals("nacos.data.group")) {
                        dataWrapper.setValue(v.toString());
                    } else if (key.startsWith("nacos.")) {
                        agentConf.put(key.substring("nacos.".length()), v);
                    }
                }
            });
            List<NacosInfo> infos = NacosInfo.parse(dataWrapper.getValue());
            if (infos.isEmpty()) {
                logger.log(Level.WARNING, "nacos.data.group is empty");
                return;
            }
            final AtomicInteger counter = new AtomicInteger();
            this.listenExecutor = Executors.newFixedThreadPool(infos.size(), r -> new Thread(r, "Nacos-Config-Listen-Thread-" + counter.incrementAndGet()));

            this.configService = NacosFactory.createConfigService(agentConf);
            for (NacosInfo info : infos) {
                final String content = configService.getConfigAndSignListener(info.dataId, info.group, 3_000, new Listener() {
                    @Override
                    public Executor getExecutor() {
                        return listenExecutor;
                    }

                    @Override
                    public void receiveConfigInfo(String configInfo) {
                        updateConent(application, info, configInfo);
                    }
                });
                updateConent(application, info, content);
            }
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
                logger.log(Level.WARNING, "shutdown nacos client error", e);
            }
        }
        if (this.listenExecutor != null) {
            this.listenExecutor.shutdownNow();
        }
    }

    private void updateConent(final Application application, NacosInfo info, String content) {
        Properties props = new Properties();
        try {
            info.content = content;
            info.contentMD5 = Utility.md5Hex(content);
            props.load(new StringReader(content));
        } catch (IOException e) {
            logger.log(Level.SEVERE, "load nacos content (dataId=" + info.dataId + ") error", e);
            return;
        }
        //更新全局配置项
        putResourceProperties(application, props);
        logger.log(Level.FINER, "nacos config(dataId=" + info.dataId + ") size: " + props.size());
    }

    private static class NacosInfo {

        public String dataId;

        public String group = "DEFAULT_GROUP";

        public String tenant = "";

        public String content = "";

        public String contentMD5 = "";

        //nacos.data.group值的数据格式为: dataId1:group1:tenant1,dataId2:group2:tenant2
        public static List<NacosInfo> parse(String dataGroupStr) {
            List<NacosInfo> list = new ArrayList<>();
            String tmpkey = new String(new char[]{2, 3, 4});
            dataGroupStr = dataGroupStr.replace("\\:", tmpkey);
            for (String str : dataGroupStr.split(",")) {
                String[] dataGroup = str.split(":");
                if (dataGroup[0].trim().isEmpty()) continue;
                String dataId = dataGroup[0].trim().replace(tmpkey, ":");
                String group = dataGroup.length > 1 ? dataGroup[1].trim().replace(tmpkey, ":") : "";
                String tenant = dataGroup.length > 2 ? dataGroup[2].trim().replace(tmpkey, ":") : "";
                NacosInfo info = new NacosInfo();
                info.dataId = dataId;
                if (!group.isEmpty()) info.group = group;
                info.tenant = tenant;
                list.add(info);
            }
            return list;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 89 * hash + Objects.hashCode(this.dataId);
            hash = 89 * hash + Objects.hashCode(this.tenant);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            final NacosInfo other = (NacosInfo) obj;
            if (!Objects.equals(this.dataId, other.dataId)) return false;
            return Objects.equals(this.tenant, other.tenant);
        }

        @Override
        public String toString() {
            return "{dataId:\"" + dataId + "\",group:\"" + group + "\",tenant:\"" + tenant + "\",contentMD5:\"" + contentMD5 + "\"}";
        }
    }
}
