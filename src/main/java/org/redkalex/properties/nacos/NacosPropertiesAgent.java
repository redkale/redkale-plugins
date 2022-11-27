/*
 */
package org.redkalex.properties.nacos;

import java.net.http.HttpClient;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.redkale.boot.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class NacosPropertiesAgent extends PropertiesAgent {

    protected static final Map<String, String> httpHeaders = Utility.ofMap("Content-Type", "application/json", "Accept", "application/json");

    protected HttpClient httpClient; //JDK11里面的HttpClient

    protected String apiurl; //不会以/结尾，且不以/nacos结尾

    protected ScheduledThreadPoolExecutor scheduler;

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
        Properties agentConf = new Properties();
        StringWrapper dataGroups = new StringWrapper();
        propertiesConf.forEach((k, v) -> {
            String key = k.replace('-', '.').replace('_', '.');
            if (key.equals("nacos.data.group")) {
                dataGroups.setValue(v);
            } else if (key.startsWith("nacos.")) {
                agentConf.put(key.substring("nacos.".length()), v);
            }
        });
        System.getProperties().forEach((k, v) -> {
            //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
            if (k.toString().startsWith("nacos")) {
                String key = k.toString().replace('-', '.').replace('_', '.');
                if (key.equals("nacos.data.group")) {
                    dataGroups.setValue(v.toString());
                } else if (key.startsWith("nacos.")) {
                    agentConf.put(key.substring("nacos.".length()), v);
                }
            }
        });
        this.apiurl = "http://" + agentConf.getProperty("serverAddr") + "/nacos/v1";
        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
        if (scheduler != null) scheduler.shutdownNow();
    }
}
