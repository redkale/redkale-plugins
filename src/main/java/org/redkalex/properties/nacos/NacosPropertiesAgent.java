/*
 */
package org.redkalex.properties.nacos;

import java.io.*;
import java.net.*;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import org.redkale.boot.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class NacosPropertiesAgent extends PropertiesAgent {

    protected static final Map<String, String> httpHeaders = Utility.ofMap("Content-Type", "application/json", "Accept", "application/json");

    protected static final int pullTimeoutMs = 30_000;

    //https://nacos.io/zh-cn/docs/open-api.html
    protected static final Map<String, String> pullHeaders = Utility.ofMap("Long-Pulling-Timeout", String.valueOf(pullTimeoutMs));

    protected HttpClient httpClient; //JDK11里面的HttpClient

    protected String apiUrl; //不会以/结尾，且不以/nacos结尾

    protected ScheduledThreadPoolExecutor listenExecutor;

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        //支持 nacos.serverAddr、nacos-serverAddr、nacos_serverAddr
        //nacos.data.group值的数据格式为: dataId1:group1:tenant1,dataId2:group2:tenant2
        //多组数据用,分隔
        return (config.getValue("nacos.serverAddr") != null
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
        this.apiUrl = "http://" + agentConf.getProperty("serverAddr") + "/nacos/v1";
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(pullTimeoutMs)).build();

        List<NacosInfo> infos = NacosInfo.parse(dataWrapper.getValue());
        if (infos.isEmpty()) {
            logger.log(Level.WARNING, "nacos.data.group is empty");
            return;
        }
        final Map<String, NacosInfo> infoMap = new HashMap<>(); //key: dataId-tenant
        for (NacosInfo info : infos) {
            remoteConfigRequest(application, info, false);
            infoMap.put(info.dataId + "-" + info.tenant, info);
        }

        this.listenExecutor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Nacos-Config-Listen-Thread"));
        this.listenExecutor.scheduleAtFixedRate(() -> {
            try {
                long s = System.currentTimeMillis();
                String url = this.apiUrl + "/cs/configs/listener?Listening-Configs=" + urlEncode(NacosInfo.paramBody(infos));
                //Listening-Configs=dataId%02group%02contentMD5%02tenant%01
                String content = Utility.remoteHttpContent(httpClient, "POST", url, pullTimeoutMs, StandardCharsets.UTF_8, pullHeaders);
                logger.log(Level.INFO, "nacos pulling content: " + (content == null ? "null" : content.trim()) + ", cost " + (System.currentTimeMillis() - s) + " ms");
                if (content == null || content.trim().isEmpty()) return;
                if (!content.contains("%02")) {
                    Thread.sleep(5_000);
                    return;
                }
                String split1 = Character.toString((char) 1);
                String split2 = Character.toString((char) 2);
                content = URLDecoder.decode(content.trim(), StandardCharsets.UTF_8);
                for (String str : content.split(split1)) {
                    if (str.isEmpty()) continue;
                    String[] items = str.split(split2); //dataId%02group%02tenant%01
                    NacosInfo info = infoMap.get(items[0] + "-" + (items.length > 2 ? items[2] : ""));
                    if (info != null) {
                        remoteConfigRequest(application, info, true);
                    }
                }
            } catch (Throwable t) {
                logger.log(Level.WARNING, "nacos pulling config error", t);
            }
        }, 1, 2, TimeUnit.SECONDS);
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
        if (listenExecutor != null) {
            listenExecutor.shutdownNow();
        }
    }

    protected void remoteConfigRequest(final Application application, NacosInfo info, boolean ignoreErr) {
        try {
            String url = this.apiUrl + "/cs/configs?dataId=" + urlEncode(info.dataId) + "&group=" + urlEncode(info.group);
            if (!info.tenant.isEmpty()) url += "&tenant=" + urlEncode(info.tenant);
            String content = Utility.remoteHttpContent(httpClient, "GET", url, StandardCharsets.UTF_8, httpHeaders);
            updateConent(application, info, content);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "load nacos content " + info + " error", e);
            if (!ignoreErr) throw (e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
        }
    }

    protected void updateConent(final Application application, NacosInfo info, String content) {
        Properties props = new Properties();
        String oldmd5 = info.contentMD5;
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
        logger.log(Level.FINER, "nacos config(dataId=" + info.dataId + ") size: " + props.size() + ", " + info + (oldmd5.isEmpty() ? "" : (" old-contentMD5: " + oldmd5)));
    }

    protected String urlEncode(String value) {
        return value == null ? null : URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    protected static class NacosInfo {

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

        public static String paramBody(List<NacosInfo> infos) {
            String split1 = Character.toString((char) 1);
            String split2 = Character.toString((char) 2);
            StringBuilder sb = new StringBuilder();
            for (NacosInfo info : infos) {
                sb.append(info.dataId).append(split2).append(info.group);
                sb.append(split2).append(info.contentMD5);
                if (!info.tenant.isEmpty()) {
                    sb.append(split2).append(info.tenant);
                }
                sb.append(split1);
            }
            return sb.toString();
        }

        @Override
        public String toString() {
            return "{dataId:\"" + dataId + "\",group:\"" + group + "\",tenant:\"" + tenant + "\",contentMD5:\"" + contentMD5 + "\"}";
        }
    }

}
