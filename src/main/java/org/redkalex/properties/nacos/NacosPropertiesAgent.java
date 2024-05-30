/*
 */
package org.redkalex.properties.nacos;

import java.io.StringReader;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import org.redkale.boot.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceEvent;
import org.redkale.props.spi.PropertiesAgent;
import org.redkale.util.*;

/** @author zhangjx */
public class NacosPropertiesAgent extends PropertiesAgent {

    protected static final Duration pullTimeoutMs = Duration.ofMillis(30_000);

    protected HttpClient httpClient; // JDK11里面的HttpClient

    protected String apiUrl; // 不会以/结尾，且不以/nacos结尾

    protected ScheduledThreadPoolExecutor listenExecutor;

    protected String username = "";

    protected String password = "";

    protected String accessToken; // 定时更新

    protected long accessExpireTime; // 过期时间点

    @Override
    public void compile(final AnyValue propertiesConf) {
        // do nothing
    }

    public static boolean acceptsConf0(AnyValue config) {
        // 支持 nacos.serverAddr、nacos-serverAddr
        // nacos.data.group值的数据格式为: dataId1:group1:tenant1,dataId2:group2:tenant2
        // 多组数据用,分隔
        return (config.getValue("nacos.serverAddr") != null
                        || config.getValue("nacos-serverAddr") != null
                        || System.getProperty("nacos.serverAddr") != null
                        || System.getProperty("nacos-serverAddr") != null)
                && (config.getValue("nacos.data.group") != null
                        || config.getValue("nacos-data-group") != null
                        || System.getProperty("nacos.data.group") != null
                        || System.getProperty("nacos-data-group") != null);
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        return acceptsConf0(config);
    }

    @Override
    public Map<String, Properties> init(final Application application, final AnyValue propertiesConf) {
        Properties agentConf = new Properties();
        ObjectRef<String> dataRef = new ObjectRef<>();
        propertiesConf.forEach((k, v) -> {
            String key = k.replace('-', '.');
            if (key.equals("nacos.data.group")) {
                dataRef.set(v);
            } else if (key.startsWith("nacos.")) {
                agentConf.put(key.substring("nacos.".length()), v);
            }
        });
        System.getProperties().forEach((k, v) -> {
            // 支持 nacos.serverAddr、nacos-serverAddr
            if (k.toString().startsWith("nacos")) {
                String key = k.toString().replace('-', '.');
                if (key.equals("nacos.data.group")) {
                    dataRef.set(v.toString());
                } else if (key.startsWith("nacos.")) {
                    agentConf.put(key.substring("nacos.".length()), v);
                }
            }
        });
        this.apiUrl = "http://" + agentConf.getProperty("serverAddr") + "/nacos/v1";
        this.username = agentConf.getProperty("username");
        this.password = agentConf.getProperty("password");
        this.httpClient = HttpClient.newBuilder().connectTimeout(pullTimeoutMs).build();

        List<NacosInfo> infos = NacosInfo.parse(dataRef.get());
        if (infos.isEmpty()) {
            logger.log(Level.WARNING, "nacos.data.group is empty");
            return null;
        }
        final Map<String, NacosInfo> infoMap = new HashMap<>(); // key: dataId-tenant
        Map<String, Properties> result = new LinkedHashMap<>();
        for (NacosInfo info : infos) {
            remoteConfigRequest(application, info, new Properties());
            infoMap.put(info.dataId + "-" + info.tenant, info);
            result.put(info.dataId, info.properties);
        }

        this.listenExecutor =
                new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Redkalex-Properties-Nacos-Listen-Thread"));
        this.listenExecutor.scheduleWithFixedDelay(
                () -> {
                    try {
                        if (!remoteLogin()) {
                            return;
                        }
                        long s = System.currentTimeMillis();
                        String url = this.apiUrl + "/cs/configs/listener?Listening-Configs="
                                + urlEncode(NacosInfo.paramBody(infos));
                        if (accessToken != null) {
                            url += "&accessToken=" + urlEncode(accessToken);
                        }
                        // Listening-Configs=dataId%02group%02contentMD5%02tenant%01
                        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                                .timeout(pullTimeoutMs)
                                .header("Long-Pulling-Timeout", String.valueOf(pullTimeoutMs.toMillis()))
                                .POST(HttpRequest.BodyPublishers.noBody())
                                .build();
                        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
                        String content = resp.body();
                        if (resp.statusCode() != 200) {
                            logger.log(
                                    Level.WARNING,
                                    "nacos pulling error, statusCode: " + resp.statusCode() + ", content: " + content
                                            + ", cost " + (System.currentTimeMillis() - s) + " ms");
                            Thread.sleep(5_000);
                            return;
                        }
                        if (content == null || content.trim().isEmpty()) {
                            return;
                        }
                        logger.log(
                                Level.FINER,
                                "nacos pulling content: " + content.trim() + ", cost "
                                        + (System.currentTimeMillis() - s) + " ms");
                        String split1 = Character.toString((char) 1);
                        String split2 = Character.toString((char) 2);
                        content = URLDecoder.decode(content.trim(), StandardCharsets.UTF_8);
                        for (String str : content.split(split1)) {
                            if (str.isEmpty()) {
                                continue;
                            }
                            String[] items = str.split(split2); // dataId%02group%02tenant%01
                            NacosInfo info = infoMap.get(items[0] + "-" + (items.length > 2 ? items[2] : ""));
                            if (info != null) {
                                remoteConfigRequest(application, info, null);
                            }
                        }
                    } catch (Throwable t) {
                        logger.log(Level.WARNING, "nacos pulling config error", t);
                    }
                },
                1,
                1,
                TimeUnit.SECONDS);
        return result;
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
        if (listenExecutor != null) {
            listenExecutor.shutdownNow();
        }
    }

    // https://nacos.io/zh-cn/docs/auth.html
    protected boolean remoteLogin() {
        if (username == null || username.isEmpty()) {
            return true;
        }
        if (accessExpireTime > 0 && accessExpireTime > System.currentTimeMillis()) {
            return true;
        }
        long s = System.currentTimeMillis();
        String content = null;
        try {
            String url =
                    this.apiUrl + "/auth/login?username=" + urlEncode(username) + "&password=" + urlEncode(password);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(pullTimeoutMs)
                    .headers("Content-Type", "application/json", "Accept", "application/json")
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            content = resp.body(); // {"accessToken":"xxxx","tokenTtl":18000,"globalAdmin":true}
            if (resp.statusCode() != 200) {
                this.accessExpireTime = 0;
                logger.log(
                        Level.WARNING,
                        "Nacos login error, statusCode: " + resp.statusCode() + ", content: " + content + ", cost "
                                + (System.currentTimeMillis() - s) + " ms");
                return false;
            }
            Map<String, String> map = JsonConvert.root().convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, content);
            this.accessToken = map.get("accessToken");
            this.accessExpireTime = s + Long.parseLong(map.get("tokenTtl")) * 1000 - 1000; // 少一秒
            return true;
        } catch (Exception e) {
            logger.log(
                    Level.WARNING,
                    "Nacos login error, content: " + content + ", cost " + (System.currentTimeMillis() - s) + " ms",
                    e);
            return false;
        }
    }

    // https://nacos.io/zh-cn/docs/open-api.html
    protected void remoteConfigRequest(final Application application, NacosInfo info, Properties result) {
        if (!remoteLogin()) {
            return;
        }
        String content = null;
        try {
            String url =
                    this.apiUrl + "/cs/configs?dataId=" + urlEncode(info.dataId) + "&group=" + urlEncode(info.group);
            if (accessToken != null) {
                url += "&accessToken=" + urlEncode(accessToken);
            }
            if (!info.tenant.isEmpty()) {
                url += "&tenant=" + urlEncode(info.tenant);
            }
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(pullTimeoutMs)
                    .headers("Content-Type", "application/json", "Accept", "application/json")
                    .GET()
                    .build();
            HttpResponse<String> resp =
                    httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            content = resp.body();
            if (resp.statusCode() != 200) {
                logger.log(
                        Level.SEVERE,
                        "Load nacos content " + info + " error, statusCode: " + resp.statusCode() + ", content: "
                                + content);
                return;
            }
            Properties props = new Properties();
            String oldmd5 = info.contentMD5;
            info.content = content;
            String md5Header = resp.headers()
                    .firstValue("content-md5")
                    .orElse("")
                    .replace("[", "")
                    .replace("]", "");
            if (md5Header.isEmpty()) {
                info.contentMD5 = Utility.md5Hex(content);
            } else {
                info.contentMD5 = md5Header;
            }
            props.load(new StringReader(content));

            if (result == null) { // 配置项动态变更时需要一次性提交所有配置项
                onEnvironmentUpdated(application, info.dataId, ResourceEvent.create(info.properties, props));
                info.properties = props;
            } else {
                info.properties = props;
                result.putAll(props);
            }
            logger.log(
                    Level.FINER,
                    "Nacos config(dataId=" + info.dataId + ") size: " + props.size() + ", " + info
                            + (oldmd5.isEmpty() ? "" : (" old-contentMD5: " + oldmd5)));
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Load nacos content " + info + " error, content: " + content, e);
            if (result != null) {
                throw (e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
            }
        }
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

        public Properties properties = new Properties();

        // nacos.data.group值的数据格式为: dataId1:group1:tenant1,dataId2:group2:tenant2
        public static List<NacosInfo> parse(String dataGroupStr) {
            List<NacosInfo> list = new ArrayList<>();
            String tmpkey = new String(new char[] {2, 3, 4});
            dataGroupStr = dataGroupStr.replace("\\:", tmpkey);
            for (String str : dataGroupStr.split(",")) {
                String[] dataGroup = str.split(":");
                if (dataGroup[0].trim().isEmpty()) {
                    continue;
                }
                String dataId = dataGroup[0].trim().replace(tmpkey, ":");
                String group = dataGroup.length > 1 ? dataGroup[1].trim().replace(tmpkey, ":") : "";
                String tenant = dataGroup.length > 2 ? dataGroup[2].trim().replace(tmpkey, ":") : "";
                NacosInfo info = new NacosInfo();
                info.dataId = dataId;
                if (!group.isEmpty()) {
                    info.group = group;
                }
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
            return "{dataId:\"" + dataId + "\",group:\"" + group + "\",tenant:\"" + tenant + "\",contentMD5:\""
                    + contentMD5 + "\"}";
        }
    }
}
