package org.redkalex.properties.apollo;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import org.redkale.boot.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class ApolloPropertiesAgent extends PropertiesAgent {

    //apollo规定必须大于60秒
    protected static final Duration pullTimeoutMs = Duration.ofMillis(66_000);

    protected HttpClient httpClient; //JDK11里面的HttpClient

    protected ScheduledThreadPoolExecutor listenExecutor;

    protected String clientIp;

    protected String apiUrl; //不会以/结尾，http://localhost:8080

    protected String appid;

    protected String cluster;

    protected String label;

    protected String secret;

    @Override
    public void compile(final AnyValue propertiesConf) {
    }

    @Override
    public boolean acceptsConf(AnyValue config) {
        return (System.getProperty("apollo.meta") != null
            || config.getValue("apollo.meta") != null
            || config.getValue("apollo-meta") != null
            || config.getValue("apollo_meta") != null)
            && (System.getProperty("apollo.appid") != null
            || System.getProperty("app.id") != null
            || config.getValue("apollo.appid") != null
            || config.getValue("app.id") != null
            || config.getValue("apollo-appid") != null
            || config.getValue("apollo_appid") != null);
    }

    @Override
    public Map<String, Properties> init(final Application application, final AnyValue propertiesConf) {
        //可系统变量:  apollo.appid、apollo.meta、apollo.cluster、apollo.label、apollo.access-key.secret、apollo.namespace
        Properties agentConf = new Properties();
        propertiesConf.forEach((k, v) -> {
            String key = k.contains(".") && k.contains("-") ? k : k.replace('-', '.').replace('_', '.');
            if (!key.startsWith("apollo.")) return;
            if (key.equals("apollo.app.id")) {
                key = "apollo.appid";
            } else if (key.equals("apollo.access.key.secret")) {
                key = "apollo.access-key.secret";
            }
            agentConf.put(key, v);
        });
        System.getProperties().forEach((k, v) -> {
            //支持 app.id、apollo.appid、apollo.meta、apollo.cluster、apollo.label、apollo.access-key.secret、apollo.namespace
            if (k.toString().startsWith("apollo") || k.toString().equals("app.id")) {
                String key = k.toString().contains(".") && k.toString().contains("-")
                    ? k.toString() : k.toString().replace('-', '.').replace('_', '.');
                if (key.equals("apollo.app.id") || key.equals("app.id")) {
                    key = "apollo.appid";
                } else if (key.equals("apollo.access.key.secret")) {
                    key = "apollo.access-key.secret";
                }
                if (!key.startsWith("apollo.")) return;
                agentConf.put(key, v);
            }
        });
        this.httpClient = HttpClient.newBuilder().connectTimeout(pullTimeoutMs).build();
        this.apiUrl = agentConf.getProperty("apollo.meta").trim();
        if (this.apiUrl.endsWith("/")) {
            this.apiUrl = this.apiUrl.substring(0, this.apiUrl.length() - 1);
        }
        this.appid = agentConf.getProperty("apollo.appid");
        this.label = agentConf.getProperty("apollo.label");
        this.secret = agentConf.getProperty("apollo.access-key.secret");
        this.cluster = agentConf.getProperty("apollo.cluster", "default");
        this.clientIp = agentConf.getProperty("apollo.ip", Utility.localInetAddress().getHostAddress());
        String namespaces = agentConf.getProperty("apollo.namespace", "application");
        final List<ApolloInfo> infos = new ArrayList<>();
        final Map<String, ApolloInfo> infoMap = new HashMap<>();
        Map<String, Properties> result = new LinkedHashMap<>();
        for (String namespace : namespaces.split(";|,")) {
            if (namespace.trim().isEmpty()) continue;
            if (infoMap.containsKey(namespace)) continue;
            ApolloInfo info = new ApolloInfo();
            info.namespaceName = namespace;
            infos.add(info);
            infoMap.put(info.namespaceName, info);
            remoteConfigRequest(application, info, new Properties());
            result.put(info.namespaceName, info.properties);
        }

        this.listenExecutor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "Apollo-Config-Listen-Thread"));
        this.listenExecutor.scheduleWithFixedDelay(() -> {
            try {
                long s = System.currentTimeMillis();
                //{config_server_url}/notifications/v2?appId={appId}&cluster={clusterName}&notifications={notifications}
                String url = this.apiUrl + "/notifications/v2?appId=" + urlEncode(appid) + "&cluster=" + urlEncode(cluster)
                    + "&notifications=" + urlEncode(JsonConvert.root().convertTo(infos));
                HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url)).timeout(pullTimeoutMs);
                HttpResponse<String> resp = httpClient.send(authLogin(builder, url).GET().build(), HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 304) { //无配置变化
                    logger.log(Level.FINER, "Apollo pulling no change, cost " + (System.currentTimeMillis() - s) + " ms");
                    return;
                }
                String content = resp.body();
                if (resp.statusCode() != 200) {
                    logger.log(Level.WARNING, "Apollo pulling error, statusCode: " + resp.statusCode() + ", content: " + content + ", cost " + (System.currentTimeMillis() - s) + " ms");
                    Thread.sleep(5_000);
                    return;
                }
                logger.log(Level.FINER, "Apollo pulling content: " + (content == null ? "null" : content.trim()) + ", cost " + (System.currentTimeMillis() - s) + " ms");

                List<ApolloInfo> list = JsonConvert.root().convertFrom(ApolloInfo.LIST_TYPE, content);
                for (ApolloInfo item : list) {
                    ApolloInfo old = infoMap.get(item.namespaceName);
                    if (old.notificationId < 0) {
                        old.notificationId = item.notificationId;
                    } else {
                        old.notificationId = item.notificationId;
                        remoteConfigRequest(application, old, null);
                    }
                }
            } catch (Throwable t) {
                logger.log(Level.WARNING, "Apollo pulling config error", t);
            }
        }, 1, 1, TimeUnit.SECONDS);
        return result;
    }

    protected HttpRequest.Builder authLogin(HttpRequest.Builder builder, String url) throws IOException {
        if (secret != null && !secret.isEmpty()) {
            long timestamp = System.currentTimeMillis();
            String stringToSign = timestamp + "\n" + url2PathWithQuery(url);
            String signature = Utility.hmacSha1Base64(secret, stringToSign);
            builder.header("Authorization", String.format("Apollo %s:%s", appid, signature));
            builder.header("Timestamp", String.valueOf(timestamp));
        }
        return builder;
    }

    //https://www.apolloconfig.com/#/zh/usage/other-language-client-user-guide
    protected void remoteConfigRequest(final Application application, ApolloInfo info, Properties result) {
        String content = null;
        try {
            //{config_server_url}/configs/{appId}/{clusterName}/{namespaceName}?ip={clientIp}
            String url = this.apiUrl + "/configs/" + urlEncode(appid) + "/" + urlEncode(cluster) + "/" + urlEncode(info.namespaceName);
            String and = "?";
            if (clientIp != null && !clientIp.isEmpty()) {
                url += and + "ip=" + urlEncode(clientIp);
                and = "&";
            }
            if (label != null && !label.isEmpty()) {
                url += and + "label=" + urlEncode(label);
                and = "&";
            }
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url));
            HttpResponse<String> resp = httpClient.send(authLogin(builder, url).GET().build(), HttpResponse.BodyHandlers.ofString());
            content = resp.body();
            if (resp.statusCode() != 200) {
                logger.log(Level.SEVERE, "Load apollo content " + info + " error, statusCode: " + resp.statusCode() + ", content: " + content);
                return;
            }
            ApolloConfigResult rs = JsonConvert.root().convertFrom(ApolloConfigResult.class, content);
            if (rs.configurations == null) {
                logger.log(Level.WARNING, "Load apollo content " + info + " configurations is empty, content: " + content);
                return;
            }
            Properties props = new Properties();
            props.putAll(rs.configurations);

            //更新全局配置项
            if (result == null) { //配置项动态变更时需要一次性提交所有配置项
                updateEnvironmentProperties(application, info.namespaceName, ResourceEvent.create(info.properties, props));
                info.properties = props;
            } else {
                info.properties = props;
                result.putAll(props);
            }
            logger.log(Level.FINER, "Apollo config(namespace=" + info.namespaceName + ") size: " + props.size());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Load apollo content " + info + " error, content: " + content, e);
            if (result != null) throw (e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e));
        }
    }

    private static String url2PathWithQuery(String urlString) throws IOException {
        URL url = new URL(urlString);
        String path = url.getPath();
        String query = url.getQuery();

        String pathWithQuery = path;
        if (query != null && query.length() > 0) {
            pathWithQuery += "?" + query;
        }
        return pathWithQuery;
    }

    @Override
    public void destroy(AnyValue propertiesConf) {
        if (listenExecutor != null) {
            listenExecutor.shutdownNow();
        }
    }

    protected String urlEncode(String value) {
        return value == null ? null : URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    //必须public， 会被JsonConvert.convertTo()使用
    public static class ApolloInfo {

        public static final Type LIST_TYPE = new TypeToken<List<ApolloInfo>>() {
        }.getType();

        public String namespaceName;

        public int notificationId = -1;

        Properties properties = new Properties();

        @Override
        public String toString() {
            return "{namespaceName:\"" + namespaceName + "\", notificationId:" + notificationId + "}";
        }
    }

    //必须public， 会被JsonConvert.convertFrom()使用
    //{"appId":"SampleApp","cluster":"default","namespaceName":"application","configurations":{"timeout":"100","test.id":"1234567","test.value":"my name is ok too"},"releaseKey":"20221125202649-1dc5e11cddd4dba8"}
    public static class ApolloConfigResult {

        public Map<String, String> configurations;
    }
}
