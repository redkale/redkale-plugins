/*
 */
package org.redkalex.cluster.nacos;

import java.net.*;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import org.redkale.boot.*;
import org.redkale.cluster.ClusterAgent;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.Service;
import org.redkale.util.*;

/**
 * <blockquote><pre>
 *  &lt;cluster value="org.redkalex.cluster.nacos.NacosClusterAgent"&gt;
 *      &lt;property name="apiurl" value="http://localhost:8500/nacos/v1"/&gt;  &lt;-- 必须要有nacos字样 --&gt;
 *      &lt;property name="ttls" value="5"/&gt;
 *      &lt;property name="namespaceid" value="dev"/&gt;
 *  &lt;/cluster&gt;
 * </pre></blockquote>
 *
 * @author zhangjx
 */
public class NacosClusterAgent extends ClusterAgent {

    protected static final Map<String, String> httpHeaders = Utility.ofMap("Content-Type", "application/json", "Accept", "application/json");

    protected final boolean fine = logger.isLoggable(Level.FINE);

    protected final boolean finer = logger.isLoggable(Level.FINER);

    protected final boolean finest = logger.isLoggable(Level.FINEST);

    protected HttpClient httpClient; //JDK11里面的HttpClient

    protected String apiurl; //不会以/结尾，且不以/nacos结尾

    protected int ttls = 5; //定时检查的秒数

    protected String namespaceid; //命名空间

    protected ScheduledThreadPoolExecutor scheduler;

    //可能被HttpMessageClient用到的服务 key: serviceName
    protected final ConcurrentHashMap<String, Collection<InetSocketAddress>> httpAddressMap = new ConcurrentHashMap<>();

    //可能被mqtp用到的服务 key: serviceName
    protected final ConcurrentHashMap<String, Collection<InetSocketAddress>> mqtpAddressMap = new ConcurrentHashMap<>();

    @Override
    public void init(ResourceFactory factory, AnyValue config) {
        super.init(factory, config);

        this.apiurl = config.getValue("apiurl");
        if (this.apiurl.endsWith("/")) {
            this.apiurl = this.apiurl.substring(0, this.apiurl.length() - 1);
        }
        this.ttls = config.getIntValue("ttls", 5);
        if (this.ttls < 3) this.ttls = 5;

        this.namespaceid = config.getValue("namespaceid");

        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    public void destroy(AnyValue config) {
        if (scheduler != null) scheduler.shutdownNow();
    }

    @Override  //ServiceLoader时判断配置是否符合当前实现类
    public boolean acceptsConf(AnyValue config) {
        if (config == null) return false;
        String url = config.getValue("apiurl");
        return url != null && url.toLowerCase().contains("/nacos");
    }

    @Override
    public void start() {
        if (this.scheduler == null) {
            this.scheduler = new ScheduledThreadPoolExecutor(4, (Runnable r) -> {
                final Thread t = new Thread(r, NacosClusterAgent.class.getSimpleName() + "-Task-Thread");
                t.setDaemon(true);
                return t;
            });

            //delay为了错开请求
            this.scheduler.scheduleAtFixedRate(() -> {
                beatApplicationHealth();
                localEntrys.values().stream().filter(e -> !e.canceled).forEach(entry -> {
                    beatLocalHealth(entry);
                });
            }, 18, Math.max(2000, ttls * 1000 - 168), TimeUnit.MILLISECONDS);

            this.scheduler.scheduleAtFixedRate(() -> {
                reloadMqtpAddressHealth();
            }, 88 * 2, Math.max(2000, ttls * 1000 - 168), TimeUnit.MILLISECONDS);

            this.scheduler.scheduleAtFixedRate(() -> {
                reloadHttpAddressHealth();
            }, 128 * 3, Math.max(2000, ttls * 1000 - 168), TimeUnit.MILLISECONDS);

            this.scheduler.scheduleAtFixedRate(() -> {
                remoteEntrys.values().stream().filter(entry -> "SNCP".equalsIgnoreCase(entry.protocol)).forEach(entry -> {
                    updateSncpTransport(entry);
                });
            }, 188 * 4, Math.max(2000, ttls * 1000 - 168), TimeUnit.MILLISECONDS);
        }
    }

    protected void reloadMqtpAddressHealth() {
        try {
            String content = Utility.remoteHttpContent(httpClient, "GET", this.apiurl + "/ns/service/list?pageNo=1&pageSize=99999&namespaceId=" + urlEncode(namespaceid), StandardCharsets.UTF_8, httpHeaders);
            final ServiceList list = JsonConvert.root().convertFrom(ServiceList.class, content);
            Set<String> mqtpkeys = new HashSet<>();
            if (list != null && list.doms != null) {
                for (String key : list.doms) {
                    if (key.startsWith("mqtp:")) mqtpkeys.add(key);
                }
            }
            mqtpkeys.forEach(serviceName -> {
                try {
                    this.mqtpAddressMap.put(serviceName, queryAddress(serviceName).get(Math.max(2, ttls / 2), TimeUnit.SECONDS));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "loadMqtpAddressHealth check " + serviceName + " error", e);
                }
            });
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "loadMqtpAddressHealth check error", ex);
        }
    }

    protected void reloadHttpAddressHealth() {
        try {
            this.httpAddressMap.keySet().stream().forEach(serviceName -> {
                try {
                    this.httpAddressMap.put(serviceName, queryAddress(serviceName).get(Math.max(2, ttls / 2), TimeUnit.SECONDS));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "checkHttpAddressHealth check " + serviceName + " error", e);
                }
            });
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "checkHttpAddressHealth check error", ex);
        }
    }

    @Override //获取MQTP的HTTP远程服务的可用ip列表, key = serviceName的后半段
    public CompletableFuture<Map<String, Collection<InetSocketAddress>>> queryMqtpAddress(String protocol, String module, String resname) {
        final Map<String, Collection<InetSocketAddress>> rsmap = new ConcurrentHashMap<>();
        final String serviceNamePrefix = generateHttpServiceName(protocol, module, null) + ":";
        mqtpAddressMap.keySet().stream().filter(k -> k.startsWith(serviceNamePrefix))
            .forEach(sn -> rsmap.put(sn.substring(serviceNamePrefix.length()), mqtpAddressMap.get(sn)));
        return CompletableFuture.completedFuture(rsmap);
    }

    @Override //获取HTTP远程服务的可用ip列表
    public CompletableFuture<Collection<InetSocketAddress>> queryHttpAddress(String protocol, String module, String resname) {
        final String serviceName = generateHttpServiceName(protocol, module, resname);
        Collection<InetSocketAddress> rs = httpAddressMap.get(serviceName);
        if (rs != null) return CompletableFuture.completedFuture(rs);
        return queryAddress(serviceName).thenApply(t -> {
            httpAddressMap.put(serviceName, t);
            return t;
        });
    }

    @Override
    protected CompletableFuture<Collection<InetSocketAddress>> queryAddress(final ClusterEntry entry) {
        return queryAddress(entry.serviceName);
    }

    private CompletableFuture<Collection<InetSocketAddress>> queryAddress(final String serviceName) {
        //return (httpClient != null) ? queryAddress11(serviceName) : queryAddress8(serviceName);
        return queryAddress11(serviceName);
    }

    //JDK11+版本以上的纯异步方法
    private CompletableFuture<Collection<InetSocketAddress>> queryAddress11(final String serviceName) {
        final HttpClient client = httpClient;
        String url = this.apiurl + "/ns/instance/list?serviceName=" + urlEncode(serviceName) + "&namespaceId=" + urlEncode(namespaceid);
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url)).expectContinue(true).timeout(Duration.ofMillis(6000));
        httpHeaders.forEach((n, v) -> builder.header(n, v));
        return client.sendAsync(builder.GET().build(), BodyHandlers.ofString(StandardCharsets.UTF_8)).thenApply(resp -> {
            final ServiceEntry entry = JsonConvert.root().convertFrom(ServiceEntry.class, resp.body());
            final Set<InetSocketAddress> set = new HashSet<>();
            if (entry != null && entry.hosts != null) {
                for (ServiceInstance instance : entry.hosts) {
                    set.add(instance.createSocketAddress());
                }
            }
            return set;
        });
    }

    protected boolean isApplicationHealth() {
        String serviceName = generateApplicationServiceName();
        String serviceType = generateApplicationServiceType();
        String host = generateApplicationHost();
        int port = generateApplicationPort();
        String querys = "ip=" + host + "&port=" + port + "&serviceName=" + urlEncode(serviceName) + "&groupName=" + serviceType + "&namespaceId=" + urlEncode(namespaceid) + "&healthyOnly=true";
        try {
            String rs = Utility.remoteHttpContent(httpClient, "GET", this.apiurl + "/ns/instance?" + querys, StandardCharsets.UTF_8, httpHeaders);
            if (finest) logger.log(Level.FINEST, "isApplicationHealth: " + querys + " --> " + rs);
            //caused: no service DEFAULT_GROUP@@application.platf.node.20100 found!;
            if (!rs.startsWith("{")) return false;
            InstanceDetail detail = JsonConvert.root().convertFrom(InstanceDetail.class, rs);
            return detail != null && detail.healthy;
        } catch (RetcodeException e) {
            if (e.getRetcode() != 404) logger.log(Level.SEVERE, serviceName + " check error", e);
            return false;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " check error", ex);
            return false;
        }
    }

    protected void beatHealth(String serviceName, String serviceType, String host, int port) {
        String beat = "{\"ip\":\"" + host + "\",\"metadata\":{},\"port\":" + port + ",\"scheduled\":true,\"groupName\":\"" + serviceType + "\",\"namespaceId\":\"" + namespaceid + "\",\"serviceName\":\"" + serviceName + "\"}";
        try {
            String rs = Utility.remoteHttpContent(httpClient, "PUT", this.apiurl + "/ns/instance/beat?serviceName=" + urlEncode(serviceName) + "&groupName=" + serviceType + "&namespaceId=" + urlEncode(namespaceid)
                + "&beat=" + urlEncode(beat), StandardCharsets.UTF_8, httpHeaders);
            //if (finest) logger.log(Level.FINEST, "checkLocalHealth: " + beat + " --> " + rs);
            if (!rs.startsWith("{")) logger.log(Level.SEVERE, serviceName + " check error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " check error", ex);
        }
    }

    protected void register(String serviceName, String serviceType, String host, int port) {
        //https://nacos.io/zh-cn/docs/open-api.html#2.1
        String querys = "ip=" + host + "&port=" + port + "&serviceName=" + urlEncode(serviceName) + "&groupName=" + serviceType + "&namespaceId=" + urlEncode(namespaceid) + "&healthy=true&enabled=true&ephemeral=false";
        try {
            String rs = Utility.remoteHttpContent(httpClient, "POST", this.apiurl + "/ns/instance?" + querys, StandardCharsets.UTF_8, httpHeaders);
            if (finest) logger.log(Level.FINEST, "register: " + querys + " --> " + rs);
            if (!"ok".equalsIgnoreCase(rs)) logger.log(Level.SEVERE, serviceName + " register error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " register error", ex);
        }
    }

    protected void deregister(String serviceName, String serviceType, String host, int port, ClusterEntry currEntry, boolean realCanceled) {
        //https://nacos.io/zh-cn/docs/open-api.html#2.2
        String querys = "ip=" + host + "&port=" + port + "&serviceName=" + urlEncode(serviceName) + "&groupName=" + serviceType + "&namespaceId=" + urlEncode(namespaceid) + "&ephemeral=false";
        try {
            String rs = Utility.remoteHttpContent(httpClient, "DELETE", this.apiurl + "/ns/instance?" + querys, StandardCharsets.UTF_8, httpHeaders);
            if (realCanceled && currEntry != null) currEntry.canceled = true;
            if (finest) logger.log(Level.FINEST, "deregister: " + querys + " --> " + rs);
            if (!"ok".equalsIgnoreCase(rs)) logger.log(Level.SEVERE, serviceName + " deregister error: " + rs);
        } catch (RetcodeException e) {
            if (e.getRetcode() != 404) logger.log(Level.SEVERE, serviceName + " deregister error", e);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " deregister error", ex);
        }
    }

    protected void beatApplicationHealth() {
        beatHealth(generateApplicationServiceName(), generateApplicationServiceType(), generateApplicationHost(), generateApplicationPort());
    }

    protected void beatLocalHealth(final ClusterEntry cluster) {
        beatHealth(cluster.serviceName, cluster.serviceType, cluster.address.getHostString(), cluster.address.getPort());
    }

    @Override
    public void register(Application application) {
        if (isApplicationHealth()) throw new RuntimeException("application.nodeid=" + nodeid + " exists in cluster");
        deregister(application);
        register(generateApplicationServiceName(), generateApplicationServiceType(), generateApplicationHost(), generateApplicationPort());
    }

    @Override
    public void deregister(Application application) {
        deregister(generateApplicationServiceName(), generateApplicationServiceType(), generateApplicationHost(), generateApplicationPort(), null, false);
    }

    @Override
    protected ClusterEntry register(NodeServer ns, String protocol, Service service) {
        deregister(ns, protocol, service, false);
        ClusterEntry cluster = new ClusterEntry(ns, protocol, service);
        register(cluster.serviceName, cluster.serviceType, cluster.address.getHostString(), cluster.address.getPort());
        return cluster;
    }

    @Override
    protected void deregister(NodeServer ns, String protocol, Service service) {
        deregister(ns, protocol, service, true);
    }

    protected void deregister(NodeServer ns, String protocol, Service service, boolean realCanceled) {
        String serviceid = generateServiceId(ns, protocol, service);
        ClusterEntry currEntry = localEntrys.values().stream().filter(x -> serviceid.equals(x.serviceid)).findAny().orElse(null);
        if (currEntry == null) currEntry = remoteEntrys.values().stream().filter(x -> serviceid.equals(x.serviceid)).findAny().orElse(null);
        ClusterEntry cluster = currEntry == null ? new ClusterEntry(ns, protocol, service) : currEntry;
        deregister(cluster.serviceName, cluster.serviceType, cluster.address.getHostString(), cluster.address.getPort(), currEntry, realCanceled);
    }

    public static final class ServiceList {

        public int count;

        public List<String> doms;
    }

    public static final class ServiceEntry {

        public List<ServiceInstance> hosts;
    }

    public static final class ServiceInstance {

        public String ip;

        public int port;

        public InetSocketAddress createSocketAddress() {
            return new InetSocketAddress(ip, port);
        }
    }

    public static final class InstanceDetail {

        public boolean healthy;
    }

    public static final class NameSpaceList {

        public List<NameSpaceDetail> data;
    }

    public static final class NameSpaceDetail {

        public String namespace;

        public String namespaceShowName;
    }
}
