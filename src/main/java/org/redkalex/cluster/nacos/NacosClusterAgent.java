/*
 */
package org.redkalex.cluster.nacos;

import java.io.Serializable;
import java.net.*;
import java.net.http.*;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.redkale.annotation.ResourceChanged;
import org.redkale.boot.*;
import org.redkale.cluster.spi.ClusterAgent;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceEvent;
import org.redkale.service.Service;
import org.redkale.util.*;

/**
 *
 *
 * <blockquote>
 *
 * <pre>
 *  &lt;cluster type="nacos" apiurl="http://localhost:8500/nacos/v1" ttls="5" namespaceid="dev"&gt;
 *  &lt;/cluster&gt;
 * </pre>
 *
 * </blockquote>
 *
 * @author zhangjx
 */
public class NacosClusterAgent extends ClusterAgent {

    protected static final Map<String, Serializable> httpHeaders =
            (Map) Utility.ofMap("Content-Type", "application/json", "Accept", "application/json");

    protected HttpClient httpClient; // JDK11里面的HttpClient

    protected String apiUrl; // 不会以/结尾，且不以/nacos结尾，目前以/nacos/v1结尾

    protected int ttls = 5; // 定时检查的秒数

    protected String namespaceid; // 命名空间

    protected ScheduledThreadPoolExecutor scheduler;

    protected ScheduledFuture taskFuture1;

    protected ScheduledFuture taskFuture2;

    protected ScheduledFuture taskFuture3;

    protected ScheduledFuture taskFuture4;

    // 可能被HttpMessageClient用到的服务 key: serviceName
    protected final ConcurrentHashMap<String, Set<InetSocketAddress>> httpAddressMap = new ConcurrentHashMap<>();

    // 可能被sncp用到的服务 key: serviceName
    protected final ConcurrentHashMap<String, Set<InetSocketAddress>> sncpAddressMap = new ConcurrentHashMap<>();

    @Override
    public void init(AnyValue config) {
        super.init(config);

        this.apiUrl = config.getValue("apiurl");
        if (this.apiUrl.endsWith("/")) {
            this.apiUrl = this.apiUrl.substring(0, this.apiUrl.length() - 1);
        }
        this.ttls = config.getIntValue("ttls", 5);
        if (this.ttls < 3) {
            this.ttls = 5;
        }

        this.namespaceid = config.getValue("namespaceid");

        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    @ResourceChanged
    public void onResourceChange(ResourceEvent[] events) {
        StringBuilder sb = new StringBuilder();
        int newTtls = this.ttls;
        for (ResourceEvent event : events) {
            if ("ttls".equals(event.name())) {
                newTtls = Integer.parseInt(event.newValue().toString());
                if (newTtls < 5) {
                    sb.append(NacosClusterAgent.class.getSimpleName())
                            .append(" cannot change '")
                            .append(event.name())
                            .append("' to '")
                            .append(event.coverNewValue())
                            .append("'\r\n");
                } else {
                    sb.append(NacosClusterAgent.class.getSimpleName())
                            .append(" change '")
                            .append(event.name())
                            .append("' to '")
                            .append(event.coverNewValue())
                            .append("'\r\n");
                }
            } else {
                sb.append(NacosClusterAgent.class.getSimpleName())
                        .append(" skip change '")
                        .append(event.name())
                        .append("' to '")
                        .append(event.coverNewValue())
                        .append("'\r\n");
            }
        }
        if (newTtls != this.ttls) {
            this.ttls = newTtls;
            start();
        }
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
    }

    @Override
    public void destroy(AnyValue config) {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Override // ServiceLoader时判断配置是否符合当前实现类
    public boolean acceptsConf(AnyValue config) {
        if (config == null) {
            return false;
        }
        if (!"nacos".equalsIgnoreCase(config.getValue("type"))) {
            return false;
        }
        String url = config.getValue("apiurl");
        return url != null && url.toLowerCase().contains("/nacos");
    }

    @Override
    public void start() {
        if (this.scheduler == null) {
            AtomicInteger counter = new AtomicInteger();
            this.scheduler = new ScheduledThreadPoolExecutor(4, (Runnable r) -> {
                final Thread t = new Thread(
                        r,
                        "Redkalex-" + NacosClusterAgent.class.getSimpleName() + "-Task-Thread-"
                                + counter.incrementAndGet());
                t.setDaemon(true);
                return t;
            });
        }
        // delay为了错开请求
        if (this.taskFuture1 != null) {
            this.taskFuture1.cancel(true);
        }
        this.taskFuture1 = this.scheduler.scheduleAtFixedRate(
                () -> {
                    beatApplicationHealth();
                    localEntrys.values().stream().filter(e -> !e.canceled).forEach(entry -> {
                        beatLocalHealth(entry);
                    });
                },
                18,
                Math.max(2000, ttls * 1000 - 168),
                TimeUnit.MILLISECONDS);

        if (this.taskFuture2 != null) {
            this.taskFuture2.cancel(true);
        }
        this.taskFuture2 = this.scheduler.scheduleAtFixedRate(
                () -> {
                    reloadSncpAddressHealth();
                },
                68 * 2,
                Math.max(2000, ttls * 1000 - 168),
                TimeUnit.MILLISECONDS);

        if (this.taskFuture3 != null) {
            this.taskFuture3.cancel(true);
        }
        this.taskFuture3 = this.scheduler.scheduleAtFixedRate(
                () -> {
                    reloadHttpAddressHealth();
                },
                128 * 3,
                Math.max(2000, ttls * 1000 - 168),
                TimeUnit.MILLISECONDS);

        if (this.taskFuture4 != null) {
            this.taskFuture4.cancel(true);
        }
        this.taskFuture4 = this.scheduler.scheduleAtFixedRate(
                () -> {
                    remoteEntrys.values().stream()
                            .filter(entry -> "SNCP".equalsIgnoreCase(entry.protocol))
                            .forEach(entry -> {
                                updateSncpAddress(entry);
                            });
                },
                188 * 4,
                Math.max(2000, ttls * 1000 - 168),
                TimeUnit.MILLISECONDS);
    }

    protected void reloadSncpAddressHealth() {
        try {
            String content = Utility.remoteHttpContent(
                    httpClient,
                    "GET",
                    this.apiUrl + "/ns/service/list?pageNo=1&pageSize=99999&namespaceId=" + urlEncode(namespaceid),
                    StandardCharsets.UTF_8,
                    httpHeaders);
            final ServiceList list = JsonConvert.root().convertFrom(ServiceList.class, content);
            Set<String> sncpkeys = new HashSet<>();
            if (list != null && list.doms != null) {
                for (String key : list.doms) {
                    if (key.startsWith("sncp:")) {
                        sncpkeys.add(key);
                    }
                }
            }
            sncpkeys.forEach(serviceName -> {
                try {
                    this.sncpAddressMap.put(
                            serviceName, queryAddress(serviceName).get(Math.max(2, ttls / 2), TimeUnit.SECONDS));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "reloadSncpAddressHealth check " + serviceName + " error", e);
                }
            });
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "reloadSncpAddressHealth check error", ex);
        }
    }

    protected void reloadHttpAddressHealth() {
        try {
            this.httpAddressMap.keySet().stream().forEach(serviceName -> {
                try {
                    this.httpAddressMap.put(
                            serviceName, queryAddress(serviceName).get(Math.max(2, ttls / 2), TimeUnit.SECONDS));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "reloadHttpAddressHealth check " + serviceName + " error", e);
                }
            });
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "reloadHttpAddressHealth check error", ex);
        }
    }

    @Override // 获取SNCP远程服务的可用ip列表
    public CompletableFuture<Set<InetSocketAddress>> querySncpAddress(String protocol, String module, String resname) {
        final String serviceName = generateSncpServiceName(protocol, module, resname);
        Set<InetSocketAddress> rs = sncpAddressMap.get(serviceName);
        if (rs != null) {
            return CompletableFuture.completedFuture(rs);
        }
        return queryAddress(serviceName).thenApply(t -> {
            sncpAddressMap.put(serviceName, t);
            return t;
        });
    }

    @Override // 获取HTTP远程服务的可用ip列表
    public CompletableFuture<Set<InetSocketAddress>> queryHttpAddress(String protocol, String module, String resname) {
        final String serviceName = generateHttpServiceName(protocol, module, resname);
        Set<InetSocketAddress> rs = httpAddressMap.get(serviceName);
        if (rs != null) {
            return CompletableFuture.completedFuture(rs);
        }
        return queryAddress(serviceName).thenApply(t -> {
            httpAddressMap.put(serviceName, t);
            return t;
        });
    }

    @Override
    protected CompletableFuture<Set<InetSocketAddress>> queryAddress(final ClusterEntry entry) {
        return queryAddress(entry.serviceName);
    }

    private CompletableFuture<Set<InetSocketAddress>> queryAddress(final String serviceName) {
        // return (httpClient != null) ? queryAddress11(serviceName) : queryAddress8(serviceName);
        return queryAddress11(serviceName);
    }

    // JDK11+版本以上的纯异步方法
    private CompletableFuture<Set<InetSocketAddress>> queryAddress11(final String serviceName) {
        final HttpClient client = httpClient;
        String url = this.apiUrl + "/ns/instance/list?serviceName=" + urlEncode(serviceName) + "&namespaceId="
                + urlEncode(namespaceid);
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .expectContinue(true)
                .timeout(Duration.ofMillis(6000));
        httpHeaders.forEach((n, v) -> {
            if (v instanceof Collection) {
                for (Object val : (Collection) v) {
                    builder.header(n, val.toString());
                }
            } else {
                builder.header(n, v.toString());
            }
        });
        return client.sendAsync(builder.GET().build(), BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenApply(resp -> {
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
        String querys = "ip=" + host + "&port=" + port + "&serviceName=" + urlEncode(serviceName) + "&groupName="
                + serviceType + "&namespaceId=" + urlEncode(namespaceid) + "&healthyOnly=true";
        try {
            String rs = Utility.remoteHttpContent(
                    httpClient, "GET", this.apiUrl + "/ns/instance?" + querys, StandardCharsets.UTF_8, httpHeaders);
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "isApplicationHealth: " + querys + " --> " + rs);
            }
            // caused: no service DEFAULT_GROUP@@application.platf.node.20100 found!;
            if (!rs.startsWith("{")) {
                return false;
            }
            InstanceDetail detail = JsonConvert.root().convertFrom(InstanceDetail.class, rs);
            return detail != null && detail.healthy;
        } catch (RetcodeException e) {
            if (e.getRetcode() != 404) {
                logger.log(Level.SEVERE, serviceName + " check error", e);
            }
            return false;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " check error", ex);
            return false;
        }
    }

    protected void beatHealth(String serviceName, String serviceType, String host, int port) {
        String beat = "{\"ip\":\"" + host + "\",\"metadata\":{},\"port\":" + port
                + ",\"scheduled\":true,\"groupName\":\"" + serviceType
                + "\",\"namespaceId\":\"" + namespaceid
                + "\",\"serviceName\":\"" + serviceName + "\"}";
        try {
            String rs = Utility.remoteHttpContent(
                    httpClient,
                    "PUT",
                    this.apiUrl + "/ns/instance/beat?serviceName=" + urlEncode(serviceName)
                            + "&groupName=" + serviceType
                            + "&namespaceId=" + urlEncode(namespaceid)
                            + "&beat=" + urlEncode(beat),
                    StandardCharsets.UTF_8,
                    httpHeaders);
            // if (finest) logger.log(Level.FINEST, "checkLocalHealth: " + beat + " --> " + rs);
            if (!rs.startsWith("{")) {
                logger.log(Level.SEVERE, serviceName + " check error: " + rs);
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " check error", ex);
        }
    }

    protected void register(String serviceName, String serviceType, String host, int port) {
        // https://nacos.io/zh-cn/docs/open-api.html#2.1
        String querys = "ip=" + host + "&port=" + port + "&serviceName=" + urlEncode(serviceName) + "&groupName="
                + serviceType + "&namespaceId=" + urlEncode(namespaceid) + "&healthy=true&enabled=true&ephemeral=false";
        try {
            String rs = Utility.remoteHttpContent(
                    httpClient, "POST", this.apiUrl + "/ns/instance?" + querys, StandardCharsets.UTF_8, httpHeaders);
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "register: " + querys + " --> " + rs);
            }
            if (!"ok".equalsIgnoreCase(rs)) {
                logger.log(Level.SEVERE, serviceName + " register error: " + rs);
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " register error", ex);
        }
    }

    protected void deregister(
            String serviceName,
            String serviceType,
            String host,
            int port,
            ClusterEntry currEntry,
            boolean realCanceled) {
        // https://nacos.io/zh-cn/docs/open-api.html#2.2
        String querys = "ip=" + host + "&port=" + port + "&serviceName=" + urlEncode(serviceName) + "&groupName="
                + serviceType + "&namespaceId=" + urlEncode(namespaceid) + "&ephemeral=false";
        try {
            String rs = Utility.remoteHttpContent(
                    httpClient, "DELETE", this.apiUrl + "/ns/instance?" + querys, StandardCharsets.UTF_8, httpHeaders);
            if (realCanceled && currEntry != null) {
                currEntry.canceled = true;
            }
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "deregister: " + querys + " --> " + rs);
            }
            if (!"ok".equalsIgnoreCase(rs)) {
                logger.log(Level.SEVERE, serviceName + " deregister error: " + rs);
            }
        } catch (RetcodeException e) {
            if (e.getRetcode() != 404) {
                logger.log(Level.SEVERE, serviceName + " deregister error", e);
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " deregister error", ex);
        }
    }

    protected void beatApplicationHealth() {
        beatHealth(
                generateApplicationServiceName(),
                generateApplicationServiceType(),
                generateApplicationHost(),
                generateApplicationPort());
    }

    protected void beatLocalHealth(final ClusterEntry cluster) {
        beatHealth(
                cluster.serviceName, cluster.resourceType, cluster.address.getHostString(), cluster.address.getPort());
    }

    @Override
    public void register(Application application) {
        if (isApplicationHealth()) {
            throw new RedkaleException("application.nodeid=" + nodeid + " exists in cluster");
        }
        deregister(application);
        register(
                generateApplicationServiceName(),
                generateApplicationServiceType(),
                generateApplicationHost(),
                generateApplicationPort());
    }

    @Override
    public void deregister(Application application) {
        deregister(
                generateApplicationServiceName(),
                generateApplicationServiceType(),
                generateApplicationHost(),
                generateApplicationPort(),
                null,
                false);
    }

    @Override
    protected ClusterEntry register(NodeServer ns, String protocol, Service service) {
        deregister(ns, protocol, service, false);
        ClusterEntry cluster = new ClusterEntry(ns, protocol, service);
        register(cluster.serviceName, cluster.resourceType, cluster.address.getHostString(), cluster.address.getPort());
        return cluster;
    }

    @Override
    protected void deregister(NodeServer ns, String protocol, Service service) {
        deregister(ns, protocol, service, true);
    }

    protected void deregister(NodeServer ns, String protocol, Service service, boolean realCanceled) {
        String serviceid = generateServiceId(ns, protocol, service);
        ClusterEntry currEntry = localEntrys.values().stream()
                .filter(x -> serviceid.equals(x.serviceid))
                .findAny()
                .orElse(null);
        if (currEntry == null) {
            currEntry = remoteEntrys.values().stream()
                    .filter(x -> serviceid.equals(x.serviceid))
                    .findAny()
                    .orElse(null);
        }
        ClusterEntry cluster = currEntry == null ? new ClusterEntry(ns, protocol, service) : currEntry;
        deregister(
                cluster.serviceName,
                cluster.resourceType,
                cluster.address.getHostString(),
                cluster.address.getPort(),
                currEntry,
                realCanceled);
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
