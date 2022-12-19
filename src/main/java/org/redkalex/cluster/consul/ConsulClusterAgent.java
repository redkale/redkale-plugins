/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cluster.consul;

import org.redkale.cluster.ClusterAgent;
import java.lang.reflect.Type;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import org.redkale.boot.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.Service;
import org.redkale.util.*;

/**
 * <blockquote><pre>
 *  &lt;cluster type="consul" apiurl="http://localhost:8500/v1" ttls="10"&gt;
 *  &lt;/cluster&gt;
 * </pre></blockquote>
 *
 * @author zhangjx
 */
public class ConsulClusterAgent extends ClusterAgent {

    protected static final Map<String, String> httpHeaders = Utility.ofMap("Content-Type", "application/json", "Accept", "application/json");

    protected static final Type MAP_STRING_ADDRESSENTRY = new TypeToken<Map<String, AddressEntry>>() {
    }.getType();

    protected static final Type MAP_STRING_SERVICEENTRY = new TypeToken<Map<String, ServiceEntry>>() {
    }.getType();

    protected String apiUrl; //不会以/结尾 

    protected HttpClient httpClient; //JDK11里面的HttpClient

    protected int ttls = 10; //定时检查的秒数

    protected ScheduledThreadPoolExecutor scheduler;

    protected ScheduledFuture taskFuture1;

    protected ScheduledFuture taskFuture2;

    protected ScheduledFuture taskFuture3;

    protected ScheduledFuture taskFuture4;

    //可能被HttpMessageClient用到的服务 key: serviceName
    protected final ConcurrentHashMap<String, Collection<InetSocketAddress>> httpAddressMap = new ConcurrentHashMap<>();

    //可能被mqtp用到的服务 key: serviceName
    protected final ConcurrentHashMap<String, Collection<InetSocketAddress>> mqtpAddressMap = new ConcurrentHashMap<>();

    @Override
    public void init(ResourceFactory factory, AnyValue config) {
        super.init(factory, config);

        this.apiUrl = config.getValue("apiurl");
        if (this.apiUrl.endsWith("/")) {
            this.apiUrl = this.apiUrl.substring(0, this.apiUrl.length() - 1);
        }
        this.ttls = config.getIntValue("ttls", 10);
        if (this.ttls < 5) this.ttls = 10;

        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    @ResourceListener
    public void onResourceChange(ResourceEvent[] events) {
        StringBuilder sb = new StringBuilder();
        int newTtls = this.ttls;
        for (ResourceEvent event : events) {
            if ("ttls".equals(event.name())) {
                newTtls = Integer.parseInt(event.newValue().toString());
                if (newTtls < 5) {
                    sb.append(ConsulClusterAgent.class.getSimpleName()).append(" cannot change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
                } else {
                    sb.append(ConsulClusterAgent.class.getSimpleName()).append(" change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
                }
            } else {
                sb.append(ConsulClusterAgent.class.getSimpleName()).append(" skip change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
            }
        }
        if (newTtls != this.ttls) {
            this.ttls = newTtls;
            start();
        }
        if (!sb.isEmpty()) {
            logger.log(Level.INFO, sb.toString());
        }
    }

    @Override
    public void destroy(AnyValue config) {
        if (scheduler != null) scheduler.shutdownNow();
    }

    @Override //ServiceLoader时判断配置是否符合当前实现类
    public boolean acceptsConf(AnyValue config) {
        if (config == null) return false;
        if (!"consul".equalsIgnoreCase(config.getValue("type"))) return false;
        return config.getValue("apiurl") != null;
    }

    @Override
    public void start() {
        if (this.scheduler == null) {
            this.scheduler = new ScheduledThreadPoolExecutor(4, (Runnable r) -> {
                final Thread t = new Thread(r, ConsulClusterAgent.class.getSimpleName() + "-Task-Thread");
                t.setDaemon(true);
                return t;
            });
        }
        //delay为了错开请求
        if (this.taskFuture1 != null) this.taskFuture1.cancel(true);
        this.taskFuture1 = this.scheduler.scheduleAtFixedRate(() -> {
            beatApplicationHealth();
            localEntrys.values().stream().filter(e -> !e.canceled).forEach(entry -> {
                beatLocalHealth(entry);
            });
        }, 18, Math.max(2000, ttls * 1000 - 168), TimeUnit.MILLISECONDS);

        if (this.taskFuture2 != null) this.taskFuture2.cancel(true);
        this.taskFuture2 = this.scheduler.scheduleAtFixedRate(() -> {
            reloadMqtpAddressHealth();
        }, 88 * 2, Math.max(2000, ttls * 1000 - 168), TimeUnit.MILLISECONDS);

        if (this.taskFuture3 != null) this.taskFuture4.cancel(true);
        this.taskFuture3 = this.scheduler.scheduleAtFixedRate(() -> {
            reloadHttpAddressHealth();
        }, 128 * 3, Math.max(2000, ttls * 1000 - 168), TimeUnit.MILLISECONDS);

        if (this.taskFuture4 != null) this.taskFuture4.cancel(true);
        this.taskFuture4 = this.scheduler.scheduleAtFixedRate(() -> {
            remoteEntrys.values().stream().filter(entry -> "SNCP".equalsIgnoreCase(entry.protocol)).forEach(entry -> {
                updateSncpTransport(entry);
            });
        }, 188 * 4, Math.max(2000, ttls * 1000 - 168), TimeUnit.MILLISECONDS);

    }

    protected void reloadMqtpAddressHealth() {
        try {
            String content = Utility.remoteHttpContent(httpClient, "GET", this.apiUrl + "/agent/services", StandardCharsets.UTF_8, httpHeaders);
            final Map<String, ServiceEntry> map = JsonConvert.root().convertFrom(MAP_STRING_SERVICEENTRY, content);
            Set<String> mqtpkeys = new HashSet<>();
            map.forEach((key, en) -> {
                if (en.Service.startsWith("mqtp:")) mqtpkeys.add(en.Service);
            });
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

    protected void beatLocalHealth(final ClusterEntry entry) {
        String url = this.apiUrl + "/agent/check/pass/" + entry.checkid;
        try {
            String rs = Utility.remoteHttpContent(httpClient, "PUT", url, StandardCharsets.UTF_8, httpHeaders);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, entry.checkid + " check error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, entry.checkid + " check error: " + url, ex);
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
        final HttpClient client = (HttpClient) httpClient;
        String url = this.apiUrl + "/agent/services?filter=" + URLEncoder.encode("Service==\"" + serviceName + "\"", StandardCharsets.UTF_8);
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url)).expectContinue(true).timeout(Duration.ofMillis(6000));
        httpHeaders.forEach((n, v) -> builder.header(n, v));
        final Set<InetSocketAddress> set = new CopyOnWriteArraySet<>();
        return client.sendAsync(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).thenApply(resp -> resp.body()).thenCompose(content -> {
            final Map<String, AddressEntry> map = JsonConvert.root().convertFrom(MAP_STRING_ADDRESSENTRY, (String) content);
            if (map.isEmpty()) return CompletableFuture.completedFuture(set);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Map.Entry<String, AddressEntry> en : map.entrySet()) {
                String url0 = this.apiUrl + "/agent/health/service/id/" + en.getKey() + "?format=text";
                HttpRequest.Builder builder0 = HttpRequest.newBuilder().uri(URI.create(url0)).expectContinue(true).timeout(Duration.ofMillis(6000));
                httpHeaders.forEach((n, v) -> builder0.header(n, v));
                futures.add(client.sendAsync(builder0.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).thenApply(resp -> resp.body()).thenApply(irs -> {
                    if ("passing".equalsIgnoreCase(irs)) {
                        set.add(en.getValue().createSocketAddress());
                    } else {
                        logger.log(Level.INFO, en.getKey() + " (url=" + url0 + ") bad result: " + irs);
                    }
                    return null;
                }));
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> set);
        });
    }

    @Deprecated
    CompletableFuture<Collection<InetSocketAddress>> queryAddress8(final String serviceName) {
        final HashSet<InetSocketAddress> set = new HashSet<>();
        String rs = null;
        try {
            String url = this.apiUrl + "/agent/services?filter=" + URLEncoder.encode("Service==\"" + serviceName + "\"", StandardCharsets.UTF_8);
            rs = Utility.remoteHttpContent(httpClient, "GET", url, StandardCharsets.UTF_8, httpHeaders);
            Map<String, AddressEntry> map = JsonConvert.root().convertFrom(MAP_STRING_ADDRESSENTRY, rs);
            map.forEach((serviceid, en) -> {
                try {
                    String irs = Utility.remoteHttpContent(httpClient, "GET", this.apiUrl + "/agent/health/service/id/" + serviceid + "?format=text", StandardCharsets.UTF_8, httpHeaders);
                    if ("passing".equalsIgnoreCase(irs)) {
                        set.add(en.createSocketAddress());
                    } else {
                        logger.log(Level.INFO, serviceid + " (url=" + url + ") bad result: " + irs);
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, serviceid + " health format=text error", e);
                }
            });
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceName + " queryAddress error, result=" + rs, ex);
        }
        return CompletableFuture.completedFuture(set);
    }

    protected boolean isApplicationHealth() {
        String serviceid = generateApplicationServiceId();
        try {
            String irs = Utility.remoteHttpContent(httpClient, "GET", this.apiUrl + "/agent/health/service/id/" + serviceid + "?format=text", StandardCharsets.UTF_8, httpHeaders);
            return "passing".equalsIgnoreCase(irs);
        } catch (java.io.FileNotFoundException ex) {
            return false;
        } catch (Exception e) {
            logger.log(Level.SEVERE, serviceid + " health format=text error", e);
            return true;
        }
    }

    protected void beatApplicationHealth() {
        String checkid = generateApplicationCheckId();
        try {
            String rs = Utility.remoteHttpContent(httpClient, "PUT", this.apiUrl + "/agent/check/pass/" + checkid, StandardCharsets.UTF_8, httpHeaders);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, checkid + " check error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, checkid + " check error", ex);
        }
    }

    @Override
    public void register(Application application) {
        if (isApplicationHealth()) throw new RuntimeException("application.nodeid=" + nodeid + " exists in cluster");
        deregister(application);

        String serviceid = generateApplicationServiceId();
        String serviceName = generateApplicationServiceName();
        String host = this.appAddress.getHostString();
        String json = "{\"ID\": \"" + serviceid + "\",\"Name\": \"" + serviceName + "\",\"Address\": \"" + host + "\",\"Port\": " + this.appAddress.getPort()
            + ",\"Check\":{\"CheckID\": \"" + generateApplicationCheckId() + "\",\"Name\": \"" + generateApplicationCheckName() + "\",\"TTL\":\"" + ttls + "s\",\"Notes\":\"Interval " + ttls + "s Check\"}}";
        try {
            String rs = Utility.remoteHttpContent(httpClient, "PUT", this.apiUrl + "/agent/service/register", StandardCharsets.UTF_8, httpHeaders, json);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, serviceid + " register error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceid + " register error", ex);
        }
    }

    @Override
    public void deregister(Application application) {
        String serviceid = generateApplicationServiceId();
        try {
            String rs = Utility.remoteHttpContent(httpClient, "PUT", this.apiUrl + "/agent/service/deregister/" + serviceid, StandardCharsets.UTF_8, httpHeaders);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, serviceid + " deregister error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceid + " deregister error", ex);
        }
    }

    @Override
    protected ClusterEntry register(NodeServer ns, String protocol, Service service) {
        deregister(ns, protocol, service, false);
        //
        ClusterEntry clusterEntry = new ClusterEntry(ns, protocol, service);
        String json = "{\"ID\": \"" + clusterEntry.serviceid + "\",\"Name\": \"" + clusterEntry.serviceName + "\",\"Address\": \"" + clusterEntry.address.getHostString() + "\",\"Port\": " + clusterEntry.address.getPort()
            + ",\"Check\":{\"CheckID\": \"" + generateCheckId(ns, protocol, service) + "\",\"Name\": \"" + generateCheckName(ns, protocol, service) + "\",\"TTL\":\"" + ttls + "s\",\"Notes\":\"Interval " + ttls + "s Check\"}}";
        try {
            String rs = Utility.remoteHttpContent(httpClient, "PUT", this.apiUrl + "/agent/service/register", StandardCharsets.UTF_8, httpHeaders, json);
            if (rs.isEmpty()) {
                //需要立马执行下check，否则立即queryAddress可能会得到critical
                Utility.remoteHttpContent(httpClient, "PUT", this.apiUrl + "/agent/check/pass/" + generateCheckId(ns, protocol, service), StandardCharsets.UTF_8, httpHeaders);
            } else {
                logger.log(Level.SEVERE, clusterEntry.serviceid + " register error: " + rs);
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, clusterEntry.serviceid + " register error", ex);
            return null;
        }
        return clusterEntry;
    }

    @Override
    protected void deregister(NodeServer ns, String protocol, Service service) {
        deregister(ns, protocol, service, true);
    }

    protected void deregister(NodeServer ns, String protocol, Service service, boolean realcanceled) {
        String serviceid = generateServiceId(ns, protocol, service);
        ClusterEntry currEntry = null;
        for (final ClusterEntry entry : localEntrys.values()) {
            if (entry.serviceid.equals(serviceid)) {
                currEntry = entry;
                break;
            }
        }
        if (currEntry == null) {
            for (final ClusterEntry entry : remoteEntrys.values()) {
                if (entry.serviceid.equals(serviceid)) {
                    currEntry = entry;
                    break;
                }
            }
        }
        try {
            String rs = Utility.remoteHttpContent(httpClient, "PUT", this.apiUrl + "/agent/service/deregister/" + serviceid, StandardCharsets.UTF_8, httpHeaders);
            if (realcanceled && currEntry != null) currEntry.canceled = true;
            if (!rs.isEmpty()) logger.log(Level.SEVERE, serviceid + " deregister error: " + rs);
            if (!"mqtp".equals(protocol) && currEntry != null && currEntry.submqtp) deregister(ns, "mqtp", service, realcanceled);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceid + " deregister error，protocol=" + protocol + ", service=" + service + ", currEntry=" + currEntry, ex);
        }

    }

    public static final class ServiceEntry {

        public String ID;  //serviceid

        public String Service; //serviceName
    }

    public static final class AddressEntry {

        public String Address;

        public int Port;

        public InetSocketAddress createSocketAddress() {
            return new InetSocketAddress(Address, Port);
        }
    }
}
