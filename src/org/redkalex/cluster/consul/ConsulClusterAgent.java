/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cluster.consul;

import org.redkale.cluster.ClusterAgent;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;
import org.redkale.boot.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.Service;
import org.redkale.util.*;

/**
 * <blockquote><pre>
 *  &lt;cluster value="org.redkalex.cluster.consul.ConsulClusterAgent"&gt;
 *      &lt;property name="apiurl" value="http://localhost:8500/v1"/&gt;
 *      &lt;property name="ttls" value="10"/&gt;
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

    protected String apiurl;

    protected Object httpClient; //JDK11里面的HttpClient

    protected int ttls = 10; //定时检查的秒数

    protected ScheduledThreadPoolExecutor scheduler;

    //可能被HttpMessageClient用到的服务 key: servicename
    protected final ConcurrentHashMap<String, Collection<InetSocketAddress>> httpAddressMap = new ConcurrentHashMap<>();

    //可能被mqtp用到的服务 key: servicename
    protected final ConcurrentHashMap<String, Collection<InetSocketAddress>> mqtpAddressMap = new ConcurrentHashMap<>();

    @Override
    public void init(AnyValue config) {
        super.init(config);
        AnyValue[] properties = config.getAnyValues("property");
        for (AnyValue property : properties) {
            if ("apiurl".equalsIgnoreCase(property.getValue("name"))) {
                this.apiurl = property.getValue("value", "").trim();
                if (this.apiurl.endsWith("/")) this.apiurl = this.apiurl.substring(0, this.apiurl.length() - 1);
            } else if ("ttls".equalsIgnoreCase(property.getValue("name"))) {
                this.ttls = Integer.parseInt(property.getValue("value", "").trim());
                if (this.ttls < 5) this.ttls = 10;
            }
        }
        try {
            Class httpClientClass = Class.forName("java.net.http.HttpClient");
            this.httpClient = httpClientClass.getMethod("newHttpClient").invoke(null);
        } catch (Exception e) {
            //不是JDK11+
        }
    }

    @Override
    public void destroy(AnyValue config) {
        if (scheduler != null) scheduler.shutdownNow();
    }

    @Override //ServiceLoader时判断配置是否符合当前实现类
    public boolean match(AnyValue config) {
        if (config == null) return false;
        AnyValue[] properties = config.getAnyValues("property");
        if (properties == null || properties.length == 0) return false;
        for (AnyValue property : properties) {
            if ("apiurl".equalsIgnoreCase(property.getValue("name"))) return true;
        }
        return false;
    }

    @Override
    public void start() {
        if (this.scheduler == null) {
            this.scheduler = new ScheduledThreadPoolExecutor(4, (Runnable r) -> {
                final Thread t = new Thread(r, ConsulClusterAgent.class.getSimpleName() + "-Task-Thread");
                t.setDaemon(true);
                return t;
            });

            this.scheduler.scheduleAtFixedRate(() -> {
                checkApplicationHealth();
                checkHttpAddressHealth();
            }, 0, ttls * 1000 * 4 / 5, TimeUnit.MILLISECONDS);

            this.scheduler.scheduleAtFixedRate(() -> {
                loadMqtpAddressHealth();
            }, 0, ttls * 1000, TimeUnit.MILLISECONDS);

            AtomicInteger offset = new AtomicInteger();
            for (final ClusterEntry entry : localEntrys.values()) {
                entry.checkScheduledFuture = this.scheduler.scheduleAtFixedRate(() -> {
                    checkLocalHealth(entry);
                }, offset.incrementAndGet() * 100, ttls * 1000 * 4 / 5, TimeUnit.MILLISECONDS);
            }
            for (final ClusterEntry entry : remoteEntrys.values()) {
                if (!"SNCP".equalsIgnoreCase(entry.protocol)) continue;
                entry.checkScheduledFuture = this.scheduler.scheduleAtFixedRate(() -> {
                    updateSncpTransport(entry);
                }, offset.incrementAndGet() * 88, ttls * 1000, TimeUnit.MILLISECONDS);  //88错开delay
            }
        }
    }

    protected void loadMqtpAddressHealth() {
        try {
            String content = Utility.remoteHttpContent("GET", this.apiurl + "/agent/services", httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
            final Map<String, ServiceEntry> map = JsonConvert.root().convertFrom(MAP_STRING_SERVICEENTRY, content);
            Set<String> mqtpkeys = new HashSet<>();
            map.forEach((key, en) -> {
                if (en.Service.startsWith("mqtp:")) mqtpkeys.add(en.Service);
            });
            mqtpkeys.forEach(servicename -> {
                try {
                    this.mqtpAddressMap.put(servicename, queryAddress(servicename).get(10, TimeUnit.SECONDS));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "loadMqtpAddressHealth check " + servicename + " error", e);
                }
            });
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "loadMqtpAddressHealth check error", ex);
        }
    }

    protected void checkHttpAddressHealth() {
        try {
            this.httpAddressMap.keySet().stream().forEach(servicename -> {
                try {
                    this.httpAddressMap.put(servicename, queryAddress(servicename).get(10, TimeUnit.SECONDS));
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "checkHttpAddressHealth check " + servicename + " error", e);
                }
            });
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "checkHttpAddressHealth check error", ex);
        }
    }

    protected void checkLocalHealth(final ClusterEntry entry) {
        String url = this.apiurl + "/agent/check/pass/" + entry.checkid;
        try {
            String rs = Utility.remoteHttpContent("PUT", url, httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, entry.checkid + " check error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, entry.checkid + " check error: " + url, ex);
        }
    }

    @Override //获取MQTP的HTTP远程服务的可用ip列表, key = servicename的后半段
    public CompletableFuture<Map<String, Collection<InetSocketAddress>>> queryMqtpAddress(String protocol, String module, String resname) {
        final Map<String, Collection<InetSocketAddress>> rsmap = new ConcurrentHashMap<>();
        final String servicenamprefix = generateHttpServiceName(protocol, module, null) + ":";
        mqtpAddressMap.keySet().stream().filter(k -> k.startsWith(servicenamprefix))
            .forEach(sn -> rsmap.put(sn.substring(servicenamprefix.length()), mqtpAddressMap.get(sn)));
        return CompletableFuture.completedFuture(rsmap);
    }

    @Override //获取HTTP远程服务的可用ip列表
    public CompletableFuture<Collection<InetSocketAddress>> queryHttpAddress(String protocol, String module, String resname) {
        final String servicename = generateHttpServiceName(protocol, module, resname);
        Collection<InetSocketAddress> rs = httpAddressMap.get(servicename);
        if (rs != null) return CompletableFuture.completedFuture(rs);
        return queryAddress(servicename).thenApply(t -> {
            httpAddressMap.put(servicename, t);
            return t;
        });
    }

    @Override
    protected CompletableFuture<Collection<InetSocketAddress>> queryAddress(final ClusterEntry entry) {
        return queryAddress(entry.servicename);
    }

    private CompletableFuture<Collection<InetSocketAddress>> queryAddress(final String servicename) {
        return (httpClient != null) ? queryAddress11(servicename) : queryAddress8(servicename);
    }

    //JDK11+版本以上的纯异步方法
    private CompletableFuture<Collection<InetSocketAddress>> queryAddress11(final String servicename) {
        final java.net.http.HttpClient client = (java.net.http.HttpClient) httpClient;
        String url = this.apiurl + "/agent/services?filter=" + URLEncoder.encode("Service==\"" + servicename + "\"", StandardCharsets.UTF_8);
        java.net.http.HttpRequest.Builder builder = java.net.http.HttpRequest.newBuilder().uri(URI.create(url)).timeout(Duration.ofMillis(6000));
        httpHeaders.forEach((n, v) -> builder.header(n, v));
        final Set<InetSocketAddress> set = new CopyOnWriteArraySet<>();
        return client.sendAsync(builder.build(), java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).thenApply(resp -> resp.body()).thenCompose(content -> {
            final Map<String, AddressEntry> map = JsonConvert.root().convertFrom(MAP_STRING_ADDRESSENTRY, (String) content);
            if (map.isEmpty()) return CompletableFuture.completedFuture(set);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Map.Entry<String, AddressEntry> en : map.entrySet()) {
                String url0 = this.apiurl + "/agent/health/service/id/" + en.getKey() + "?format=text";
                java.net.http.HttpRequest.Builder builder0 = java.net.http.HttpRequest.newBuilder().uri(URI.create(url0)).timeout(Duration.ofMillis(6000));
                httpHeaders.forEach((n, v) -> builder0.header(n, v));
                futures.add(client.sendAsync(builder0.build(), java.net.http.HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).thenApply(resp -> resp.body()).thenApply(irs -> {
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

    private CompletableFuture<Collection<InetSocketAddress>> queryAddress8(final String servicename) {
        final HashSet<InetSocketAddress> set = new HashSet<>();
        String rs = null;
        String url = this.apiurl + "/agent/services?filter=" + URLEncoder.encode("Service==\"" + servicename + "\"", StandardCharsets.UTF_8);
        try {
            rs = Utility.remoteHttpContent("GET", url, httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
            Map<String, AddressEntry> map = JsonConvert.root().convertFrom(MAP_STRING_ADDRESSENTRY, rs);
            map.forEach((serviceid, en) -> {
                try {
                    String irs = Utility.remoteHttpContent("GET", this.apiurl + "/agent/health/service/id/" + serviceid + "?format=text", httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
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
            logger.log(Level.SEVERE, servicename + " queryAddress error, result=" + rs, ex);
        }
        return CompletableFuture.completedFuture(set);
    }

    protected boolean isApplicationHealth() {
        String serviceid = generateApplicationServiceId();
        try {
            String irs = Utility.remoteHttpContent("GET", this.apiurl + "/agent/health/service/id/" + serviceid + "?format=text", httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
            return "passing".equalsIgnoreCase(irs);
        } catch (java.io.FileNotFoundException ex) {
            return false;
        } catch (Exception e) {
            logger.log(Level.SEVERE, serviceid + " health format=text error", e);
            return true;
        }
    }

    protected void checkApplicationHealth() {
        String checkid = generateApplicationCheckId();
        try {
            String rs = Utility.remoteHttpContent("PUT", this.apiurl + "/agent/check/pass/" + checkid, httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
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
        String servicename = generateApplicationServiceName();
        String host = this.appAddress.getHostString();
        String json = "{\"ID\": \"" + serviceid + "\",\"Name\": \"" + servicename + "\",\"Address\": \"" + host + "\",\"Port\": " + this.appAddress.getPort()
            + ",\"Check\":{\"CheckID\": \"" + generateApplicationCheckId() + "\",\"Name\": \"" + generateApplicationCheckName() + "\",\"TTL\":\"" + ttls + "s\",\"Notes\":\"Interval " + ttls + "s Check\"}}";
        try {
            String rs = Utility.remoteHttpContent("PUT", this.apiurl + "/agent/service/register", httpHeaders, json).toString(StandardCharsets.UTF_8);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, serviceid + " register error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceid + " register error", ex);
        }
    }

    @Override
    public void deregister(Application application) {
        String serviceid = generateApplicationServiceId();
        try {
            String rs = Utility.remoteHttpContent("PUT", this.apiurl + "/agent/service/deregister/" + serviceid, httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, serviceid + " deregister error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceid + " deregister error", ex);
        }
    }

    @Override
    protected void register(NodeServer ns, String protocol, Service service) {
        deregister(ns, protocol, service);
        //
        String serviceid = generateServiceId(ns, protocol, service);
        String servicename = generateServiceName(ns, protocol, service);
        InetSocketAddress address = ns.isSNCP() ? ns.getSncpAddress() : ns.getServer().getSocketAddress();
        String host = address.getHostString();
        if ("0.0.0.0".equals(host)) host = appAddress.getHostString();
        String json = "{\"ID\": \"" + serviceid + "\",\"Name\": \"" + servicename + "\",\"Address\": \"" + host + "\",\"Port\": " + address.getPort()
            + ",\"Check\":{\"CheckID\": \"" + generateCheckId(ns, protocol, service) + "\",\"Name\": \"" + generateCheckName(ns, protocol, service) + "\",\"TTL\":\"" + ttls + "s\",\"Notes\":\"Interval " + ttls + "s Check\"}}";
        try {
            String rs = Utility.remoteHttpContent("PUT", this.apiurl + "/agent/service/register", httpHeaders, json).toString(StandardCharsets.UTF_8);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, serviceid + " register error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceid + " register error", ex);
        }
    }

    @Override
    protected void deregister(NodeServer ns, String protocol, Service service) {
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
            String rs = Utility.remoteHttpContent("PUT", this.apiurl + "/agent/service/deregister/" + serviceid, httpHeaders, (String) null).toString(StandardCharsets.UTF_8);
            if (currEntry != null && currEntry.checkScheduledFuture != null) currEntry.checkScheduledFuture.cancel(true);
            if (!rs.isEmpty()) logger.log(Level.SEVERE, serviceid + " deregister error: " + rs);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, serviceid + " deregister error", ex);
        }
    }

    public static class ServiceEntry {

        public String ID;  //serviceid

        public String Service; //servicename
    }

    public static class AddressEntry {

        public String Address;

        public int Port;

        public InetSocketAddress createSocketAddress() {
            return new InetSocketAddress(Address, Port);
        }
    }
}
