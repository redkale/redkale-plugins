/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.io.*;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.config.*;
import org.redkale.convert.Convert;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.*;
import org.redkale.source.*;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_MAXCONNS;
import org.redkale.util.*;

/**
 * //https://www.cnblogs.com/xiami2046/p/13934146.html
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@ResourceType(CacheSource.class)
public class RedissionCacheSource extends AbstractCacheSource {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Resource
    protected JsonConvert defaultConvert;

    @Resource(name = "$_convert")
    protected JsonConvert convert;

    protected Type objValueType = String.class;

    protected List<String> nodeAddrs;

    protected int db;

    protected RedissonClient client;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = this.defaultConvert;
        if (conf == null) conf = new AnyValue.DefaultAnyValue();

        final List<String> addresses = new ArrayList<>();
        Config config = new Config();
        AnyValue[] nodes = getNodes(conf);
        String cluster = conf.getOrDefault("cluster", "");
        int maxconns = conf.getIntValue(CACHE_SOURCE_MAXCONNS, Utility.cpus());
        BaseConfig baseConfig = null;
        for (AnyValue node : nodes) {
            String addr = node.getValue(CACHE_SOURCE_URL, node.getValue("addr"));  //兼容addr
            String db0 = node.getValue(CACHE_SOURCE_DB, "").trim();
            if (!db0.isEmpty()) this.db = Integer.valueOf(db0);
            String username = node.getValue(CACHE_SOURCE_USER, "").trim();
            String password = node.getValue(CACHE_SOURCE_PASSWORD, "").trim();
            if (addr.startsWith("redis")) {
                URI uri = URI.create(addr);
                if (uri.getQuery() != null && !uri.getQuery().isEmpty()) {
                    String[] qrys = uri.getQuery().split("&|=");
                    for (int i = 0; i < qrys.length; i += 2) {
                        if (CACHE_SOURCE_USER.equals(qrys[i])) {
                            username = i == qrys.length - 1 ? "" : qrys[i + 1];
                        } else if (CACHE_SOURCE_PASSWORD.equals(qrys[i])) {
                            password = i == qrys.length - 1 ? "" : qrys[i + 1];
                        } else if (CACHE_SOURCE_DB.equals(qrys[i])) {
                            String urldb = i == qrys.length - 1 ? "-1" : qrys[i + 1];
                            this.db = Integer.valueOf(urldb);
                        }
                        if (CACHE_SOURCE_MAXCONNS.equals(qrys[i])) {
                            maxconns = i == qrys.length - 1 ? Utility.cpus() : Integer.parseInt(qrys[i + 1]);
                        }
                    }
                }
            }
            addresses.add(addr);
            if (nodes.length == 1) {
                baseConfig = config.useSingleServer();
                if (maxconns > 0) {
                    config.useSingleServer().setConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useSingleServer().setConnectionPoolSize(maxconns);
                }
                config.useSingleServer().setAddress(addr);
                config.useSingleServer().setDatabase(this.db);
            } else if ("masterslave".equalsIgnoreCase(cluster)) { //主从
                baseConfig = config.useMasterSlaveServers();
                if (maxconns > 0) {
                    config.useMasterSlaveServers().setMasterConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useMasterSlaveServers().setMasterConnectionPoolSize(maxconns);
                    config.useMasterSlaveServers().setSlaveConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useMasterSlaveServers().setSlaveConnectionPoolSize(maxconns);
                }
                if (node.get("master") != null) {
                    config.useMasterSlaveServers().setMasterAddress(addr);
                } else {
                    config.useMasterSlaveServers().addSlaveAddress(addr);
                }
                config.useMasterSlaveServers().setDatabase(this.db);
            } else if ("cluster".equalsIgnoreCase(cluster)) { //集群
                baseConfig = config.useClusterServers();
                if (maxconns > 0) {
                    config.useClusterServers().setMasterConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useClusterServers().setMasterConnectionPoolSize(maxconns);
                    config.useClusterServers().setSlaveConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useClusterServers().setSlaveConnectionPoolSize(maxconns);
                }
                config.useClusterServers().addNodeAddress(addr);
            } else if ("replicated".equalsIgnoreCase(cluster)) { //
                baseConfig = config.useReplicatedServers();
                if (maxconns > 0) {
                    config.useReplicatedServers().setMasterConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useReplicatedServers().setMasterConnectionPoolSize(maxconns);
                    config.useReplicatedServers().setSlaveConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useReplicatedServers().setSlaveConnectionPoolSize(maxconns);
                }
                config.useReplicatedServers().addNodeAddress(addr);
                config.useReplicatedServers().setDatabase(this.db);
            } else if ("sentinel".equalsIgnoreCase(cluster)) { //
                baseConfig = config.useSentinelServers();
                if (maxconns > 0) {
                    config.useSentinelServers().setMasterConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useSentinelServers().setMasterConnectionPoolSize(maxconns);
                    config.useSentinelServers().setSlaveConnectionMinimumIdleSize(maxconns / 2 + 1);
                    config.useSentinelServers().setSlaveConnectionPoolSize(maxconns);
                }
                config.useSentinelServers().addSentinelAddress(addr);
                config.useSentinelServers().setDatabase(this.db);
            }
            if (baseConfig != null) {  //单个进程的不同自定义密码
                if (!username.isEmpty()) baseConfig.setUsername(username);
                if (!password.isEmpty()) baseConfig.setPassword(password);
            }
        }
        if (baseConfig != null) { //配置全局密码
            String username = conf.getValue(CACHE_SOURCE_USER, "").trim();
            String password = conf.getValue(CACHE_SOURCE_PASSWORD, "").trim();
            String retryAttempts = conf.getValue("retryAttempts", "").trim();
            String retryInterval = conf.getValue("retryInterval", "").trim();
            if (!username.isEmpty()) baseConfig.setUsername(username);
            if (!password.isEmpty()) baseConfig.setPassword(password);
            if (!retryAttempts.isEmpty()) baseConfig.setRetryAttempts(Integer.parseInt(retryAttempts));
            if (!retryInterval.isEmpty()) baseConfig.setRetryInterval(Integer.parseInt(retryInterval));
        }
        this.client = Redisson.create(config);
        this.nodeAddrs = addresses;
//        RTopic topic = client.getTopic("__keyevent@" + db + "__:expired", new StringCodec());
//        topic.addListener(String.class, (CharSequence cs, String key) -> {
//            if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedissionCacheSource.class.getSimpleName() + "." + db + ": expired key=" + key + ", cs=" + cs);
//        });
        //if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedissionCacheSource.class.getSimpleName() + ": addrs=" + addresses + ", db=" + db);

    }

    public boolean acceptsConf(AnyValue config) {
        if (config == null) return false;
        AnyValue[] nodes = getNodes(config);
        if (nodes == null || nodes.length == 0) return false;
        for (AnyValue node : nodes) {
            String val = node.getValue(CACHE_SOURCE_URL, node.getValue("addr"));  //兼容addr
            if (val != null && val.startsWith("redis://")) return true;
            if (val != null && val.startsWith("rediss://")) return true;
        }
        return false;
    }

    protected AnyValue[] getNodes(AnyValue config) {
        AnyValue[] nodes = config.getAnyValues(CACHE_SOURCE_NODE);
        if (nodes == null || nodes.length == 0) {
            AnyValue one = config.getAnyValue(CACHE_SOURCE_NODE);
            if (one == null) {
                String val = config.getValue(CACHE_SOURCE_URL);
                if (val == null) return nodes;
                nodes = new AnyValue[]{config};
            } else {
                nodes = new AnyValue[]{one};
            }
        }
        return nodes;
    }
    
    @Override
    public final String getType() {
        return "redis";
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{addrs=" + this.nodeAddrs + ", db=" + this.db + "}";
    }

    @Local
    public RedissonClient getRedissonClient() {
        return client;
    }

    protected <T> CompletableFuture<T> completableFuture(CompletionStage<T> rf) {
        return rf.toCompletableFuture();
    }

    @Override
    public void close() throws Exception {  //在 Application 关闭时调用
        destroy(null);
    }

    @Override
    public String resourceName() {
        Resource res = this.getClass().getAnnotation(Resource.class);
        return res == null ? "" : res.name();
    }

    @Override
    public void destroy(AnyValue conf) {
        if (client != null) client.shutdown();
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.isExistsAsync());
    }

    @Override
    public boolean exists(String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.isExists();
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenApply(bs -> bs == null ? null : convert.convertFrom(type, bs)));
    }

    @Override
    public CompletableFuture<String> getStringAsync(String key) {
        final RBucket<String> bucket = client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.getAsync());
    }

    @Override
    public CompletableFuture<String> getSetStringAsync(String key, String value) {
        final RBucket<String> bucket = client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.getAndSetAsync(value));
    }

    @Override
    public CompletableFuture<Long> getLongAsync(String key, long defValue) {
        final RAtomicLong bucket = client.getAtomicLong(key);
        return completableFuture(bucket.getAsync());
    }

    @Override
    public CompletableFuture<Long> getSetLongAsync(String key, long value, long defValue) {
        final RAtomicLong bucket = client.getAtomicLong(key);
        return completableFuture(bucket.getAndSetAsync(value));
    }

    @Override
    public <T> T get(String key, final Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        byte[] bs = bucket.get();
        return bs == null ? null : convert.convertFrom(type, bs);
    }

    @Override
    public String getString(String key) {
        final RBucket<String> bucket = client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return bucket.get();
    }

    @Override
    public String getSetString(String key, String value) {
        final RBucket<String> bucket = client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return bucket.getAndSet(value);
    }

    @Override
    public long getLong(String key, long defValue) {
        final RAtomicLong bucket = client.getAtomicLong(key);
        return bucket.get();
    }

    @Override
    public long getSetLong(String key, long value, long defValue) {
        final RAtomicLong bucket = client.getAtomicLong(key);
        return bucket.getAndAdd(value);
    }

    //--------------------- getAndRefresh ------------------------------
    @Override
    public <T> CompletableFuture<T> getAndRefreshAsync(String key, int expireSeconds, final Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenCompose(bs -> {
            T rs = convert.convertFrom(type, bs);
            if (rs == null) return CompletableFuture.completedFuture(null);
            return bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> rs);
        }));
    }

    @Override
    public <T> T getAndRefresh(String key, final int expireSeconds, final Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        T rs = convert.convertFrom(type, bucket.get());
        if (rs == null) return rs;
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
        return rs;
    }

    @Override
    public CompletableFuture<String> getStringAndRefreshAsync(String key, int expireSeconds) {
        final RBucket<String> bucket = client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenCompose(rs -> {
            if (rs == null) return CompletableFuture.completedFuture(null);
            return bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> rs);
        }));
    }

    @Override
    public String getStringAndRefresh(String key, final int expireSeconds) {
        final RBucket<String> bucket = client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        String rs = bucket.get();
        if (rs == null) return rs;
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
        return rs;
    }

    @Override
    public CompletableFuture<Long> getLongAndRefreshAsync(String key, int expireSeconds, long defValue) {
        final RAtomicLong bucket = client.getAtomicLong(key);
        return completableFuture(bucket.getAsync().thenCompose(rs -> {
            if (rs == null) return CompletableFuture.completedFuture(defValue);
            return bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> rs);
        }));
    }

    @Override
    public long getLongAndRefresh(String key, final int expireSeconds, long defValue) {
        final RAtomicLong bucket = client.getAtomicLong(key);
        long rs = bucket.get();
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
        return rs;
    }

    //--------------------- refresh ------------------------------
    @Override
    public CompletableFuture<Void> refreshAsync(String key, int expireSeconds) {
        final RBucket bucket = client.getBucket(key);
        return completableFuture(bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public void refresh(String key, final int expireSeconds) {
        final RBucket bucket = client.getBucket(key);
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    //--------------------- set ------------------------------
    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert0, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? this.convert : convert0).convertToBytes(value)));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(type == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : this.convert.convertToBytes(type, value)));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(type == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : (convert0 == null ? this.convert : convert0).convertToBytes(type, value)));
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAndSetAsync(type == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : this.convert.convertToBytes(type, value))
            .thenApply(old -> old == null ? null : convert.convertFrom(type, old)));
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        Convert c = convert0 == null ? this.convert : convert0;
        return completableFuture(bucket.getAndSetAsync(type == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : c.convertToBytes(type, value))
            .thenApply(old -> old == null ? null : (T) c.convertFrom(type, old)));
    }

    @Override
    public <T> void set(final String key, final Convert convert0, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(value instanceof String ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : (convert0 == null ? this.convert : convert0).convertToBytes(value));
    }

    @Override
    public <T> void set(final String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(type == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : this.convert.convertToBytes(type, value));
    }

    @Override
    public <T> void set(String key, final Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? this.convert : convert0).convertToBytes(type, value));
    }

    @Override
    public <T> T getSet(final String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        byte[] old = bucket.getAndSet(type == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : this.convert.convertToBytes(type, value));
        return old == null ? null : convert.convertFrom(type, old);
    }

    @Override
    public <T> T getSet(String key, final Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        Convert c = convert0 == null ? this.convert : convert0;
        byte[] old = bucket.getAndSet(c.convertToBytes(type, value));
        return old == null ? null : (T) c.convertFrom(type, old);
    }

    @Override
    public CompletableFuture<Void> setStringAsync(String key, String value) {
        return completableFuture(client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE).setAsync(value));
    }

    @Override
    public void setString(String key, String value) {
        client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE).set(value);
    }

    @Override
    public CompletableFuture<Void> setLongAsync(String key, long value) {
        return completableFuture(client.getAtomicLong(key).setAsync(value));
    }

    @Override
    public void setLong(String key, long value) {
        client.getAtomicLong(key).set(value);
    }

    //--------------------- set ------------------------------    
    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, Convert convert0, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? convert : convert0).convertToBytes(value), expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(convert.convertToBytes(type, value), expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? convert : convert0).convertToBytes(type, value), expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public <T> void set(int expireSeconds, String key, Convert convert0, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? convert : convert0).convertToBytes(value), expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public <T> void set(int expireSeconds, String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(convert.convertToBytes(type, value), expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public <T> void set(int expireSeconds, String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? convert : convert0).convertToBytes(type, value), expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Void> setStringAsync(int expireSeconds, String key, String value) {
        final RBucket<String> bucket = client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.setAsync(value, expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public void setString(int expireSeconds, String key, String value) {
        final RBucket<String> bucket = client.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        bucket.set(value, expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Void> setLongAsync(int expireSeconds, String key, long value) {
        final RAtomicLong bucket = client.getAtomicLong(key);
        return completableFuture(bucket.setAsync(value).thenCompose(v -> bucket.expireAsync(expireSeconds, TimeUnit.SECONDS)).thenApply(r -> null));
    }

    @Override
    public void setLong(int expireSeconds, String key, long value) {
        final RAtomicLong bucket = client.getAtomicLong(key);
        bucket.set(value);
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    //--------------------- setExpireSeconds ------------------------------    
    @Override
    public CompletableFuture<Void> setExpireSecondsAsync(String key, int expireSeconds) {
        return completableFuture(client.getBucket(key).expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public void setExpireSeconds(String key, int expireSeconds) {
        client.getBucket(key).expire(expireSeconds, TimeUnit.SECONDS);
    }

    //--------------------- remove ------------------------------    
    @Override
    public CompletableFuture<Integer> removeAsync(String key) {
        return completableFuture(client.getBucket(key).deleteAsync().thenApply(rs -> rs ? 1 : 0));
    }

    @Override
    public int remove(String key) {
        return client.getBucket(key).delete() ? 1 : 0;
    }

    //--------------------- incr ------------------------------    
    @Override
    public long incr(final String key) {
        return client.getAtomicLong(key).incrementAndGet();
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return completableFuture(client.getAtomicLong(key).incrementAndGetAsync());
    }

    @Override
    public long incr(final String key, long num) {
        return client.getAtomicLong(key).addAndGet(num);
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key, long num) {
        return completableFuture(client.getAtomicLong(key).addAndGetAsync(num));
    }

    //--------------------- decr ------------------------------    
    @Override
    public long decr(final String key) {
        return client.getAtomicLong(key).decrementAndGet();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return completableFuture(client.getAtomicLong(key).decrementAndGetAsync());
    }

    @Override
    public long decr(final String key, long num) {
        return client.getAtomicLong(key).addAndGet(-num);
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key, long num) {
        return completableFuture(client.getAtomicLong(key).addAndGetAsync(-num));
    }

    @Override
    public int hremove(final String key, String... fields) {
        RMap<String, ?> map = client.getMap(key, MapByteArrayCodec.instance);
        return (int) map.fastRemove(fields);
    }

    @Override
    public int hsize(final String key) {
        return client.getMap(key, MapByteArrayCodec.instance).size();
    }

    @Override
    public List<String> hkeys(final String key) {
        return (List) new ArrayList<>(client.getMap(key, MapStringCodec.instance).keySet());
    }

    @Override
    public long hincr(final String key, String field) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, 1L);
    }

    @Override
    public long hincr(final String key, String field, long num) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, num);
    }

    @Override
    public long hdecr(final String key, String field) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, -1L);
    }

    @Override
    public long hdecr(final String key, String field, long num) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, -num);
    }

    @Override
    public boolean hexists(final String key, String field) {
        return client.getMap(key, MapByteArrayCodec.instance).containsKey(field);
    }

    @Override
    public <T> void hset(final String key, final String field, final Convert convert0, final T value) {
        if (value == null) return;
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        map.fastPut(field, (convert0 == null ? convert : convert0).convertToBytes(objValueType, value));
    }

    @Override
    public <T> void hset(final String key, final String field, final Type type, final T value) {
        if (value == null) return;
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        map.fastPut(field, this.convert.convertToBytes(type, value));
    }

    @Override
    public <T> void hset(final String key, final String field, final Convert convert0, final Type type, final T value) {
        if (value == null) return;
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        map.fastPut(field, (convert0 == null ? convert : convert0).convertToBytes(type, value));
    }

    @Override
    public void hsetString(final String key, final String field, final String value) {
        if (value == null) return;
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        map.fastPut(field, value);
    }

    @Override
    public void hsetLong(final String key, final String field, final long value) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        map.fastPut(field, value);
    }

    @Override
    public void hmset(final String key, final Serializable... values) {
        Map<String, byte[]> vals = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            vals.put(String.valueOf(values[i]), values[i + 1] instanceof String ? values[i + 1].toString().getBytes(StandardCharsets.UTF_8) : this.convert.convertToBytes(values[i + 1]));
        }
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        map.putAll(vals);
    }

    @Override
    public List<Serializable> hmget(final String key, final Type type, final String... fields) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        Map<String, byte[]> rs = map.getAll(Utility.ofSet(fields));
        List<Serializable> list = new ArrayList<>(fields.length);
        for (String field : fields) {
            byte[] bs = rs.get(field);
            if (bs == null) {
                list.add(null);
            } else {
                list.add(convert.convertFrom(type, bs));
            }
        }
        return list;
    }

    @Override
    public <T> Map<String, T> hmap(final String key, final Type type, int offset, int limit, String pattern) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        Iterator<String> it = map.keySet(pattern, offset + limit).iterator();
        final Map<String, T> rs = new LinkedHashMap<>();
        int index = -1;
        while (it.hasNext()) {
            if (++index < offset) continue;
            if (index >= offset + limit) break;
            String field = it.next();
            byte[] bs = map.get(field);
            if (bs != null) rs.put(field, convert.convertFrom(type, bs));
        }
        return rs;
    }

    @Override
    public <T> Map<String, T> hmap(final String key, final Type type, int offset, int limit) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        Iterator<String> it = map.keySet(offset + limit).iterator();
        final Map<String, T> rs = new LinkedHashMap<>();
        int index = -1;
        while (it.hasNext()) {
            if (++index < offset) continue;
            if (index >= offset + limit) break;
            String field = it.next();
            byte[] bs = map.get(field);
            if (bs != null) rs.put(field, convert.convertFrom(type, bs));
        }
        return rs;
    }

    @Override
    public <T> T hget(final String key, final String field, final Type type) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        byte[] bs = map.get(field);
        return bs == null ? null : convert.convertFrom(type, bs);
    }

    @Override
    public String hgetString(final String key, final String field) {
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        return map.get(field);
    }

    @Override
    public long hgetLong(final String key, final String field, long defValue) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        Long rs = map.get(field);
        return rs == null ? defValue : rs;
    }

    @Override
    public CompletableFuture<Integer> hremoveAsync(final String key, String... fields) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastRemoveAsync(fields).thenApply(r -> r.intValue()));
    }

    @Override
    public CompletableFuture<Integer> hsizeAsync(final String key) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.sizeAsync());
    }

    @Override
    public CompletableFuture<List<String>> hkeysAsync(final String key) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.readAllKeySetAsync().thenApply(set -> set == null ? null : new ArrayList(set)));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, 1L));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field, long num) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, num));
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(final String key, String field) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, -1L));
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(final String key, String field, long num) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, -num));
    }

    @Override
    public CompletableFuture<Boolean> hexistsAsync(final String key, String field) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.containsKeyAsync(field));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert0, final T value) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutAsync(field, (convert0 == null ? convert : convert0).convertToBytes(objValueType, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Type type, final T value) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutAsync(field, convert.convertToBytes(type, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert0, final Type type, final T value) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutAsync(field, (convert0 == null ? convert : convert0).convertToBytes(type, value)).thenApply(r -> null));
    }

    @Override
    public CompletableFuture<Void> hsetStringAsync(final String key, final String field, final String value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        return completableFuture(map.fastPutAsync(field, value).thenApply(r -> null));
    }

    @Override
    public CompletableFuture<Void> hsetLongAsync(final String key, final String field, final long value) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.fastPutAsync(field, value).thenApply(r -> null));
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Serializable... values) {
        Map<String, byte[]> vals = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            vals.put(String.valueOf(values[i]), this.convert.convertToBytes(values[i + 1]));
        }
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.putAllAsync(vals));
    }

    @Override
    public CompletableFuture<List<Serializable>> hmgetAsync(final String key, final Type type, final String... fields) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.getAllAsync(Utility.ofSet(fields)).thenApply(rs -> {
            List<Serializable> list = new ArrayList<>(fields.length);
            for (String field : fields) {
                byte[] bs = rs.get(field);
                if (bs == null) {
                    list.add(null);
                } else {
                    list.add(convert.convertFrom(type, bs));
                }
            }
            return list;
        }));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(final String key, final Type type, int offset, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);

            Iterator<String> it = map.keySet(offset + limit).iterator();
            final Map<String, T> rs = new LinkedHashMap<>();
            int index = -1;
            while (it.hasNext()) {
                if (++index < offset) continue;
                if (index >= offset + limit) break;
                String field = it.next();
                byte[] bs = map.get(field);
                if (bs != null) rs.put(field, convert.convertFrom(type, bs));
            }
            return rs;
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(final String key, final Type type, int offset, int limit, String pattern) {
        return CompletableFuture.supplyAsync(() -> {
            RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);

            Iterator<String> it = map.keySet(pattern, offset + limit).iterator();
            final Map<String, T> rs = new LinkedHashMap<>();
            int index = -1;
            while (it.hasNext()) {
                if (++index < offset) continue;
                if (index >= offset + limit) break;
                String field = it.next();
                byte[] bs = map.get(field);
                if (bs != null) rs.put(field, convert.convertFrom(type, bs));
            }
            return rs;
        });
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(final String key, final String field, final Type type) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.getAsync(field).thenApply(r -> r == null ? null : convert.convertFrom(type, r)));
    }

    @Override
    public CompletableFuture<String> hgetStringAsync(final String key, final String field) {
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        return completableFuture(map.getAsync(field));
    }

    @Override
    public CompletableFuture<Long> hgetLongAsync(final String key, final String field, long defValue) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.getAsync(field).thenApply(r -> r == null ? defValue : r.longValue()));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Integer> getCollectionSizeAsync(String key) {
        return completableFuture(client.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return client.getList(key).sizeAsync();
            } else {
                return client.getSet(key).sizeAsync();
            }
        }));
    }

    @Override
    public int getCollectionSize(String key) {
        String type = client.getScript().eval(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE);
        if (String.valueOf(type).contains("list")) {
            return client.getList(key).size();
        } else {
            return client.getSet(key).size();
        }
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return completableFuture(client.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) client.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
                    if (list == null || list.isEmpty()) return list;
                    List<T> rs = new ArrayList<>();
                    for (Object item : list) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(componentType == String.class ? (T) new String(bs, StandardCharsets.UTF_8) : convert.convertFrom(componentType, bs));
                        }
                    }
                    return rs;
                });
            } else {
                return (CompletionStage) client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).readAllAsync().thenApply(set -> {
                    if (set == null || set.isEmpty()) return set;
                    Set<T> rs = new LinkedHashSet<>();
                    for (Object item : set) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(componentType == String.class ? (T) new String(bs, StandardCharsets.UTF_8) : convert.convertFrom(componentType, bs));
                        }
                    }
                    return rs;
                });
            }
        }));
//        return (CompletableFuture) send("TYPE", null, componentType, key, key.getBytes(StandardCharsets.UTF_8)).thenCompose(t -> {
//            if (t == null) return CompletableFuture.completedFuture(null);
//            if (new String((byte[]) t).contains("list")) { //list
//                return send("LRANGE", CacheEntryType.OBJECT, componentType, false, key, key.getBytes(StandardCharsets.UTF_8), new byte[]{'0'}, new byte[]{'-', '1'});
//            } else {
//                return send("SMEMBERS", CacheEntryType.OBJECT, componentType, true, key, key.getBytes(StandardCharsets.UTF_8));
//            }
//        });
    }

    @Override
    public CompletableFuture<Map<String, Long>> getLongMapAsync(String... keys) {
        return completableFuture(client.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).getAsync(keys));
    }

    @Override
    public CompletableFuture<Long[]> getLongArrayAsync(String... keys) {
        return completableFuture(client.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            Long[] rs = new Long[keys.length];
            for (int i = 0; i < rs.length; i++) {
                rs[i] = (Long) map.get(keys[i]);
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<String[]> getStringArrayAsync(String... keys) {
        return completableFuture(client.getBuckets(org.redisson.client.codec.StringCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            String[] rs = new String[keys.length];
            for (int i = 0; i < rs.length; i++) {
                rs[i] = (String) map.get(keys[i]);
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<Map<String, String>> getStringMapAsync(String... keys) {
        return completableFuture(client.getBuckets(org.redisson.client.codec.StringCodec.INSTANCE).getAsync(keys));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> getMapAsync(final Type componentType, String... keys) {
        return completableFuture(client.getBuckets(org.redisson.client.codec.ByteArrayCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            Map rs = new LinkedHashMap();
            map.forEach((k, v) -> rs.put(k, convert.convertFrom(componentType, (byte[]) v)));
            return rs;
        }));
    }

    @Override
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(final boolean set, final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Collection<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<T>> map = new LinkedHashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
                    if (list == null || list.isEmpty()) return list;
                    List<T> rs = new ArrayList<>();
                    for (Object item : list) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(convert.convertFrom(componentType, bs));
                        }
                    }
                    synchronized (map) {
                        map.put(key, rs);
                    }
                    return rs;
                }));
            }
        } else {
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
                    if (list == null || list.isEmpty()) return list;
                    List<T> rs = new ArrayList<>();
                    for (Object item : list) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(convert.convertFrom(componentType, bs));
                        }
                    }
                    synchronized (map) {
                        map.put(key, rs);
                    }
                    return rs;
                }));
            }
        }
        CompletableFuture.allOf(futures).whenComplete((w, e) -> {
            if (e != null) {
                rsFuture.completeExceptionally(e);
            } else {
                rsFuture.complete(map);
            }
        });
        return rsFuture;
    }

    @Override
    public <T> Collection<T> getCollection(String key, final Type componentType) {
        return (Collection) getCollectionAsync(key, componentType).join();
    }

    @Override
    public Map<String, Long> getLongMap(final String... keys) {
        return client.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).get(keys);
    }

    @Override
    public Long[] getLongArray(final String... keys) {
        Map<String, Long> map = client.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).get(keys);
        Long[] rs = new Long[keys.length];
        for (int i = 0; i < rs.length; i++) {
            rs[i] = map.get(keys[i]);
        }
        return rs;
    }

    @Override
    public Map<String, String> getStringMap(final String... keys) {
        return client.getBuckets(org.redisson.client.codec.StringCodec.INSTANCE).get(keys);
    }

    @Override
    public String[] getStringArray(final String... keys) {
        Map<String, String> map = client.getBuckets(org.redisson.client.codec.StringCodec.INSTANCE).get(keys);
        String[] rs = new String[keys.length];
        for (int i = 0; i < rs.length; i++) {
            rs[i] = map.get(keys[i]);
        }
        return rs;
    }

    @Override
    public <T> Map<String, T> getMap(final Type componentType, final String... keys) {
        Map<String, byte[]> map = client.getBuckets(org.redisson.client.codec.ByteArrayCodec.INSTANCE).get(keys);
        Map<String, T> rs = new LinkedHashMap(map.size());
        map.forEach((k, v) -> rs.put(k, convert.convertFrom(componentType, v)));
        return rs;
    }

    @Override
    public <T> Map<String, Collection<T>> getCollectionMap(final boolean set, final Type componentType, String... keys) {
        return (Map) getCollectionMapAsync(set, componentType, keys).join();
    }

    @Override
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return completableFuture(client.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) client.getList(key, org.redisson.client.codec.StringCodec.INSTANCE).readAllAsync();
            } else {
                return (CompletionStage) client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE).readAllAsync().thenApply(s -> s == null ? null : new ArrayList(s));
            }
        }));
    }

    @Override
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<String>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<String>> map = new LinkedHashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getList(key, org.redisson.client.codec.StringCodec.INSTANCE).readAllAsync().thenApply(r -> {
                    if (r != null) {
                        synchronized (map) {
                            map.put(key, (Collection) r);
                        }
                    }
                    return null;
                }));
            }
        } else {
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE).readAllAsync().thenApply(r -> {
                    if (r != null) {
                        synchronized (map) {
                            map.put(key, new ArrayList(r));
                        }
                    }
                    return null;
                }));
            }
        }
        CompletableFuture.allOf(futures).whenComplete((w, e) -> {
            if (e != null) {
                rsFuture.completeExceptionally(e);
            } else {
                rsFuture.complete(map);
            }
        });
        return rsFuture;
    }

    @Override
    public Collection<String> getStringCollection(String key) {
        return getStringCollectionAsync(key).join();
    }

    @Override
    public Map<String, Collection<String>> getStringCollectionMap(final boolean set, String... keys) {
        return getStringCollectionMapAsync(set, keys).join();
    }

    @Override
    public CompletableFuture<Collection<Long>> getLongCollectionAsync(String key) {
        return completableFuture(client.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync();
            } else {
                return (CompletionStage) client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync().thenApply(s -> s == null ? null : new ArrayList(s));
            }
        }));
    }

    @Override
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<Long>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<Long>> map = new LinkedHashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync().thenApply(r -> {
                    if (r != null) {
                        synchronized (map) {
                            map.put(key, (Collection) r);
                        }
                    }
                    return null;
                }));
            }
        } else {
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync().thenApply(r -> {
                    if (r != null) {
                        synchronized (map) {
                            map.put(key, new ArrayList(r));
                        }
                    }
                    return null;
                }));
            }
        }
        CompletableFuture.allOf(futures).whenComplete((w, e) -> {
            if (e != null) {
                rsFuture.completeExceptionally(e);
            } else {
                rsFuture.complete(map);
            }
        });
        return rsFuture;
    }

    @Override
    public Collection<Long> getLongCollection(String key) {
        return getLongCollectionAsync(key).join();
    }

    @Override
    public Map<String, Collection<Long>> getLongCollectionMap(final boolean set, String... keys) {
        return getLongCollectionMapAsync(set, keys).join();
    }

    //--------------------- getCollectionAndRefresh ------------------------------  
    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAndRefreshAsync(String key, int expireSeconds, final Type componentType) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key, componentType));
    }

    @Override
    public <T> Collection<T> getCollectionAndRefresh(String key, final int expireSeconds, final Type componentType) {
        return (Collection) getCollectionAndRefreshAsync(key, expireSeconds, componentType).join();
    }

    @Override
    public CompletableFuture<Collection<String>> getStringCollectionAndRefreshAsync(String key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getStringCollectionAsync(key));
    }

    @Override
    public Collection<String> getStringCollectionAndRefresh(String key, final int expireSeconds) {
        return getStringCollectionAndRefreshAsync(key, expireSeconds).join();
    }

    @Override
    public CompletableFuture<Collection<Long>> getLongCollectionAndRefreshAsync(String key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getLongCollectionAsync(key));
    }

    @Override
    public Collection<Long> getLongCollectionAndRefresh(String key, final int expireSeconds) {
        return getLongCollectionAndRefreshAsync(key, expireSeconds).join();
    }

    //--------------------- existsItem ------------------------------  
    @Override
    public <T> boolean existsSetItem(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.contains(componentType == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : convert.convertToBytes(componentType, value));
    }

    @Override
    public <T> CompletableFuture<Boolean> existsSetItemAsync(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(convert.convertToBytes(componentType, value)));
    }

    @Override
    public boolean existsStringSetItem(String key, String value) {
        final RSet<String> bucket = client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return bucket.contains(value);
    }

    @Override
    public CompletableFuture<Boolean> existsStringSetItemAsync(String key, String value) {
        final RSet<String> bucket = client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(value));
    }

    @Override
    public boolean existsLongSetItem(String key, long value) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return bucket.contains(value);
    }

    @Override
    public CompletableFuture<Boolean> existsLongSetItemAsync(String key, long value) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(value));
    }

    //--------------------- appendListItem ------------------------------  
    @Override
    public <T> CompletableFuture<Void> appendListItemAsync(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = client.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAsync(convert.convertToBytes(componentType, value)).thenApply(r -> null));
    }

    @Override
    public <T> void appendListItem(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = client.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.add(componentType == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : convert.convertToBytes(componentType, value));
    }

    @Override
    public CompletableFuture<Void> appendStringListItemAsync(String key, String value) {
        final RList<String> bucket = client.getList(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void appendStringListItem(String key, String value) {
        final RList<String> bucket = client.getList(key, org.redisson.client.codec.StringCodec.INSTANCE);
        bucket.add(value);
    }

    @Override
    public CompletableFuture<Void> appendLongListItemAsync(String key, long value) {
        final RList<Long> bucket = client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void appendLongListItem(String key, long value) {
        final RList<Long> bucket = client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE);
        bucket.add(value);
    }

    //--------------------- removeListItem ------------------------------  
    @Override
    public <T> CompletableFuture<Integer> removeListItemAsync(String key, final Type componentType, T value) {
        return completableFuture(client.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).removeAsync(convert.convertToBytes(componentType, value)).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public <T> int removeListItem(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = client.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.remove(componentType == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : convert.convertToBytes(componentType, value)) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> removeStringListItemAsync(String key, String value) {
        return completableFuture(client.getList(key, org.redisson.client.codec.StringCodec.INSTANCE).removeAsync(value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int removeStringListItem(String key, String value) {
        return client.getList(key, org.redisson.client.codec.StringCodec.INSTANCE).remove(value) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> removeLongListItemAsync(String key, long value) {
        return completableFuture(client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).removeAsync((Object) value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int removeLongListItem(String key, long value) {
        return client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).remove((Object) value) ? 1 : 0;
    }

    //--------------------- appendSetItem ------------------------------  
    @Override
    public <T> CompletableFuture<Void> appendSetItemAsync(String key, Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAsync(convert.convertToBytes(componentType, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<T> spopSetItemAsync(String key, Type componentType) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync().thenApply(bs -> bs == null ? null : convert.convertFrom(componentType, bs)));
    }

    @Override
    public <T> CompletableFuture<Set<T>> spopSetItemAsync(String key, int count, Type componentType) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count).thenApply((Set<byte[]> bslist) -> {
            if (bslist == null || bslist.isEmpty()) return new LinkedHashSet<T>();
            Set<T> rs = new LinkedHashSet<>();
            for (byte[] bs : bslist) {
                rs.add(convert.convertFrom(componentType, bs));
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<String> spopStringSetItemAsync(String key) {
        final RSet<String> bucket = client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync());
    }

    @Override
    public CompletableFuture<Set<String>> spopStringSetItemAsync(String key, int count) {
        final RSet<String> bucket = client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count).thenApply(r -> r == null ? null : new LinkedHashSet<>(r)));
    }

    @Override
    public CompletableFuture<Long> spopLongSetItemAsync(String key) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync());
    }

    @Override
    public CompletableFuture<Set<Long>> spopLongSetItemAsync(String key, int count) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count));
    }

    @Override
    public <T> void appendSetItem(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.add(componentType == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : convert.convertToBytes(componentType, value));
    }

    @Override
    public <T> T spopSetItem(String key, final Type componentType) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        byte[] bs = bucket.removeRandom();
        return bs == null ? null : convert.convertFrom(componentType, bs);
    }

    @Override
    public <T> Set<T> spopSetItem(String key, int count, final Type componentType) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        Set< byte[]> bslist = bucket.removeRandom(count);
        Set<T> rs = new LinkedHashSet<>();
        if (bslist == null) return rs;
        for (byte[] bs : bslist) {
            rs.add(convert.convertFrom(componentType, bs));
        }
        return rs;
    }

    @Override
    public String spopStringSetItem(String key) {
        final RSet<String> bucket = client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return bucket.removeRandom();
    }

    @Override
    public Set<String> spopStringSetItem(String key, int count) {
        final RSet<String> bucket = client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return bucket.removeRandom(count);
    }

    @Override
    public Long spopLongSetItem(String key) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return bucket.removeRandom();
    }

    @Override
    public Set<Long> spopLongSetItem(String key, int count) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return bucket.removeRandom(count);
    }

    @Override
    public CompletableFuture<Void> appendStringSetItemAsync(String key, String value) {
        final RSet<String> bucket = client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void appendStringSetItem(String key, String value) {
        final RSet<String> bucket = client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        bucket.add(value);
    }

    @Override
    public CompletableFuture<Void> appendLongSetItemAsync(String key, long value) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void appendLongSetItem(String key, long value) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        bucket.add(value);
    }

    //--------------------- removeSetItem ------------------------------  
    @Override
    public <T> CompletableFuture<Integer> removeSetItemAsync(String key, final Type componentType, T value) {
        return completableFuture(client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).removeAsync(convert.convertToBytes(componentType, value)).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public <T> int removeSetItem(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.remove(componentType == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : convert.convertToBytes(componentType, value)) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> removeStringSetItemAsync(String key, String value) {
        return completableFuture(client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE).removeAsync(value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int removeStringSetItem(String key, String value) {
        return client.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE).remove(value) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> removeLongSetItemAsync(String key, long value) {
        return completableFuture(client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).removeAsync(value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int removeLongSetItem(String key, long value) {
        return client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).remove(value) ? 1 : 0;
    }

    //--------------------- queryKeys ------------------------------  
    @Override
    public List<String> queryKeys() {
        return client.getKeys().getKeysStream().collect(Collectors.toList());
    }

    @Override
    public List<String> queryKeysStartsWith(String startsWith) {
        return client.getKeys().getKeysStreamByPattern(startsWith + "*").collect(Collectors.toList());
    }

    @Override
    public List<String> queryKeysEndsWith(String endsWith) {
        return client.getKeys().getKeysStreamByPattern("*" + endsWith).collect(Collectors.toList());
    }

    @Override
    public byte[] getBytes(final String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.get();
    }

    @Override
    public byte[] getSetBytes(final String key, byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.getAndSet(value);
    }

    @Override
    public byte[] getBytesAndRefresh(final String key, final int expireSeconds) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        byte[] bs = bucket.get();
        if (bs == null) return bs;
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
        return bs;
    }

    @Override
    public void setBytes(final String key, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(value);
    }

    @Override
    public void setBytes(final int expireSeconds, final String key, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(value, expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public <T> void setBytes(final String key, final Convert convert0, final Type type, final T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? convert : convert0).convertToBytes(type, value));
    }

    @Override
    public <T> void setBytes(final int expireSeconds, final String key, final Convert convert0, final Type type, final T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? convert : convert0).convertToBytes(type, value), expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<byte[]> getBytesAsync(final String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync());
    }

    @Override
    public CompletableFuture<byte[]> getSetBytesAsync(final String key, byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAndSetAsync(value));
    }

    @Override
    public CompletableFuture<byte[]> getBytesAndRefreshAsync(final String key, final int expireSeconds) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenCompose(bs -> bs == null ? CompletableFuture.completedFuture(null) : bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> bs)));
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(final String key, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(value));
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(final int expireSeconds, final String key, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(value, expireSeconds, TimeUnit.SECONDS).thenApply(v -> null));
    }

    @Override
    public <T> CompletableFuture<Void> setBytesAsync(final String key, final Convert convert0, final Type type, final T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? convert : convert0).convertToBytes(type, value)).thenApply(v -> null));
    }

    @Override
    public <T> CompletableFuture<Void> setBytesAsync(final int expireSeconds, final String key, final Convert convert0, final Type type, final T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? convert : convert0).convertToBytes(type, value), expireSeconds, TimeUnit.SECONDS).thenCompose(v -> null));
    }

    @Override
    public CompletableFuture<List<String>> queryKeysAsync() {
        return CompletableFuture.supplyAsync(() -> queryKeys());
    }

    @Override
    public CompletableFuture<List<String>> queryKeysStartsWithAsync(String startsWith) {
        if (startsWith == null) return CompletableFuture.supplyAsync(() -> queryKeys());
        return CompletableFuture.supplyAsync(() -> queryKeysStartsWith(startsWith));
    }

    @Override
    public CompletableFuture<List<String>> queryKeysEndsWithAsync(String endsWith) {
        if (endsWith == null) return CompletableFuture.supplyAsync(() -> queryKeys());
        return CompletableFuture.supplyAsync(() -> queryKeysEndsWith(endsWith));
    }

    //--------------------- getKeySize ------------------------------  
    @Override
    public int getKeySize() {
        return (int) client.getKeys().count();
    }

    @Override
    public CompletableFuture<Integer> getKeySizeAsync() {
        return completableFuture(client.getKeys().countAsync().thenApply(r -> r.intValue()));
    }

    protected static class MapByteArrayCodec extends org.redisson.client.codec.ByteArrayCodec {

        public static final MapByteArrayCodec instance = new MapByteArrayCodec();

        @Override
        public org.redisson.client.protocol.Decoder<Object> getMapKeyDecoder() {
            return org.redisson.client.codec.StringCodec.INSTANCE.getValueDecoder();
        }

        @Override
        public org.redisson.client.protocol.Encoder getMapKeyEncoder() {
            return org.redisson.client.codec.StringCodec.INSTANCE.getValueEncoder();
        }
    }

    protected static class MapStringCodec extends org.redisson.client.codec.StringCodec {

        public static final MapStringCodec instance = new MapStringCodec();

        @Override
        public org.redisson.client.protocol.Decoder<Object> getMapKeyDecoder() {
            return org.redisson.client.codec.StringCodec.INSTANCE.getValueDecoder();
        }

        @Override
        public org.redisson.client.protocol.Encoder getMapKeyEncoder() {
            return org.redisson.client.codec.StringCodec.INSTANCE.getValueEncoder();
        }
    }

    protected static class MapLongCodec extends org.redisson.client.codec.LongCodec {

        public static final MapLongCodec instance = new MapLongCodec();

        @Override
        public org.redisson.client.protocol.Decoder<Object> getMapKeyDecoder() {
            return org.redisson.client.codec.StringCodec.INSTANCE.getValueDecoder();
        }

        @Override
        public org.redisson.client.protocol.Encoder getMapKeyEncoder() {
            return org.redisson.client.codec.StringCodec.INSTANCE.getValueEncoder();
        }
    }
}
