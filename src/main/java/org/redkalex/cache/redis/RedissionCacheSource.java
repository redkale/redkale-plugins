/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;
import java.util.stream.Collectors;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.*;
import org.redisson.config.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceListener;
import org.redkale.annotation.ResourceType;
import org.redkale.convert.Convert;
import org.redkale.service.Local;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_MAXCONNS;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_NODE;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_URL;
import org.redkale.source.CacheSource;
import org.redkale.util.*;

/**
 * //https://www.cnblogs.com/xiami2046/p/13934146.html
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@ResourceType(CacheSource.class)
public class RedissionCacheSource extends AbstractRedisSource {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected List<String> nodeAddrs;

    protected RedissonClient client;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (conf == null) {
            conf = AnyValue.create();
        }
        initClient(conf);
    }

    private void initClient(AnyValue conf) {
        final List<String> addresses = new ArrayList<>();
        Config redisConfig = new Config();
        AnyValue[] nodes = getNodes(conf);
        String cluster = conf.getOrDefault("cluster", "");
        int maxconns = conf.getIntValue(CACHE_SOURCE_MAXCONNS, Utility.cpus());
        BaseConfig baseConfig = null;
        for (AnyValue node : nodes) {
            String addr = node.getValue(CACHE_SOURCE_URL, node.getValue("addr"));  //兼容addr
            String db0 = node.getValue(CACHE_SOURCE_DB, "").trim();
            if (!db0.isEmpty()) {
                this.db = Integer.valueOf(db0);
            }
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
                baseConfig = redisConfig.useSingleServer();
                if (maxconns > 0) {
                    redisConfig.useSingleServer().setConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useSingleServer().setConnectionPoolSize(maxconns);
                }
                redisConfig.useSingleServer().setAddress(addr);
                redisConfig.useSingleServer().setDatabase(this.db);
            } else if ("masterslave".equalsIgnoreCase(cluster)) { //主从
                baseConfig = redisConfig.useMasterSlaveServers();
                if (maxconns > 0) {
                    redisConfig.useMasterSlaveServers().setMasterConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useMasterSlaveServers().setMasterConnectionPoolSize(maxconns);
                    redisConfig.useMasterSlaveServers().setSlaveConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useMasterSlaveServers().setSlaveConnectionPoolSize(maxconns);
                }
                if (node.get("master") != null) {
                    redisConfig.useMasterSlaveServers().setMasterAddress(addr);
                } else {
                    redisConfig.useMasterSlaveServers().addSlaveAddress(addr);
                }
                redisConfig.useMasterSlaveServers().setDatabase(this.db);
            } else if ("cluster".equalsIgnoreCase(cluster)) { //集群
                baseConfig = redisConfig.useClusterServers();
                if (maxconns > 0) {
                    redisConfig.useClusterServers().setMasterConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useClusterServers().setMasterConnectionPoolSize(maxconns);
                    redisConfig.useClusterServers().setSlaveConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useClusterServers().setSlaveConnectionPoolSize(maxconns);
                }
                redisConfig.useClusterServers().addNodeAddress(addr);
            } else if ("replicated".equalsIgnoreCase(cluster)) { //
                baseConfig = redisConfig.useReplicatedServers();
                if (maxconns > 0) {
                    redisConfig.useReplicatedServers().setMasterConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useReplicatedServers().setMasterConnectionPoolSize(maxconns);
                    redisConfig.useReplicatedServers().setSlaveConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useReplicatedServers().setSlaveConnectionPoolSize(maxconns);
                }
                redisConfig.useReplicatedServers().addNodeAddress(addr);
                redisConfig.useReplicatedServers().setDatabase(this.db);
            } else if ("sentinel".equalsIgnoreCase(cluster)) { //
                baseConfig = redisConfig.useSentinelServers();
                if (maxconns > 0) {
                    redisConfig.useSentinelServers().setMasterConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useSentinelServers().setMasterConnectionPoolSize(maxconns);
                    redisConfig.useSentinelServers().setSlaveConnectionMinimumIdleSize(maxconns / 2 + 1);
                    redisConfig.useSentinelServers().setSlaveConnectionPoolSize(maxconns);
                }
                redisConfig.useSentinelServers().addSentinelAddress(addr);
                redisConfig.useSentinelServers().setDatabase(this.db);
            }
            if (baseConfig != null) {  //单个进程的不同自定义密码
                if (!username.isEmpty()) {
                    baseConfig.setUsername(username);
                }
                if (!password.isEmpty()) {
                    baseConfig.setPassword(password);
                }
            }
        }
        if (baseConfig != null) { //配置全局密码
            String username = conf.getValue(CACHE_SOURCE_USER, "").trim();
            String password = conf.getValue(CACHE_SOURCE_PASSWORD, "").trim();
            String retryAttempts = conf.getValue("retryAttempts", "").trim();
            String retryInterval = conf.getValue("retryInterval", "").trim();
            if (!username.isEmpty()) {
                baseConfig.setUsername(username);
            }
            if (!password.isEmpty()) {
                baseConfig.setPassword(password);
            }
            if (!retryAttempts.isEmpty()) {
                baseConfig.setRetryAttempts(Integer.parseInt(retryAttempts));
            }
            if (!retryInterval.isEmpty()) {
                baseConfig.setRetryInterval(Integer.parseInt(retryInterval));
            }
        }
        RedissonClient old = this.client;
        this.client = Redisson.create(redisConfig);
        this.nodeAddrs = addresses;
        if (old != null) {
            old.shutdown();
        }
//        RTopic topic = client.getTopic("__keyevent@" + db + "__:expired", new StringCodec());
//        topic.addListener(String.class, (CharSequence cs, String key) -> {
//            if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedissionCacheSource.class.getSimpleName() + "." + db + ": expired key=" + key + ", cs=" + cs);
//        });
        //if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedissionCacheSource.class.getSimpleName() + ": addrs=" + addresses + ", db=" + db);

    }

    @Override
    @ResourceListener
    public void onResourceChange(ResourceEvent[] events) {
        if (events == null || events.length < 1) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            sb.append("CacheSource(name=").append(resourceName()).append(") change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
        }
        initClient(this.config);
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
    }

    public boolean acceptsConf(AnyValue config) {
        if (config == null) {
            return false;
        }
        AnyValue[] nodes = getNodes(config);
        if (nodes == null || nodes.length == 0) {
            return false;
        }
        for (AnyValue node : nodes) {
            String val = node.getValue(CACHE_SOURCE_URL, node.getValue("addr"));  //兼容addr
            if (val != null && val.startsWith("redis://")) {
                return true;
            }
            if (val != null && val.startsWith("rediss://")) {
                return true;
            }
        }
        return false;
    }

    protected AnyValue[] getNodes(AnyValue config) {
        AnyValue[] nodes = config.getAnyValues(CACHE_SOURCE_NODE);
        if (nodes == null || nodes.length == 0) {
            AnyValue one = config.getAnyValue(CACHE_SOURCE_NODE);
            if (one == null) {
                String val = config.getValue(CACHE_SOURCE_URL);
                if (val == null) {
                    return nodes;
                }
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

    protected CompletableFuture<String> completableFuture(String key, RedisCryptor cryptor, CompletionStage<String> rf) {
        return cryptor != null ? rf.toCompletableFuture().thenApply(v -> cryptor.decrypt(key, v)) : rf.toCompletableFuture();
    }

    protected <T> CompletableFuture<T> completableFuture(String key, RedisCryptor cryptor, Type type, CompletionStage<byte[]> rf) {
        return rf.toCompletableFuture().thenApply(bs -> decryptValue(key, cryptor, type, bs));
    }

    protected <T> CompletableFuture<T> completableFuture(String key, RedisCryptor cryptor, Convert c, Type type, CompletionStage<byte[]> rf) {
        return rf.toCompletableFuture().thenApply(bs -> decryptValue(key, cryptor, c, type, bs));
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (client != null) {
            client.shutdown();
        }
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.isExistsAsync());
    }

    @Override
    public boolean exists(String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.isExists();
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(key, cryptor, type, bucket.getAsync());
    }

    @Override
    public CompletableFuture<String> getStringAsync(String key) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(key, cryptor, bucket.getAsync());
    }

    @Override
    public CompletableFuture<String> getSetStringAsync(String key, String value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(key, cryptor, bucket.getAndSetAsync(encryptValue(key, cryptor, value)));
    }

    @Override
    public CompletableFuture<Long> getLongAsync(String key, long defValue) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenApply(v -> v == null ? defValue : Long.parseLong(v)));
    }

    @Override
    public CompletableFuture<Long> getSetLongAsync(String key, long value, long defValue) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.getAndSetAsync(String.valueOf(value)).thenApply(v -> v == null ? defValue : Long.parseLong(v)));
    }

    @Override
    public <T> T get(String key, final Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return decryptValue(key, cryptor, type, bucket.get());
    }

    @Override
    public String getString(String key) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return decryptValue(key, cryptor, bucket.get());
    }

    @Override
    public String getSetString(String key, String value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return decryptValue(key, cryptor, bucket.getAndSet(encryptValue(key, cryptor, value)));
    }

    @Override
    public long getLong(String key, long defValue) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        String v = bucket.get();
        return v == null ? defValue : Long.parseLong(v);
    }

    @Override
    public long getSetLong(String key, long value, long defValue) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        String v = bucket.getAndSet(String.valueOf(value));
        return v == null ? defValue : Long.parseLong(v);
    }

    //--------------------- getex ------------------------------
    @Override
    public <T> CompletableFuture<T> getexAsync(String key, int expireSeconds, final Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAndExpireAsync(Duration.ofSeconds(expireSeconds)).thenApply(bs -> decryptValue(key, cryptor, type, bs)));
    }

    @Override
    public <T> T getex(String key, final int expireSeconds, final Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return decryptValue(key, cryptor, type, bucket.getAndExpire(Duration.ofSeconds(expireSeconds)));
    }

    @Override
    public CompletableFuture<String> getexStringAsync(String key, int expireSeconds) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.getAndExpireAsync(Duration.ofSeconds(expireSeconds)).thenApply(v -> decryptValue(key, cryptor, v)));
    }

    @Override
    public String getexString(String key, final int expireSeconds) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        String rs = bucket.getAndExpire(Duration.ofSeconds(expireSeconds));
        return rs == null ? rs : decryptValue(key, cryptor, rs);
    }

    @Override
    public CompletableFuture<Long> getexLongAsync(String key, int expireSeconds, long defValue) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.getAndExpireAsync(Duration.ofSeconds(expireSeconds)).thenApply(v -> v == null ? defValue : Long.parseLong(v)));
    }

    @Override
    public long getexLong(String key, final int expireSeconds, long defValue) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        String v = bucket.getAndExpire(Duration.ofSeconds(expireSeconds));
        return v == null ? defValue : Long.parseLong(v);
    }

    //--------------------- setex ------------------------------
    @Override
    public <T> CompletableFuture<Void> setAsync(String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert, value)));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert0, value)));
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxAsync(String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert, value)));
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert0, value)));
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAndSetAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert, value))
            .thenApply(old -> old == null ? null : convert.convertFrom(type, old)));
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        Convert c = convert0 == null ? this.convert : convert0;
        return completableFuture(bucket.getAndSetAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, c, value))
            .thenApply(old -> old == null ? null : (T) c.convertFrom(type, old)));
    }

    @Override
    public boolean setnxBytes(final String key, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.setIfAbsent(value);
    }

    @Override
    public CompletableFuture<Boolean> setnxBytesAsync(final String key, byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(value));
    }

    @Override
    public CompletableFuture<Void> msetAsync(Object... keyVals) {
        Map<String, byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < keyVals.length; i += 2) {
            String key = keyVals[i].toString();
            Object val = keyVals[i + 1];
            map.put(key, val instanceof String ? encryptValue(key, cryptor, val.toString()).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, this.convert, val));
        }
        final RBuckets bucket = client.getBuckets(ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(map).thenApply(v -> null));
    }

    @Override
    public CompletableFuture<Void> msetAsync(Map map) {
        Map<String, byte[]> bs = new LinkedHashMap<>();
        map.forEach((key, val) -> {
            bs.put(key.toString(), val instanceof String ? encryptValue(key.toString(), cryptor, val.toString()).getBytes(StandardCharsets.UTF_8) : encryptValue(key.toString(), cryptor, this.convert, val));
        });
        final RBuckets bucket = client.getBuckets(ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(bs).thenApply(v -> null));
    }

    @Override
    public void mset(Object... keyVals) {
        Map<String, byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < keyVals.length; i += 2) {
            String key = keyVals[i].toString();
            Object val = keyVals[i + 1];
            map.put(key, val instanceof String ? encryptValue(key, cryptor, val.toString()).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, this.convert, val));
        }
        client.getBuckets(ByteArrayCodec.INSTANCE).set(map);
    }

    @Override
    public void mset(Map map) {
        Map<String, byte[]> bs = new LinkedHashMap<>();
        map.forEach((key, val) -> {
            bs.put(key.toString(), val instanceof String ? encryptValue(key.toString(), cryptor, val.toString()).getBytes(StandardCharsets.UTF_8) : encryptValue(key.toString(), cryptor, this.convert, val));
        });
        client.getBuckets(ByteArrayCodec.INSTANCE).set(bs);
    }

    @Override
    public <T> void set(final String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        bucket.set(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert, value));
    }

    @Override
    public <T> void set(String key, final Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        bucket.set(encryptValue(key, cryptor, type, convert0, value));
    }

    @Override
    public <T> boolean setnx(final String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.setIfAbsent(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert, value));
    }

    @Override
    public <T> boolean setnx(String key, final Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.setIfAbsent(encryptValue(key, cryptor, type, convert0, value));
    }

    @Override
    public <T> T getSet(final String key, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        byte[] old = bucket.getAndSet(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert, value));
        return old == null ? null : convert.convertFrom(type, old);
    }

    @Override
    public <T> T getSet(String key, final Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        Convert c = convert0 == null ? this.convert : convert0;
        byte[] old = bucket.getAndSet(encryptValue(key, cryptor, type, c, value));
        return decryptValue(key, cryptor, c, type, old);
    }

    @Override
    public CompletableFuture<Void> setStringAsync(String key, String value) {
        return completableFuture(client.getBucket(key, StringCodec.INSTANCE).setAsync(value));
    }

    @Override
    public CompletableFuture<Boolean> setnxStringAsync(String key, String value) {
        return completableFuture(client.getBucket(key, StringCodec.INSTANCE).setIfAbsentAsync(value));
    }

    @Override
    public void setString(String key, String value) {
        client.getBucket(key, StringCodec.INSTANCE).set(value);
    }

    @Override
    public boolean setnxString(String key, String value) {
        return client.getBucket(key, StringCodec.INSTANCE).setIfAbsent(value);
    }

    @Override
    public CompletableFuture<Void> setLongAsync(String key, long value) {
        return completableFuture(client.getAtomicLong(key).setAsync(value));
    }

    @Override
    public CompletableFuture<Boolean> setnxLongAsync(String key, long value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(String.valueOf(value)));
    }

    @Override
    public void setLong(String key, long value) {
        client.getAtomicLong(key).set(value);
    }

    @Override
    public boolean setnxLong(String key, long value) {
        return client.getBucket(key, StringCodec.INSTANCE).setIfAbsent(String.valueOf(value));
    }

    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(encryptValue(key, cryptor, type, convert, value), expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(encryptValue(key, cryptor, type, convert0, value), expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public <T> void setex(String key, int expireSeconds, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        bucket.set(encryptValue(key, cryptor, type, convert, value), expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public <T> void setex(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        bucket.set(encryptValue(key, cryptor, type, convert0, value), expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Void> setexStringAsync(String key, int expireSeconds, String value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.setAsync(encryptValue(key, cryptor, value), expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public void setexString(String key, int expireSeconds, String value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        bucket.set(encryptValue(key, cryptor, value), expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Void> setexLongAsync(String key, int expireSeconds, long value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.setAsync(String.valueOf(value), expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public void setexLong(String key, int expireSeconds, long value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        bucket.set(String.valueOf(value), expireSeconds, TimeUnit.SECONDS);
    }

    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Boolean> setnxexAsync(String key, int expireSeconds, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(encryptValue(key, cryptor, type, convert, value), Duration.ofSeconds(expireSeconds)));
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxexAsync(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(encryptValue(key, cryptor, type, convert0, value), Duration.ofSeconds(expireSeconds)));
    }

    @Override
    public <T> boolean setnxex(String key, int expireSeconds, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.setIfAbsent(encryptValue(key, cryptor, type, convert, value), Duration.ofSeconds(expireSeconds));
    }

    @Override
    public <T> boolean setnxex(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.setIfAbsent(encryptValue(key, cryptor, type, convert0, value), Duration.ofSeconds(expireSeconds));
    }

    @Override
    public CompletableFuture<Boolean> setnxexStringAsync(String key, int expireSeconds, String value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(encryptValue(key, cryptor, value), Duration.ofSeconds(expireSeconds)));
    }

    @Override
    public boolean setnxexString(String key, int expireSeconds, String value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return bucket.setIfAbsent(encryptValue(key, cryptor, value), Duration.ofSeconds(expireSeconds));
    }

    @Override
    public CompletableFuture<Boolean> setnxexLongAsync(String key, int expireSeconds, long value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(String.valueOf(value), Duration.ofSeconds(expireSeconds)));
    }

    @Override
    public boolean setnxexLong(String key, int expireSeconds, long value) {
        final RBucket<String> bucket = client.getBucket(key, StringCodec.INSTANCE);
        return bucket.setIfAbsent(String.valueOf(value), Duration.ofSeconds(expireSeconds));
    }

    //--------------------- expire ------------------------------    
    @Override
    public CompletableFuture<Void> expireAsync(String key, int expireSeconds) {
        return completableFuture(client.getBucket(key).expireAsync(Duration.ofSeconds(expireSeconds)).thenApply(r -> null));
    }

    @Override
    public void expire(String key, int expireSeconds) {
        client.getBucket(key).expire(Duration.ofSeconds(expireSeconds));
    }

    //--------------------- persist ------------------------------    
    @Override
    public CompletableFuture<Boolean> persistAsync(String key) {
        return completableFuture(client.getBucket(key).clearExpireAsync());
    }

    @Override
    public boolean persist(String key) {
        return client.getBucket(key).clearExpire();
    }

    //--------------------- rename ------------------------------    
    @Override
    public CompletableFuture<Boolean> renameAsync(String oldKey, String newKey) {
        return completableFuture(client.getBucket(oldKey).renameAsync(newKey).handle((v, t) -> t == null));
    }

    @Override
    public boolean rename(String oldKey, String newKey) {
        try {
            client.getBucket(oldKey).rename(newKey);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public CompletableFuture<Boolean> renamenxAsync(String oldKey, String newKey) {
        return completableFuture(client.getBucket(oldKey).renamenxAsync(newKey));
    }

    @Override
    public boolean renamenx(String oldKey, String newKey) {
        try {
            return client.getBucket(oldKey).renamenx(newKey);
        } catch (Exception e) {
            return false;
        }
    }

    //--------------------- del ------------------------------    
    @Override
    public CompletableFuture<Integer> delAsync(String... keys) {
        return completableFuture(client.getKeys().deleteAsync(keys).thenApply(rs -> rs.intValue()));
    }

    @Override
    public int del(String... keys) {
        return (int) client.getKeys().delete(keys);
    }

    //--------------------- incrby ------------------------------    
    @Override
    public long incr(final String key) {
        return client.getAtomicLong(key).incrementAndGet();
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return completableFuture(client.getAtomicLong(key).incrementAndGetAsync());
    }

    @Override
    public long incrby(final String key, long num) {
        return client.getAtomicLong(key).addAndGet(num);
    }

    @Override
    public double incrbyFloat(final String key, double num) {
        return client.getAtomicDouble(key).addAndGet(num);
    }

    @Override
    public CompletableFuture<Long> incrbyAsync(final String key, long num) {
        return completableFuture(client.getAtomicLong(key).addAndGetAsync(num));
    }

    @Override
    public CompletableFuture<Double> incrbyFloatAsync(final String key, double num) {
        return completableFuture(client.getAtomicDouble(key).addAndGetAsync(num));
    }

    //--------------------- decrby ------------------------------    
    @Override
    public long decr(final String key) {
        return client.getAtomicLong(key).decrementAndGet();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return completableFuture(client.getAtomicLong(key).decrementAndGetAsync());
    }

    @Override
    public long decrby(final String key, long num) {
        return client.getAtomicLong(key).addAndGet(-num);
    }

    @Override
    public CompletableFuture<Long> decrbyAsync(final String key, long num) {
        return completableFuture(client.getAtomicLong(key).addAndGetAsync(-num));
    }

    @Override
    public int hdel(final String key, String... fields) {
        RMap<String, ?> map = client.getMap(key, MapByteArrayCodec.instance);
        return (int) map.fastRemove(fields);
    }

    @Override
    public int hlen(final String key) {
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
    public long hincrby(final String key, String field, long num) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, num);
    }

    @Override
    public double hincrbyFloat(final String key, String field, double num) {
        RMap<String, Double> map = client.getMap(key, MapDoubleCodec.instance);
        return map.addAndGet(field, num);
    }

    @Override
    public long hdecr(final String key, String field) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, -1L);
    }

    @Override
    public long hdecrby(final String key, String field, long num) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, -num);
    }

    @Override
    public boolean hexists(final String key, String field) {
        return client.getMap(key, MapByteArrayCodec.instance).containsKey(field);
    }

    @Override
    public <T> void hset(final String key, final String field, final Type type, final T value) {
        if (value == null) {
            return;
        }
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        map.fastPut(field, encryptValue(key, cryptor, type, convert, value));
    }

    @Override
    public <T> void hset(final String key, final String field, final Convert convert0, final Type type, final T value) {
        if (value == null) {
            return;
        }
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        map.fastPut(field, encryptValue(key, cryptor, type, convert0, value));
    }

    @Override
    public <T> boolean hsetnx(final String key, final String field, final Type type, final T value) {
        if (value == null) {
            return false;
        }
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return map.fastPutIfAbsent(field, encryptValue(key, cryptor, type, convert, value));
    }

    @Override
    public <T> boolean hsetnx(final String key, final String field, final Convert convert0, final Type type, final T value) {
        if (value == null) {
            return false;
        }
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return map.fastPutIfAbsent(field, encryptValue(key, cryptor, type, convert0, value));
    }

    @Override
    public void hsetString(final String key, final String field, final String value) {
        if (value == null) {
            return;
        }
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        map.fastPut(field, encryptValue(key, cryptor, value));
    }

    @Override
    public boolean hsetnxString(final String key, final String field, final String value) {
        if (value == null) {
            return false;
        }
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        return map.fastPutIfAbsent(field, encryptValue(key, cryptor, value));
    }

    @Override
    public void hsetLong(final String key, final String field, final long value) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        map.fastPut(field, value);
    }

    @Override
    public boolean hsetnxLong(final String key, final String field, final long value) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return map.fastPutIfAbsent(field, value);
    }

    @Override
    public void hmset(final String key, final Serializable... values) {
        Map<String, byte[]> vals = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            vals.put(String.valueOf(values[i]), values[i + 1] instanceof String ? encryptValue(key, cryptor, values[i + 1].toString()).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, this.convert, values[i + 1]));
        }
        RMap<String, byte[]> rm = client.getMap(key, MapByteArrayCodec.instance);
        rm.putAll(vals);
    }

    @Override
    public void hmset(final String key, final Map map) {
        Map<String, byte[]> vals = new LinkedHashMap<>();
        map.forEach((k, v) -> {
            vals.put(k.toString(), v instanceof String ? encryptValue(key, cryptor, v.toString()).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, this.convert, v));
        });
        RMap<String, byte[]> rm = client.getMap(key, MapByteArrayCodec.instance);
        rm.putAll(vals);
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
                list.add(decryptValue(key, cryptor, type, bs));
            }
        }
        return list;
    }

    @Override
    public <T> Map<String, T> hscan(final String key, final Type type, int offset, int limit, String pattern) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        Iterator<String> it = map.keySet(pattern, offset + limit).iterator();
        final Map<String, T> rs = new LinkedHashMap<>();
        int index = -1;
        while (it.hasNext()) {
            if (++index < offset) {
                continue;
            }
            if (index >= offset + limit) {
                break;
            }
            String field = it.next();
            byte[] bs = map.get(field);
            if (bs != null) {
                rs.put(field, decryptValue(key, cryptor, type, bs));
            }
        }
        return rs;
    }

    @Override
    public <T> Map<String, T> hscan(final String key, final Type type, int offset, int limit) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        Iterator<String> it = map.keySet(offset + limit).iterator();
        final Map<String, T> rs = new LinkedHashMap<>();
        int index = -1;
        while (it.hasNext()) {
            if (++index < offset) {
                continue;
            }
            if (index >= offset + limit) {
                break;
            }
            Object field = it.next();
            if (field == null) {
                continue;
            }
            if (field instanceof byte[]) {
                field = new String((byte[]) field, StandardCharsets.UTF_8);
            }
            byte[] bs = map.get(field.toString());
            if (bs != null) {
                rs.put(field.toString(), decryptValue(key, cryptor, type, bs));
            }
        }
        return rs;
    }

    @Override
    public <T> T hget(final String key, final String field, final Type type) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        byte[] bs = map.get(field);
        return decryptValue(key, cryptor, type, bs);
    }

    @Override
    public String hgetString(final String key, final String field) {
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        return decryptValue(key, cryptor, map.get(field));
    }

    @Override
    public long hgetLong(final String key, final String field, long defValue) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        Long rs = map.get(field);
        return rs == null ? defValue : rs;
    }

    @Override
    public CompletableFuture<Integer> hdelAsync(final String key, String... fields) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastRemoveAsync(fields).thenApply(r -> r.intValue()));
    }

    @Override
    public CompletableFuture<Integer> hlenAsync(final String key) {
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
    public CompletableFuture<Long> hincrbyAsync(final String key, String field, long num) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, num));
    }

    @Override
    public CompletableFuture<Double> hincrbyFloatAsync(final String key, String field, double num) {
        RMap<String, Double> map = client.getMap(key, MapDoubleCodec.instance);
        return completableFuture(map.addAndGetAsync(field, num));
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(final String key, String field) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, -1L));
    }

    @Override
    public CompletableFuture<Long> hdecrbyAsync(final String key, String field, long num) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, -num));
    }

    @Override
    public CompletableFuture<Boolean> hexistsAsync(final String key, String field) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.containsKeyAsync(field));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Type type, final T value) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutAsync(field, encryptValue(key, cryptor, type, convert, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert0, final Type type, final T value) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutAsync(field, encryptValue(key, cryptor, type, convert0, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Boolean> hsetnxAsync(final String key, final String field, final Type type, final T value) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutIfAbsentAsync(field, encryptValue(key, cryptor, type, convert, value)));
    }

    @Override
    public <T> CompletableFuture<Boolean> hsetnxAsync(final String key, final String field, final Convert convert0, final Type type, final T value) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutIfAbsentAsync(field, encryptValue(key, cryptor, type, convert0, value)));
    }

    @Override
    public CompletableFuture<Void> hsetStringAsync(final String key, final String field, final String value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        return completableFuture(map.fastPutAsync(field, encryptValue(key, cryptor, value)).thenApply(r -> null));
    }

    @Override
    public CompletableFuture<Boolean> hsetnxStringAsync(final String key, final String field, final String value) {
        if (value == null) {
            return CompletableFuture.completedFuture(false);
        }
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        return completableFuture(map.fastPutIfAbsentAsync(field, encryptValue(key, cryptor, value)));
    }

    @Override
    public CompletableFuture<Void> hsetLongAsync(final String key, final String field, final long value) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.fastPutAsync(field, value).thenApply(r -> null));
    }

    @Override
    public CompletableFuture<Boolean> hsetnxLongAsync(final String key, final String field, final long value) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.fastPutIfAbsentAsync(field, value));
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Serializable... values) {
        Map<String, byte[]> vals = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            vals.put(String.valueOf(values[i]), encryptValue(key, cryptor, convert, values[i + 1]));
        }
        RMap<String, byte[]> rm = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(rm.putAllAsync(vals));
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Map map) {
        Map<String, byte[]> vals = new LinkedHashMap<>();
        map.forEach((k, v) -> {
            vals.put(k.toString(), encryptValue(key, cryptor, convert, v));
        });
        RMap<String, byte[]> rm = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(rm.putAllAsync(vals));
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
                    list.add(decryptValue(key, cryptor, type, bs));
                }
            }
            return list;
        }));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hscanAsync(final String key, final Type type, int offset, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);

            Iterator<String> it = map.keySet(offset + limit).iterator();
            final Map<String, T> rs = new LinkedHashMap<>();
            int index = -1;
            while (it.hasNext()) {
                if (++index < offset) {
                    continue;
                }
                if (index >= offset + limit) {
                    break;
                }
                String field = it.next();
                byte[] bs = map.get(field);
                if (bs != null) {
                    rs.put(field, decryptValue(key, cryptor, type, bs));
                }
            }
            return rs;
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hscanAsync(final String key, final Type type, int offset, int limit, String pattern) {
        return CompletableFuture.supplyAsync(() -> {
            RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);

            Iterator<String> it = map.keySet(pattern, offset + limit).iterator();
            final Map<String, T> rs = new LinkedHashMap<>();
            int index = -1;
            while (it.hasNext()) {
                if (++index < offset) {
                    continue;
                }
                if (index >= offset + limit) {
                    break;
                }
                String field = it.next();
                byte[] bs = map.get(field);
                if (bs != null) {
                    rs.put(field, decryptValue(key, cryptor, type, bs));
                }
            }
            return rs;
        });
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(final String key, final String field, final Type type) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.getAsync(field).thenApply(r -> decryptValue(key, cryptor, type, r)));
    }

    @Override
    public CompletableFuture<String> hgetStringAsync(final String key, final String field) {
        RMap<String, String> map = client.getMap(key, MapStringCodec.instance);
        return completableFuture(key, cryptor, map.getAsync(field));
    }

    @Override
    public CompletableFuture<Long> hgetLongAsync(final String key, final String field, long defValue) {
        RMap<String, Long> map = client.getMap(key, MapLongCodec.instance);
        return completableFuture(map.getAsync(field).thenApply(r -> r == null ? defValue : r.longValue()));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Integer> llenAsync(String key) {
        return completableFuture(client.getList(key).sizeAsync());
    }

    @Override
    public CompletableFuture<Integer> scardAsync(String key) {
        return completableFuture(client.getSet(key).sizeAsync());
    }

    @Override
    public int llen(String key) {
        return client.getList(key).size();
    }

    @Override
    public int scard(String key) {
        return client.getSet(key).size();
    }

    @Override
    public <T> CompletableFuture<Set<T>> smembersAsync(String key, final Type componentType) {
        return completableFuture((CompletionStage) client.getSet(key, ByteArrayCodec.INSTANCE).readAllAsync().thenApply(set -> {
            if (set == null || set.isEmpty()) {
                return set;
            }
            Set<T> rs = new LinkedHashSet<>();
            for (Object item : set) {
                byte[] bs = (byte[]) item;
                if (bs == null) {
                    rs.add(null);
                } else {
                    rs.add(componentType == String.class ? (T) decryptValue(key, cryptor, new String(bs, StandardCharsets.UTF_8)) : (T) decryptValue(key, cryptor, componentType, bs));
                }
            }
            return rs;
        }));
    }

    @Override
    public <T> CompletableFuture<List<T>> lrangeAsync(String key, final Type componentType) {
        return completableFuture((CompletionStage) client.getList(key, ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
            if (list == null || list.isEmpty()) {
                return list;
            }
            List<T> rs = new ArrayList<>();
            for (Object item : list) {
                byte[] bs = (byte[]) item;
                if (bs == null) {
                    rs.add(null);
                } else {
                    rs.add(componentType == String.class ? (T) decryptValue(key, cryptor, new String(bs, StandardCharsets.UTF_8)) : (T) decryptValue(key, cryptor, componentType, bs));
                }
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<Map<String, Long>> mgetLongAsync(String... keys) {
        return completableFuture(client.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).getAsync(keys));
    }

    @Override
    public CompletableFuture<Map<String, String>> mgetStringAsync(String... keys) {
        return completableFuture(client.getBuckets(StringCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            if (cryptor == null) {
                return (Map) map;
            }
            Map rs = new LinkedHashMap();
            map.forEach((k, v) -> rs.put(k, decryptValue(k, cryptor, v == null ? null : v.toString())));
            return rs;
        }));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> mgetAsync(final Type componentType, String... keys) {
        return completableFuture(client.getBuckets(ByteArrayCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            Map rs = new LinkedHashMap();
            map.forEach((k, v) -> rs.put(k, decryptValue(k, cryptor, componentType, (byte[]) v)));
            return rs;
        }));
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> mgetBytesAsync(String... keys) {
        return completableFuture(client.getBuckets(ByteArrayCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            Map rs = new LinkedHashMap();
            map.forEach((k, v) -> rs.put(k, decryptValue(k, cryptor, byte[].class, (byte[]) v)));
            return rs;
        }));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hgetallAsync(final String key, final Type type) {
        return completableFuture(client.getMap(key, MapByteArrayCodec.instance).readAllMapAsync().thenApply(map -> {
            Map rs = new LinkedHashMap();
            map.forEach((k, v) -> rs.put(k.toString(), decryptValue(k.toString(), cryptor, type, (byte[]) v)));
            return rs;
        }));
    }

    @Override
    public <T> CompletableFuture<List<T>> hvalsAsync(final String key, final Type type) {
        return completableFuture(client.getMap(key, MapByteArrayCodec.instance).readAllValuesAsync().thenApply(list -> {
            List<T> rs = new ArrayList<>();
            for (Object v : list) {
                rs.add(decryptValue(key, cryptor, type, (byte[]) v));
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<Map<String, String>> hgetallStringAsync(final String key) {
        return hgetallAsync(key, String.class);
    }

    @Override
    public CompletableFuture<List<String>> hvalsStringAsync(final String key) {
        return hvalsAsync(key, String.class);
    }

    @Override
    public CompletableFuture<Map<String, Long>> hgetallLongAsync(final String key) {
        return hgetallAsync(key, Long.class);
    }

    @Override
    public CompletableFuture<List<Long>> hvalsLongAsync(final String key) {
        return hvalsAsync(key, Long.class);
    }

    @Override
    public <T> Map<String, T> hgetall(final String key, final Type type) {
        Map<Object, Object> map = client.getMap(key, MapByteArrayCodec.instance).readAllMap();
        Map rs = new LinkedHashMap();
        map.forEach((k, v) -> rs.put(k.toString(), decryptValue(k.toString(), cryptor, type, (byte[]) v)));
        return rs;
    }

    @Override
    public <T> List<T> hvals(final String key, final Type type) {
        Collection<Object> list = client.getMap(key, MapByteArrayCodec.instance).readAllValues();
        List<T> rs = new ArrayList<>();
        for (Object v : list) {
            rs.add(decryptValue(key, cryptor, type, (byte[]) v));
        }
        return rs;
    }

    @Override
    public Map<String, String> hgetallString(final String key) {
        return hgetallStringAsync(key).join();
    }

    @Override
    public List<String> hvalsString(final String key) {
        return hvalsStringAsync(key).join();
    }

    @Override
    public Map<String, Long> hgetallLong(final String key) {
        return hgetallLongAsync(key).join();
    }

    @Override
    public List<Long> hvalsLong(final String key) {
        return hvalsLongAsync(key).join();
    }

    @Override
    public <T> CompletableFuture<Map<String, List<T>>> lrangeAsync(final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, List<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, List<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = completableFuture(client.getList(key, ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
                if (list == null || list.isEmpty()) {
                    return list;
                }
                List<T> rs = new ArrayList<>();
                for (Object item : list) {
                    byte[] bs = (byte[]) item;
                    if (bs == null) {
                        rs.add(null);
                    } else {
                        rs.add(decryptValue(key, cryptor, componentType, bs));
                    }
                }
                mapLock.lock();
                try {
                    map.put(key, rs);
                } finally {
                    mapLock.unlock();
                }
                return rs;
            }));
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
    public <T> CompletableFuture<Map<String, Set<T>>> smembersAsync(final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Set<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Set<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).readAllAsync().thenApply(set -> {
                if (set == null || set.isEmpty()) {
                    return set;
                }
                Set<T> rs = new LinkedHashSet<>();
                for (Object item : set) {
                    byte[] bs = (byte[]) item;
                    if (bs == null) {
                        rs.add(null);
                    } else {
                        rs.add(decryptValue(key, cryptor, componentType, bs));
                    }
                }
                mapLock.lock();
                try {
                    map.put(key, rs);
                } finally {
                    mapLock.unlock();
                }
                return rs;
            }));
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
    public <T> Set<T> smembers(String key, final Type componentType) {
        return (Set) smembersAsync(key, componentType).join();
    }

    @Override
    public <T> List<T> lrange(String key, final Type componentType) {
        return (List) lrangeAsync(key, componentType).join();
    }

    @Override
    public Map<String, Long> mgetLong(final String... keys) {
        return client.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).get(keys);
    }

    @Override
    public Map<String, String> mgetString(final String... keys) {
        Map<String, String> map = client.getBuckets(StringCodec.INSTANCE).get(keys);
        if (cryptor != null && !map.isEmpty()) {
            Map<String, String> rs = new LinkedHashMap<>();
            map.forEach((k, v) -> rs.put(k, decryptValue(k, cryptor, v)));
            return rs;
        }
        return map;
    }

    @Override
    public <T> Map<String, T> mget(final Type componentType, final String... keys) {
        Map<String, byte[]> map = client.getBuckets(ByteArrayCodec.INSTANCE).get(keys);
        Map<String, T> rs = new LinkedHashMap(map.size());
        map.forEach((k, v) -> rs.put(k, decryptValue(k, cryptor, componentType, v)));
        return rs;
    }

    @Override
    public Map<String, byte[]> mgetBytes(final String... keys) {
        Map<String, byte[]> map = client.getBuckets(ByteArrayCodec.INSTANCE).get(keys);
        Map<String, byte[]> rs = new LinkedHashMap(map.size());
        map.forEach((k, v) -> rs.put(k, decryptValue(k, cryptor, byte[].class, v)));
        return rs;
    }

    @Override
    public <T> Map<String, Set<T>> smembers(final Type componentType, String... keys) {
        return (Map) smembersAsync(componentType, keys).join();
    }

    @Override
    public <T> Map<String, List<T>> lrange(final Type componentType, String... keys) {
        return (Map) lrangeAsync(componentType, keys).join();
    }

    //--------------------- existsItem ------------------------------  
    @Override
    public <T> boolean sismember(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        return bucket.contains(componentType == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, componentType, convert, value));
    }

    @Override
    public <T> CompletableFuture<Boolean> sismemberAsync(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(encryptValue(key, cryptor, componentType, convert, value)));
    }

    @Override
    public boolean sismemberString(String key, String value) {
        final RSet<String> bucket = client.getSet(key, StringCodec.INSTANCE);
        return bucket.contains(encryptValue(key, cryptor, value));
    }

    @Override
    public CompletableFuture<Boolean> sismemberStringAsync(String key, String value) {
        final RSet<String> bucket = client.getSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(encryptValue(key, cryptor, value)));
    }

    @Override
    public boolean sismemberLong(String key, long value) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return bucket.contains(value);
    }

    @Override
    public CompletableFuture<Boolean> sismemberLongAsync(String key, long value) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(value));
    }

    //--------------------- rpush ------------------------------  
    @Override
    public <T> CompletableFuture<Void> rpushAsync(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = client.getList(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAsync(encryptValue(key, cryptor, componentType, convert, value)).thenApply(r -> null));
    }

    @Override
    public <T> void rpush(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = client.getList(key, ByteArrayCodec.INSTANCE);
        bucket.add(componentType == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, componentType, convert, value));
    }

    @Override
    public CompletableFuture<Void> rpushStringAsync(String key, String value) {
        final RList<String> bucket = client.getList(key, StringCodec.INSTANCE);
        return completableFuture(bucket.addAsync(encryptValue(key, cryptor, value)).thenApply(r -> null));
    }

    @Override
    public void rpushString(String key, String value) {
        final RList<String> bucket = client.getList(key, StringCodec.INSTANCE);
        bucket.add(encryptValue(key, cryptor, value));
    }

    @Override
    public CompletableFuture<Void> rpushLongAsync(String key, long value) {
        final RList<Long> bucket = client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void rpushLong(String key, long value) {
        final RList<Long> bucket = client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE);
        bucket.add(value);
    }

    //--------------------- lrem ------------------------------  
    @Override
    public <T> CompletableFuture<Integer> lremAsync(String key, final Type componentType, T value) {
        return completableFuture(client.getList(key, ByteArrayCodec.INSTANCE).removeAsync(encryptValue(key, cryptor, componentType, convert, value)).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public <T> int lrem(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = client.getList(key, ByteArrayCodec.INSTANCE);
        return bucket.remove(componentType == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, componentType, convert, value)) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> lremStringAsync(String key, String value) {
        return completableFuture(client.getList(key, StringCodec.INSTANCE).removeAsync(encryptValue(key, cryptor, value)).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int lremString(String key, String value) {
        return client.getList(key, StringCodec.INSTANCE).remove(encryptValue(key, cryptor, value)) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> lremLongAsync(String key, long value) {
        return completableFuture(client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).removeAsync((Object) value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int lremLong(String key, long value) {
        return client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).remove((Object) value) ? 1 : 0;
    }

    //--------------------- sadd ------------------------------  
    @Override
    public <T> CompletableFuture<Void> saddAsync(String key, Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAsync(encryptValue(key, cryptor, componentType, convert, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<T> spopAsync(String key, Type componentType) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync().thenApply(bs -> bs == null ? null : decryptValue(key, cryptor, componentType, bs)));
    }

    @Override
    public <T> CompletableFuture<Set<T>> spopAsync(String key, int count, Type componentType) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count).thenApply((Set<byte[]> bslist) -> {
            if (bslist == null || bslist.isEmpty()) {
                return new LinkedHashSet<T>();
            }
            Set<T> rs = new LinkedHashSet<>();
            for (byte[] bs : bslist) {
                rs.add(decryptValue(key, cryptor, componentType, bs));
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<String> spopStringAsync(String key) {
        final RSet<String> bucket = client.getSet(key, StringCodec.INSTANCE);
        return completableFuture(key, cryptor, bucket.removeRandomAsync());
    }

    @Override
    public CompletableFuture<Set<String>> spopStringAsync(String key, int count) {
        final RSet<String> bucket = client.getSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count).thenApply(r -> {
            if (r == null) {
                return r;
            }
            if (cryptor == null) {
                return new LinkedHashSet<>(r);
            }
            Set<String> rs = new LinkedHashSet<>();
            for (Object item : r) {
                rs.add(item == null ? null : decryptValue(key, cryptor, item.toString()));
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<Long> spopLongAsync(String key) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync());
    }

    @Override
    public CompletableFuture<Set<Long>> spopLongAsync(String key, int count) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count));
    }

    @Override
    public <T> void sadd(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        bucket.add(componentType == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, componentType, convert, value));
    }

    @Override
    public <T> T spop(String key, final Type componentType) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        byte[] bs = bucket.removeRandom();
        return decryptValue(key, cryptor, componentType, bs);
    }

    @Override
    public <T> Set<T> spop(String key, int count, final Type componentType) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        Set< byte[]> bslist = bucket.removeRandom(count);
        Set<T> rs = new LinkedHashSet<>();
        if (bslist == null) {
            return rs;
        }
        for (byte[] bs : bslist) {
            rs.add(decryptValue(key, cryptor, componentType, bs));
        }
        return rs;
    }

    @Override
    public String spopString(String key) {
        final RSet<String> bucket = client.getSet(key, StringCodec.INSTANCE);
        return decryptValue(key, cryptor, bucket.removeRandom());
    }

    @Override
    public Set<String> spopString(String key, int count) {
        final RSet<String> bucket = client.getSet(key, StringCodec.INSTANCE);
        Set<String> r = bucket.removeRandom(count);
        if (cryptor == null) {
            return r;
        }
        Set<String> rs = new LinkedHashSet<>();
        for (String item : r) {
            rs.add(decryptValue(key, cryptor, item));
        }
        return rs;
    }

    @Override
    public Long spopLong(String key) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return bucket.removeRandom();
    }

    @Override
    public Set<Long> spopLong(String key, int count) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return bucket.removeRandom(count);
    }

    @Override
    public CompletableFuture<Void> saddStringAsync(String key, String value) {
        final RSet<String> bucket = client.getSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.addAsync(encryptValue(key, cryptor, value)).thenApply(r -> null));
    }

    @Override
    public void saddString(String key, String value) {
        final RSet<String> bucket = client.getSet(key, StringCodec.INSTANCE);
        bucket.add(encryptValue(key, cryptor, value));
    }

    @Override
    public CompletableFuture<Void> saddLongAsync(String key, long value) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void saddLong(String key, long value) {
        final RSet<Long> bucket = client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        bucket.add(value);
    }

    //--------------------- srem ------------------------------  
    @Override
    public <T> CompletableFuture<Integer> sremAsync(String key, final Type componentType, T value) {
        return completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).removeAsync(encryptValue(key, cryptor, componentType, convert, value)).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public <T> int srem(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        return bucket.remove(componentType == String.class ? String.valueOf(value).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, componentType, convert, value)) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> sremStringAsync(String key, String value) {
        return completableFuture(client.getSet(key, StringCodec.INSTANCE).removeAsync(encryptValue(key, cryptor, value)).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int sremString(String key, String value) {
        return client.getSet(key, StringCodec.INSTANCE).remove(encryptValue(key, cryptor, value)) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> sremLongAsync(String key, long value) {
        return completableFuture(client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).removeAsync(value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int sremLong(String key, long value) {
        return client.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).remove(value) ? 1 : 0;
    }

    //--------------------- keys ------------------------------  
    @Override
    public List<String> keys(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return client.getKeys().getKeysStream().collect(Collectors.toList());
        } else {
            return client.getKeys().getKeysStreamByPattern(pattern).collect(Collectors.toList());
        }
    }

    @Override
    public byte[] getBytes(final String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.get();
    }

    @Override
    public byte[] getSetBytes(final String key, byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.getAndSet(value);
    }

    @Override
    public byte[] getexBytes(final String key, final int expireSeconds) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.getAndExpire(Duration.ofSeconds(expireSeconds));
    }

    @Override
    public void setBytes(final String key, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        bucket.set(value);
    }

    @Override
    public void setexBytes(final String key, final int expireSeconds, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        bucket.set(value, expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public boolean setnxexBytes(final String key, final int expireSeconds, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return bucket.setIfAbsent(value, Duration.ofSeconds(expireSeconds));
    }

    @Override
    public CompletableFuture<byte[]> getBytesAsync(final String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync());
    }

    @Override
    public CompletableFuture<byte[]> getSetBytesAsync(final String key, byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAndSetAsync(value));
    }

    @Override
    public CompletableFuture<byte[]> getexBytesAsync(final String key, final int expireSeconds) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAndExpireAsync(Duration.ofSeconds(expireSeconds)));
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(final String key, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(value));
    }

    @Override
    public CompletableFuture<Void> setexBytesAsync(final String key, final int expireSeconds, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(value, expireSeconds, TimeUnit.SECONDS).thenApply(v -> null));
    }

    @Override
    public CompletableFuture<Boolean> setnxexBytesAsync(final String key, final int expireSeconds, final byte[] value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(value, Duration.ofSeconds(expireSeconds)));
    }

    @Override
    public CompletableFuture<List<String>> keysAsync(String pattern) {
        return CompletableFuture.supplyAsync(() -> keys(pattern));
    }

    //--------------------- dbsize ------------------------------  
    @Override
    public long dbsize() {
        return client.getKeys().count();
    }

    @Override
    public CompletableFuture<Long> dbsizeAsync() {
        return completableFuture(client.getKeys().countAsync());
    }

    protected static class MapByteArrayCodec extends ByteArrayCodec {

        public static final MapByteArrayCodec instance = new MapByteArrayCodec();

        @Override
        public org.redisson.client.protocol.Decoder<Object> getMapKeyDecoder() {
            return StringCodec.INSTANCE.getValueDecoder();
        }

        @Override
        public org.redisson.client.protocol.Encoder getMapKeyEncoder() {
            return StringCodec.INSTANCE.getValueEncoder();
        }
    }

    protected static class MapStringCodec extends StringCodec {

        public static final MapStringCodec instance = new MapStringCodec();

        @Override
        public org.redisson.client.protocol.Decoder<Object> getMapKeyDecoder() {
            return StringCodec.INSTANCE.getValueDecoder();
        }

        @Override
        public org.redisson.client.protocol.Encoder getMapKeyEncoder() {
            return StringCodec.INSTANCE.getValueEncoder();
        }
    }

    protected static class MapLongCodec extends org.redisson.client.codec.LongCodec {

        public static final MapLongCodec instance = new MapLongCodec();

        @Override
        public org.redisson.client.protocol.Decoder<Object> getMapKeyDecoder() {
            return StringCodec.INSTANCE.getValueDecoder();
        }

        @Override
        public org.redisson.client.protocol.Encoder getMapKeyEncoder() {
            return StringCodec.INSTANCE.getValueEncoder();
        }
    }

    protected static class MapDoubleCodec extends org.redisson.client.codec.DoubleCodec {

        public static final MapLongCodec instance = new MapLongCodec();

        @Override
        public org.redisson.client.protocol.Decoder<Object> getMapKeyDecoder() {
            return StringCodec.INSTANCE.getValueDecoder();
        }

        @Override
        public org.redisson.client.protocol.Encoder getMapKeyEncoder() {
            return StringCodec.INSTANCE.getValueEncoder();
        }
    }

    //-------------------------- 过期方法 ----------------------------------
    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(final boolean set, final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Collection<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getList(key, ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
                    if (list == null || list.isEmpty()) {
                        return list;
                    }
                    List<T> rs = new ArrayList<>();
                    for (Object item : list) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(decryptValue(key, cryptor, componentType, bs));
                        }
                    }
                    mapLock.lock();
                    try {
                        map.put(key, rs);
                    } finally {
                        mapLock.unlock();
                    }
                    return rs;
                }));
            }
        } else {
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
                    if (list == null || list.isEmpty()) {
                        return list;
                    }
                    List<T> rs = new ArrayList<>();
                    for (Object item : list) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(decryptValue(key, cryptor, componentType, bs));
                        }
                    }
                    mapLock.lock();
                    try {
                        map.put(key, rs);
                    } finally {
                        mapLock.unlock();
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
    @Deprecated(since = "2.8.0")
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
    @Deprecated(since = "2.8.0")
    public int getCollectionSize(String key) {
        String type = client.getScript().eval(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE);
        if (String.valueOf(type).contains("list")) {
            return client.getList(key).size();
        } else {
            return client.getSet(key).size();
        }
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return completableFuture(client.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) client.getList(key, ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
                    if (list == null || list.isEmpty()) {
                        return list;
                    }
                    List<T> rs = new ArrayList<>();
                    for (Object item : list) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(componentType == String.class ? (T) decryptValue(key, cryptor, new String(bs, StandardCharsets.UTF_8)) : (T) decryptValue(key, cryptor, componentType, bs));
                        }
                    }
                    return rs;
                });
            } else {
                return (CompletionStage) client.getSet(key, ByteArrayCodec.INSTANCE).readAllAsync().thenApply(set -> {
                    if (set == null || set.isEmpty()) {
                        return set;
                    }
                    Set<T> rs = new LinkedHashSet<>();
                    for (Object item : set) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(componentType == String.class ? (T) decryptValue(key, cryptor, new String(bs, StandardCharsets.UTF_8)) : (T) decryptValue(key, cryptor, componentType, bs));
                        }
                    }
                    return rs;
                });
            }
        }));
    }

    @Override
    @Deprecated(since = "2.8.0")
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
    @Deprecated(since = "2.8.0")
    public CompletableFuture<String[]> getStringArrayAsync(String... keys) {
        return completableFuture(client.getBuckets(StringCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            String[] rs = new String[keys.length];
            for (int i = 0; i < rs.length; i++) {
                rs[i] = decryptValue(keys[i], cryptor, (String) map.get(keys[i]));
            }
            return rs;
        }));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return completableFuture(client.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) client.getList(key, StringCodec.INSTANCE).readAllAsync().thenApply(list -> {
                    if (list == null || list.isEmpty() || cryptor == null) {
                        return list;
                    }
                    List<String> rs = new ArrayList<>();
                    for (Object item : list) {
                        rs.add(item == null ? null : decryptValue(key, cryptor, item.toString()));
                    }
                    return rs;
                });
            } else {
                return (CompletionStage) client.getSet(key, StringCodec.INSTANCE).readAllAsync().thenApply(set -> {
                    if (set == null) {
                        return set;
                    }
                    if (set.isEmpty() || cryptor == null) {
                        return new ArrayList<>(set);
                    }
                    List<String> rs = new ArrayList<>(); //不用set
                    for (Object item : set) {
                        rs.add(item == null ? null : decryptValue(key, cryptor, item.toString()));
                    }
                    return rs;
                });
            }
        }));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<String>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<String>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getList(key, StringCodec.INSTANCE).readAllAsync().thenApply((Collection r) -> {
                    if (r != null) {
                        if (cryptor != null && !r.isEmpty()) {
                            List<String> rs = new ArrayList<>();
                            for (Object item : r) {
                                rs.add(item == null ? null : decryptValue(key, cryptor, item.toString()));
                            }
                            r = rs;
                        }
                        mapLock.lock();
                        try {
                            map.put(key, r);
                        } finally {
                            mapLock.unlock();
                        }
                    }
                    return null;
                }));
            }
        } else {
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getSet(key, StringCodec.INSTANCE).readAllAsync().thenApply((Collection r) -> {
                    if (r != null) {
                        boolean changed = false;
                        if (cryptor != null && !r.isEmpty()) {
                            List<String> rs = new ArrayList<>();
                            for (Object item : r) {
                                rs.add(item == null ? null : decryptValue(key, cryptor, item.toString()));
                            }
                            r = rs;
                            changed = true;
                        }
                        mapLock.lock();
                        try {
                            map.put(key, changed ? r : new ArrayList(r));
                        } finally {
                            mapLock.unlock();
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
    @Deprecated(since = "2.8.0")
    public Collection<String> getStringCollection(String key) {
        return getStringCollectionAsync(key).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
    public Map<String, Collection<String>> getStringCollectionMap(final boolean set, String... keys) {
        return getStringCollectionMapAsync(set, keys).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
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
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<Long>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<Long>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(client.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync().thenApply(r -> {
                    if (r != null) {
                        mapLock.lock();
                        try {
                            map.put(key, (Collection) r);
                        } finally {
                            mapLock.unlock();
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
                        mapLock.lock();
                        try {
                            map.put(key, new ArrayList(r));
                        } finally {
                            mapLock.unlock();
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
    @Deprecated(since = "2.8.0")
    public Collection<Long> getLongCollection(String key) {
        return getLongCollectionAsync(key).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
    public Map<String, Collection<Long>> getLongCollectionMap(final boolean set, String... keys) {
        return getLongCollectionMapAsync(set, keys).join();
    }

    //--------------------- getexCollection ------------------------------  
    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Collection<T>> getexCollectionAsync(String key, int expireSeconds, final Type componentType) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key, componentType));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> Collection<T> getexCollection(String key, final int expireSeconds, final Type componentType) {
        return (Collection) getexCollectionAsync(key, expireSeconds, componentType).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<String>> getexStringCollectionAsync(String key, int expireSeconds) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getStringCollectionAsync(key));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public Collection<String> getexStringCollection(String key, final int expireSeconds) {
        return getexStringCollectionAsync(key, expireSeconds).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<Long>> getexLongCollectionAsync(String key, int expireSeconds) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getLongCollectionAsync(key));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public Collection<Long> getexLongCollection(String key, final int expireSeconds) {
        return getexLongCollectionAsync(key, expireSeconds).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> Collection<T> getCollection(String key, final Type componentType) {
        return (Collection) getCollectionAsync(key, componentType).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
    public Long[] getLongArray(final String... keys) {
        Map<String, Long> map = client.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).get(keys);
        Long[] rs = new Long[keys.length];
        for (int i = 0; i < rs.length; i++) {
            rs[i] = map.get(keys[i]);
        }
        return rs;
    }

    @Override
    @Deprecated(since = "2.8.0")
    public String[] getStringArray(final String... keys) {
        Map<String, String> map = client.getBuckets(StringCodec.INSTANCE).get(keys);
        String[] rs = new String[keys.length];
        for (int i = 0; i < rs.length; i++) {
            rs[i] = decryptValue(keys[i], cryptor, map.get(keys[i]));
        }
        return rs;
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> Map<String, Collection<T>> getCollectionMap(final boolean set, final Type componentType, String... keys) {
        return (Map) getCollectionMapAsync(set, componentType, keys).join();
    }

}
