/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;
import java.util.stream.Collectors;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.client.codec.*;
import org.redisson.client.protocol.*;
import org.redisson.config.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceListener;
import org.redkale.annotation.ResourceType;
import org.redkale.convert.Convert;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkale.util.Utility.*;

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

    protected static final Codec SCAN_CODEC = new Codec() {
        @Override
        public Decoder<Object> getMapValueDecoder() {
            return ByteArrayCodec.INSTANCE.getMapValueDecoder();
        }

        @Override
        public Encoder getMapValueEncoder() {
            return ByteArrayCodec.INSTANCE.getMapValueEncoder();
        }

        @Override
        public Decoder<Object> getMapKeyDecoder() {
            return StringCodec.INSTANCE.getMapKeyDecoder();
        }

        @Override
        public Encoder getMapKeyEncoder() {
            return StringCodec.INSTANCE.getMapKeyEncoder();
        }

        @Override
        public Decoder<Object> getValueDecoder() {
            return ByteArrayCodec.INSTANCE.getMapValueDecoder();
        }

        @Override
        public Encoder getValueEncoder() {
            return StringCodec.INSTANCE.getValueEncoder();
        }

        @Override
        public ClassLoader getClassLoader() {
            return ByteArrayCodec.INSTANCE.getClassLoader();
        }
    };

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (conf == null) {
            conf = AnyValue.create();
        }
        initClient(conf);
    }

    private void initClient(AnyValue conf) {
        RedisConfig config = RedisConfig.create(conf);
        Config redisConfig = new Config();
        String cluster = conf.getOrDefault("cluster", "");
        BaseConfig baseConfig = null;
        SingleServerConfig singleConfig = null;
        ClusterServersConfig clusterConfig = null;
        ReplicatedServersConfig replicateConfig = null;
        SentinelServersConfig sentinelConfig = null;
        for (String addr : config.getAddresses()) {
            if (config.getAddresses().size() == 1) { //单体
                if (singleConfig == null) {
                    singleConfig = redisConfig.useSingleServer();
                    singleConfig.setConnectionMinimumIdleSize(config.getMaxconns() / 2 + 1);
                    singleConfig.setConnectionPoolSize(config.getMaxconns());
                    baseConfig = singleConfig;
                }
                singleConfig.setAddress(addr);
                singleConfig.setDatabase(this.db);
            } else if ("cluster".equalsIgnoreCase(cluster)) { //集群
                if (clusterConfig == null) {
                    clusterConfig = redisConfig.useClusterServers();
                    clusterConfig.setMasterConnectionMinimumIdleSize(config.getMaxconns() / 2 + 1);
                    clusterConfig.setMasterConnectionPoolSize(config.getMaxconns());
                    clusterConfig.setSlaveConnectionMinimumIdleSize(config.getMaxconns() / 2 + 1);
                    clusterConfig.setSlaveConnectionPoolSize(config.getMaxconns());
                    baseConfig = clusterConfig;
                }
                clusterConfig.addNodeAddress(addr);
            } else if ("replicated".equalsIgnoreCase(cluster)) { //主从             
                if (replicateConfig == null) {
                    replicateConfig = redisConfig.useReplicatedServers();
                    replicateConfig.setMasterConnectionMinimumIdleSize(config.getMaxconns() / 2 + 1);
                    replicateConfig.setMasterConnectionPoolSize(config.getMaxconns());
                    replicateConfig.setSlaveConnectionMinimumIdleSize(config.getMaxconns() / 2 + 1);
                    replicateConfig.setSlaveConnectionPoolSize(config.getMaxconns());
                    baseConfig = replicateConfig;
                }
                replicateConfig.addNodeAddress(addr);
                replicateConfig.setDatabase(this.db);
            } else if ("sentinel".equalsIgnoreCase(cluster)) { //哨兵
                if (sentinelConfig == null) {
                    sentinelConfig = redisConfig.useSentinelServers();
                    sentinelConfig.setMasterConnectionMinimumIdleSize(config.getMaxconns() / 2 + 1);
                    sentinelConfig.setMasterConnectionPoolSize(config.getMaxconns());
                    sentinelConfig.setSlaveConnectionMinimumIdleSize(config.getMaxconns() / 2 + 1);
                    sentinelConfig.setSlaveConnectionPoolSize(config.getMaxconns());
                    baseConfig = sentinelConfig;
                }
                sentinelConfig.addSentinelAddress(addr);
                sentinelConfig.setDatabase(this.db);
            }
            if (baseConfig != null) {  //单个进程的不同自定义密码
                if (config.getUsername() != null) {
                    baseConfig.setUsername(config.getUsername());
                }
                if (config.getPassword() != null) {
                    baseConfig.setPassword(config.getPassword());
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
        this.nodeAddrs = config.getAddresses();
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
        initClient(this.conf);
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
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
    public org.redisson.api.RedissonClient getRedisClient() {
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

    protected <T> Collection<T> getCollectionValue(String key, Collection bytesResult, boolean set, Type componentType) {
        final Collection<T> rs = set ? new LinkedHashSet<>() : new ArrayList<>();
        Collection<byte[]> kvs = bytesResult;
        for (byte[] bs : kvs) {
            if (bs == null) {
                rs.add(null);
            } else if (componentType == String.class) {
                rs.add((T) decryptValue(key, cryptor, new String(bs, StandardCharsets.UTF_8)));
            } else {
                rs.add((T) decryptValue(key, cryptor, componentType, bs));
            }
        }
        return rs;
    }

    protected List<String> getSortedListValue(String key, Collection bytesResult) {
        final List<String> rs = new ArrayList<>();
        Collection<org.redisson.client.protocol.ScoredEntry> kvs = bytesResult;
        for (org.redisson.client.protocol.ScoredEntry bs : kvs) {
            rs.add(bs.getValue().toString());
        }
        return rs;
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (client != null) {
            client.shutdown();
        }
    }

    @Override
    public CompletableFuture<Boolean> isOpenAsync() {
        return CompletableFuture.completedFuture(client != null && !client.isShutdown());
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.isExistsAsync());
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(key, cryptor, type, bucket.getAsync());
    }

    //--------------------- getex ------------------------------
    @Override
    public <T> CompletableFuture<T> getexAsync(String key, int expireSeconds, final Type type) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAndExpireAsync(Duration.ofSeconds(expireSeconds)).thenApply(bs -> decryptValue(key, cryptor, type, bs)));
    }

    //--------------------- setex ------------------------------
    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert0, value)));
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, convert0, value)));
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        Convert c = convert0 == null ? this.convert : convert0;
        return completableFuture(bucket.getAndSetAsync(type == String.class ? encryptValue(key, cryptor, String.valueOf(value)).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, type, c, value))
            .thenApply(old -> old == null ? null : (T) c.convertFrom(type, old)));
    }

    @Override
    public CompletableFuture<Void> msetAsync(Serializable... keyVals) {
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

    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, Convert convert, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(encryptValue(key, cryptor, type, convert, value), expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Boolean> setnxexAsync(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = client.getBucket(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setIfAbsentAsync(encryptValue(key, cryptor, type, convert0, value), Duration.ofSeconds(expireSeconds)));
    }

    //--------------------- expire ------------------------------    
    @Override
    public CompletableFuture<Void> expireAsync(String key, int expireSeconds) {
        return completableFuture(client.getBucket(key).expireAsync(Duration.ofSeconds(expireSeconds)).thenApply(r -> null));
    }

    //--------------------- persist ------------------------------    
    @Override
    public CompletableFuture<Boolean> persistAsync(String key) {
        return completableFuture(client.getBucket(key).clearExpireAsync());
    }

    //--------------------- rename ------------------------------    
    @Override
    public CompletableFuture<Boolean> renameAsync(String oldKey, String newKey) {
        return completableFuture(client.getBucket(oldKey).renameAsync(newKey).handle((v, t) -> t == null));
    }

    @Override
    public CompletableFuture<Boolean> renamenxAsync(String oldKey, String newKey) {
        return completableFuture(client.getBucket(oldKey).renamenxAsync(newKey));
    }

    //--------------------- del ------------------------------    
    @Override
    public CompletableFuture<Long> delAsync(String... keys) {
        return completableFuture(client.getKeys().deleteAsync(keys));
    }

    //--------------------- incrby ------------------------------    
    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return completableFuture(client.getAtomicLong(key).incrementAndGetAsync());
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
    public CompletableFuture<Long> decrAsync(final String key) {
        return completableFuture(client.getAtomicLong(key).decrementAndGetAsync());
    }

    @Override
    public CompletableFuture<Long> decrbyAsync(final String key, long num) {
        return completableFuture(client.getAtomicLong(key).addAndGetAsync(-num));
    }

    @Override
    public CompletableFuture<Long> hdelAsync(final String key, String... fields) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastRemoveAsync(fields));
    }

    @Override
    public CompletableFuture<Long> hlenAsync(final String key) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.sizeAsync().thenApply(v -> v.longValue()));
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
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert0, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
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
    public <T> CompletableFuture<Map<String, T>> hscanAsync(final String key, final Type type, AtomicLong cursor, int limit, String pattern) {
        RFuture<List> future;
        RScript script = client.getScript(SCAN_CODEC);
        if (isEmpty(pattern)) {
            if (limit > 0) {
                String lua = "return redis.call('hscan', KEYS[1], ARGV[1], 'count', ARGV[2]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), String.valueOf(limit));
            } else {
                String lua = "return redis.call('hscan', KEYS[1], ARGV[1]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString());
            }
        } else {
            if (limit > 0) {
                String lua = "return redis.call('hscan', KEYS[1], ARGV[1], 'match', ARGV[2], 'count', ARGV[3]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), pattern, String.valueOf(limit));
            } else {
                String lua = "return redis.call('hscan', KEYS[1], ARGV[1], 'match', ARGV[2]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), pattern);
            }
        }
        return completableFuture(future.thenApply(result -> {
            final Map<String, T> rs = new LinkedHashMap<>();
            List<byte[]> kvs = (List) result.get(1);
            for (int i = 0; i < kvs.size(); i += 2) {
                String field = new String(kvs.get(i), StandardCharsets.UTF_8);
                byte[] bs = kvs.get(i + 1);
                if (bs != null) {
                    rs.put(field, decryptValue(key, cryptor, type, bs));
                }
            }
            cursor.set(Long.parseLong(new String((byte[]) result.get(0))));
            return rs;
        }));
    }

    @Override
    public CompletableFuture<List<String>> scanAsync(AtomicLong cursor, int limit, String pattern) {
        RFuture<List> future;
        RScript script = client.getScript(SCAN_CODEC);
        if (isEmpty(pattern)) {
            if (limit > 0) {
                String lua = "return redis.call('scan', ARGV[1], 'count', ARGV[2]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(), cursor.toString(), String.valueOf(limit));
            } else {
                String lua = "return redis.call('scan', ARGV[1]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(), cursor.toString());
            }
        } else {
            if (limit > 0) {
                String lua = "return redis.call('scan', ARGV[1], 'match', ARGV[2], 'count', ARGV[3]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(), cursor.toString(), pattern, String.valueOf(limit));
            } else {
                String lua = "return redis.call('scan', ARGV[1], 'match', ARGV[2]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(), cursor.toString(), pattern);
            }
        }
        return completableFuture(future.thenApply(result -> {
            final List<String> rs = (List) getCollectionValue(null, (Collection) result.get(1), false, String.class);
            cursor.set(Long.parseLong(new String((byte[]) result.get(0))));
            return rs;
        }));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sscanAsync(final String key, final Type componentType, AtomicLong cursor, int limit, String pattern) {
        RFuture<List> future;
        RScript script = client.getScript(SCAN_CODEC);
        if (isEmpty(pattern)) {
            if (limit > 0) {
                String lua = "return redis.call('sscan', KEYS[1], ARGV[1], 'count', ARGV[2]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), String.valueOf(limit));
            } else {
                String lua = "return redis.call('sscan', KEYS[1], ARGV[1]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString());
            }
        } else {
            if (limit > 0) {
                String lua = "return redis.call('sscan', KEYS[1], ARGV[1], 'match', ARGV[2], 'count', ARGV[3]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), pattern, String.valueOf(limit));
            } else {
                String lua = "return redis.call('sscan', KEYS[1], ARGV[1], 'match', ARGV[2]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), pattern);
            }
        }
        return completableFuture(future.thenApply(result -> {
            final Set<T> rs = (Set) getCollectionValue(key, (Collection) result.get(1), true, componentType);
            cursor.set(Long.parseLong(new String((byte[]) result.get(0))));
            return rs;
        }));
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(final String key, final String field, final Type type) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.getAsync(field).thenApply(r -> decryptValue(key, cryptor, type, r)));
    }

    @Override
    public CompletableFuture<Long> hstrlenAsync(final String key, final String field) {
        RMap<String, byte[]> map = client.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.valueSizeAsync(field).thenApply(r -> r.longValue()));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Long> scardAsync(String key) {
        return completableFuture(client.getSet(key).sizeAsync().thenApply(v -> v.longValue()));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sdiffAsync(final String key, final Type componentType, final String... key2s) {
        return completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).readDiffAsync(key2s)
            .thenApply(result -> (Set) getCollectionValue(key, result, true, componentType)));
    }

    @Override
    public <T> CompletableFuture<Boolean> smoveAsync(String key, String key2, Type componentType, T member) {
        return completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).moveAsync(key2, encryptValue(key, cryptor, componentType, convert, member)));
    }

    @Override
    public <T> CompletableFuture<List<T>> srandmemberAsync(String key, Type componentType, int count) {
        return completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).randomAsync(count)
            .thenApply(result -> (List) getCollectionValue(key, result, false, componentType)));
    }

    @Override
    public <T> CompletableFuture<List<Boolean>> smismembersAsync(final String key, final String... members) {
        List bs = new ArrayList<>();
        for (String m : members) {
            bs.add(m.getBytes(StandardCharsets.UTF_8));
        }
        return completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).containsEachAsync(bs)
            .thenApply(result -> {
                Set<String> keys = new HashSet<>();
                for (Object item : (List) result) {
                    keys.add(new String((byte[]) item, StandardCharsets.UTF_8));
                }
                List<Boolean> rs = new ArrayList<>();
                for (String m : members) {
                    rs.add(keys.contains(m));
                }
                return rs;
            }));
    }

    @Override
    public CompletableFuture<Long> sdiffstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return completableFuture(client.getSet(key).diffAsync(Utility.append(srcKey, srcKey2s)).thenApply(v -> v.longValue()));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sinterAsync(final String key, final Type componentType, final String... key2s) {
        return completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).readIntersectionAsync(key2s)
            .thenApply(result -> (Set) getCollectionValue(key, result, true, componentType)));
    }

    @Override
    public CompletableFuture<Long> sinterstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return completableFuture(client.getSet(key).intersectionAsync(Utility.append(srcKey, srcKey2s)).thenApply(v -> v.longValue()));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sunionAsync(final String key, final Type componentType, final String... key2s) {
        return completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).readUnionAsync(key2s)
            .thenApply(result -> (Set) getCollectionValue(key, result, true, componentType)));
    }

    @Override
    public CompletableFuture<Long> sunionstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return completableFuture(client.getSet(key).unionAsync(Utility.append(srcKey, srcKey2s)).thenApply(v -> v.longValue()));
    }

    @Override
    public <T> CompletableFuture<Set<T>> smembersAsync(String key, final Type componentType) {
        return completableFuture((CompletionStage) client.getSet(key, ByteArrayCodec.INSTANCE).readAllAsync()
            .thenApply(result -> (Set) getCollectionValue(key, result, true, componentType)));
    }

    @Override
    public <T> CompletableFuture<List<T>> lrangeAsync(String key, final Type componentType, int start, int stop) {
        return completableFuture((CompletionStage) client.getList(key, ByteArrayCodec.INSTANCE).rangeAsync(start, stop)
            .thenApply(result -> (List) getCollectionValue(key, result, false, componentType)));
    }

    @Override
    public <T> CompletableFuture<List<T>> mgetAsync(final Type componentType, String... keys) {
        return completableFuture(client.getBuckets(ByteArrayCodec.INSTANCE).getAsync(keys)
            .thenApply(result -> {
                List vs = new ArrayList();
                for (String key : keys) {
                    vs.add(result.get(key));
                }
                return (List) getCollectionValue(keys[0], vs, false, componentType);
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
    public <T> CompletableFuture<Map<String, List<T>>> lrangesAsync(final Type componentType, final String... keys) {
        final RBatch batch = client.createBatch();
        for (String key : keys) {
            batch.getList(key, ByteArrayCodec.INSTANCE).readAllAsync();
        }
        return completableFuture(batch.executeAsync().thenApply(resps -> {
            final Map<String, List<T>> map = new LinkedHashMap<>();
            List<Collection> list = (List) resps.getResponses();
            for (int i = 0; i < keys.length; i++) {
                String key = keys[i];
                List<T> rs = new ArrayList<>();
                for (Object item : list.get(i)) {
                    byte[] bs = (byte[]) item;
                    if (bs == null) {
                        rs.add(null);
                    } else {
                        rs.add(decryptValue(key, cryptor, componentType, bs));
                    }
                }
                map.put(key, rs);
            }
            return map;
        }));
    }

    @Override
    public <T> CompletableFuture<Map<String, Set<T>>> smembersAsync(final Type componentType, final String... keys) {
        final RBatch batch = client.createBatch();
        for (String key : keys) {
            batch.getSet(key, ByteArrayCodec.INSTANCE).readAllAsync();
        }
        return completableFuture(batch.executeAsync().thenApply(resps -> {
            final Map<String, Set<T>> map = new LinkedHashMap<>();
            List<Collection> list = (List) resps.getResponses();
            for (int i = 0; i < keys.length; i++) {
                String key = keys[i];
                Set<T> rs = new LinkedHashSet<>();
                for (Object item : list.get(i)) {
                    byte[] bs = (byte[]) item;
                    if (bs == null) {
                        rs.add(null);
                    } else {
                        rs.add(decryptValue(key, cryptor, componentType, bs));
                    }
                }
                map.put(key, rs);
            }
            return map;
        }));
    }

    //--------------------- existsItem ------------------------------  
    @Override
    public <T> CompletableFuture<Boolean> sismemberAsync(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(encryptValue(key, cryptor, componentType, convert, value)));
    }

    //--------------------- push ------------------------------  
    @Override
    public <T> CompletableFuture<Void> rpushAsync(String key, final Type componentType, T... values) {
        List<byte[]> list = new ArrayList<>();
        for (T value : values) {
            list.add(encryptValue(key, cryptor, componentType, convert, value));
        }
        final RDeque<byte[]> bucket = client.getDeque(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addLastAsync(list.toArray(new byte[list.size()][])).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> rpushxAsync(String key, final Type componentType, T... values) {
        List<byte[]> list = new ArrayList<>();
        for (T value : values) {
            list.add(encryptValue(key, cryptor, componentType, convert, value));
        }
        final RDeque<byte[]> bucket = client.getDeque(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.isExistsAsync()
            .thenCompose(b -> b ? bucket.addLastAsync(list.toArray(new byte[list.size()][])) : CompletableFuture.completedFuture(null))
            .thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<T> rpopAsync(String key, final Type componentType) {
        final RDeque<byte[]> bucket = client.getDeque(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.pollLastAsync()).thenApply(v -> decryptValue(key, cryptor, componentType, v));
    }

    @Override
    public <T> CompletableFuture<T> rpoplpushAsync(final String key, final String key2, final Type componentType) {
        final RDeque<byte[]> bucket = client.getDeque(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.pollLastAndOfferFirstToAsync(key2)).thenApply(v -> decryptValue(key, cryptor, componentType, v));
    }

    @Override
    public <T> CompletableFuture<Void> lpushAsync(String key, final Type componentType, T... values) {
        List<byte[]> list = new ArrayList<>();
        for (T value : values) {
            list.add(encryptValue(key, cryptor, componentType, convert, value));
        }
        final RDeque<byte[]> bucket = client.getDeque(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addFirstAsync(list.toArray(new byte[list.size()][])).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> lpushxAsync(String key, final Type componentType, T... values) {
        List<byte[]> list = new ArrayList<>();
        for (T value : values) {
            list.add(encryptValue(key, cryptor, componentType, convert, value));
        }
        final RDeque<byte[]> bucket = client.getDeque(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.isExistsAsync()
            .thenCompose(b -> b ? bucket.addFirstAsync(list.toArray(new byte[list.size()][])) : CompletableFuture.completedFuture(null))
            .thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<T> lpopAsync(String key, final Type componentType) {
        final RDeque<byte[]> bucket = client.getDeque(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.pollFirstAsync()).thenApply(v -> decryptValue(key, cryptor, componentType, v));
    }

    @Override
    public <T> CompletableFuture<T> lindexAsync(String key, Type componentType, int index) {
        final RList<byte[]> bucket = client.getList(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync(index)).thenApply(v -> decryptValue(key, cryptor, componentType, v));
    }

    @Override
    public <T> CompletableFuture<Long> linsertBeforeAsync(String key, Type componentType, T pivot, T value) {
        final RList<byte[]> bucket = client.getList(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addBeforeAsync(encryptValue(key, cryptor, componentType, convert, pivot),
            encryptValue(key, cryptor, componentType, convert, value)).thenApply(v -> v.longValue()));
    }

    @Override
    public <T> CompletableFuture<Long> linsertAfterAsync(String key, Type componentType, T pivot, T value) {
        final RList<byte[]> bucket = client.getList(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAfterAsync(encryptValue(key, cryptor, componentType, convert, pivot),
            encryptValue(key, cryptor, componentType, convert, value)).thenApply(v -> v.longValue()));
    }

    @Override
    public CompletableFuture<Long> llenAsync(String key) {
        return completableFuture(client.getList(key).sizeAsync().thenApply(v -> v.longValue()));
    }

    @Override
    public CompletableFuture<Void> ltrimAsync(final String key, int start, int stop) {
        final RList<byte[]> bucket = client.getList(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.trimAsync(start, stop));
    }

    //--------------------- lrem ------------------------------  
    @Override
    public <T> CompletableFuture<Long> lremAsync(String key, final Type componentType, T value) {
        return completableFuture(client.getList(key, ByteArrayCodec.INSTANCE).removeAsync(encryptValue(key, cryptor, componentType, convert, value)).thenApply(r -> r ? 1L : 0L));
    }

    //--------------------- sadd ------------------------------  
    @Override
    public <T> CompletableFuture<Void> saddAsync(String key, Type componentType, T... values) {
        List<byte[]> list = new ArrayList<>();
        for (T value : values) {
            list.add(encryptValue(key, cryptor, componentType, convert, value));
        }
        final RSet<byte[]> bucket = client.getSet(key, ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAllAsync(list).thenApply(r -> null));
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
            if (isEmpty(bslist)) {
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
    public <T> CompletableFuture<Long> sremAsync(String key, final Type componentType, T... values) {
        List<byte[]> list = new ArrayList<>();
        for (T value : values) {
            list.add(encryptValue(key, cryptor, componentType, convert, value));
        }
        return completableFuture(client.getSet(key, ByteArrayCodec.INSTANCE).removeAllAsync(list).thenApply(r -> r ? 1L : 0L));
    }

    //--------------------- sorted set ------------------------------  
    @Override
    public CompletableFuture<Void> zaddAsync(String key, CacheScoredValue... values) {
        Map<String, Double> map = new HashMap<>();
        for (CacheScoredValue value : values) {
            map.put(value.getValue(), value.getScore().doubleValue());
        }
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.addAllAsync(map).thenApply(r -> null));
    }

    @Override
    public <T extends Number> CompletableFuture<T> zincrbyAsync(String key, CacheScoredValue value) {
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.addScoreAsync(value.getValue(), value.getScore())
            .thenApply(v -> (T) decryptScore(value.getScore().getClass(), v)));
    }

    @Override
    public CompletableFuture<Long> zremAsync(String key, String... members) {
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.removeAllAsync(List.of(members)).thenApply(r -> r ? 1L : 0L));
    }

    @Override
    public <T extends Number> CompletableFuture<List<T>> zmscoreAsync(String key, Class<T> scoreType, String... members) {
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.getScoreAsync(List.of(members))
            .thenApply(list -> list.stream().map(v -> decryptScore(scoreType, v)).collect(Collectors.toList())));
    }

    @Override
    public <T extends Number> CompletableFuture<T> zscoreAsync(String key, Class<T> scoreType, String member) {
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.getScoreAsync(member).thenApply(r -> decryptScore(scoreType, r)));
    }

    @Override
    public CompletableFuture<Long> zcardAsync(String key) {
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.sizeAsync().thenApply(r -> r.longValue()));
    }

    @Override
    public CompletableFuture<Long> zrankAsync(String key, String member) {
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.rankAsync(member).thenApply(r -> r == null ? null : r.longValue()));
    }

    @Override
    public CompletableFuture<Long> zrevrankAsync(String key, String member) {
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.revRankAsync(member).thenApply(r -> r == null ? null : r.longValue()));
    }

    @Override
    public CompletableFuture<List<String>> zrangeAsync(String key, int start, int stop) {
        final RScoredSortedSet<String> bucket = client.getScoredSortedSet(key, StringCodec.INSTANCE);
        return completableFuture(bucket.entryRangeAsync(start, stop)
            .thenApply(result -> getSortedListValue(key, result)));
    }

    @Override
    public CompletableFuture<List<CacheScoredValue.NumberScoredValue>> zscanAsync(String key, Type scoreType, AtomicLong cursor, int limit, String pattern) {
        RFuture<List> future;
        RScript script = client.getScript(SCAN_CODEC);
        if (isEmpty(pattern)) {
            if (limit > 0) {
                String lua = "return redis.call('zscan', KEYS[1], ARGV[1], 'count', ARGV[2]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), String.valueOf(limit));
            } else {
                String lua = "return redis.call('zscan', KEYS[1], ARGV[1]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString());
            }
        } else {
            if (limit > 0) {
                String lua = "return redis.call('zscan', KEYS[1], ARGV[1], 'match', ARGV[2], 'count', ARGV[3]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), pattern, String.valueOf(limit));
            } else {
                String lua = "return redis.call('zscan', KEYS[1], ARGV[1], 'match', ARGV[2]);";
                future = script.evalAsync(RScript.Mode.READ_ONLY, lua, RScript.ReturnType.MULTI, List.of(key), cursor.toString(), pattern);
            }
        }
        return completableFuture(future.thenApply(result -> {
            final List<CacheScoredValue.NumberScoredValue> rs = new ArrayList<>();
            List<byte[]> kvs = (List) result.get(1);
            for (int i = 0; i < kvs.size(); i += 2) {
                String field = new String(kvs.get(i), StandardCharsets.UTF_8);
                byte[] bs = kvs.get(i + 1);
                if (bs != null) {
                    rs.add(new CacheScoredValue.NumberScoredValue(decryptValue(key, cryptor, scoreType, bs), field));
                }
            }
            cursor.set(Long.parseLong(new String((byte[]) result.get(0))));
            return rs;
        }));
    }

    //--------------------- keys ------------------------------  
    @Override
    public CompletableFuture<List<String>> keysAsync(String pattern) {
        if (isEmpty(pattern)) {
            return client.reactive().getKeys().getKeys().collectList().toFuture();
        } else {
            return client.reactive().getKeys().getKeysByPattern(pattern).collectList().toFuture();
        }
    }

    //--------------------- dbsize ------------------------------  
    @Override
    public CompletableFuture<Long> dbsizeAsync() {
        return completableFuture(client.getKeys().countAsync());
    }

    @Override
    public CompletableFuture<Void> flushdbAsync() {
        return completableFuture(client.getKeys().flushdbAsync());
    }

    @Override
    public CompletableFuture<Void> flushallAsync() {
        return completableFuture(client.getKeys().flushallAsync());
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
                    if (isEmpty(list)) {
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
                    if (isEmpty(list)) {
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
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return completableFuture(client.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) client.getList(key, ByteArrayCodec.INSTANCE).readAllAsync()
                    .thenApply(result -> (List) getCollectionValue(key, result, false, componentType));
            } else {
                return (CompletionStage) client.getSet(key, ByteArrayCodec.INSTANCE).readAllAsync()
                    .thenApply(result -> (Set) getCollectionValue(key, result, true, componentType));
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
                    if (isEmpty(list) || cryptor == null) {
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

    //--------------------- getexCollection ------------------------------  
    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Collection<T>> getexCollectionAsync(String key, int expireSeconds, final Type componentType) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key, componentType));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<String>> getexStringCollectionAsync(String key, int expireSeconds) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getStringCollectionAsync(key));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<Long>> getexLongCollectionAsync(String key, int expireSeconds) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getLongCollectionAsync(key));
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

}
