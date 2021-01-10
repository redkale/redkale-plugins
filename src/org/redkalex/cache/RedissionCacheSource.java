/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache;

import java.io.*;
import java.lang.reflect.Type;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.config.*;
import org.redkale.convert.Convert;
import org.redkale.convert.json.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.*;
import org.redkale.source.*;
import org.redkale.util.*;
import org.redkale.util.AnyValue.DefaultAnyValue;

/**
 * //https://www.cnblogs.com/xiami2046/p/13934146.html
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@ResourceType(CacheSource.class)
public class RedissionCacheSource<V extends Object> extends AbstractService implements CacheSource<V>, Service, AutoCloseable, Resourcable {

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Resource
    protected JsonConvert defaultConvert;

    @Resource(name = "$_convert")
    protected JsonConvert convert;

    protected Type objValueType = String.class;

    protected List<String> nodeAddrs;

    protected int db;

    protected RedissonClient redisson;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = this.defaultConvert;
        if (conf == null) conf = new AnyValue.DefaultAnyValue();

        final List<String> addresses = new ArrayList<>();
        Config config = new Config();
        AnyValue[] nodes = conf.getAnyValues("node");
        String type = conf.getOrDefault("type", "");
        int maxconns = conf.getIntValue("maxconns", 0);
        BaseConfig baseConfig = null;
        for (AnyValue node : nodes) {
            String addr = node.getValue("addr");
            addresses.add(addr);
            String db0 = node.getValue("db", "").trim();
            if (!db0.isEmpty()) this.db = Integer.valueOf(db0);
            if (nodes.length == 1) {
                baseConfig = config.useSingleServer();
                if (maxconns > 0) config.useSingleServer().setConnectionPoolSize(maxconns);
                config.useSingleServer().setAddress(addr);
                config.useSingleServer().setDatabase(this.db);
            } else if ("masterslave".equalsIgnoreCase(type)) { //主从
                baseConfig = config.useMasterSlaveServers();
                if (maxconns > 0) config.useMasterSlaveServers().setMasterConnectionPoolSize(maxconns);
                if (maxconns > 0) config.useMasterSlaveServers().setSlaveConnectionPoolSize(maxconns);
                if (node.get("master") != null) {
                    config.useMasterSlaveServers().setMasterAddress(addr);
                } else {
                    config.useMasterSlaveServers().addSlaveAddress(addr);
                }
                config.useMasterSlaveServers().setDatabase(this.db);
            } else if ("cluster".equalsIgnoreCase(type)) { //集群
                baseConfig = config.useClusterServers();
                if (maxconns > 0) config.useClusterServers().setMasterConnectionPoolSize(maxconns);
                if (maxconns > 0) config.useClusterServers().setSlaveConnectionPoolSize(maxconns);
                config.useClusterServers().addNodeAddress(addr);
            } else if ("replicated".equalsIgnoreCase(type)) { //
                baseConfig = config.useReplicatedServers();
                if (maxconns > 0) config.useMasterSlaveServers().setMasterConnectionPoolSize(maxconns);
                if (maxconns > 0) config.useReplicatedServers().setSlaveConnectionPoolSize(maxconns);
                config.useReplicatedServers().addNodeAddress(addr);
                config.useReplicatedServers().setDatabase(this.db);
            } else if ("sentinel".equalsIgnoreCase(type)) { //
                baseConfig = config.useSentinelServers();
                if (maxconns > 0) config.useSentinelServers().setMasterConnectionPoolSize(maxconns);
                if (maxconns > 0) config.useSentinelServers().setSlaveConnectionPoolSize(maxconns);
                config.useSentinelServers().addSentinelAddress(addr);
                config.useSentinelServers().setDatabase(this.db);
            }
        }
        if (baseConfig != null) {
            String username = conf.getValue("username", "").trim();
            String password = conf.getValue("password", "").trim();
            String retryAttempts = conf.getValue("retryAttempts", "").trim();
            String retryInterval = conf.getValue("retryInterval", "").trim();
            if (!username.isEmpty()) baseConfig.setUsername(username);
            if (!password.isEmpty()) baseConfig.setPassword(password);
            if (!retryAttempts.isEmpty()) baseConfig.setRetryAttempts(Integer.parseInt(retryAttempts));
            if (!retryInterval.isEmpty()) baseConfig.setRetryInterval(Integer.parseInt(retryInterval));
        }
        this.redisson = Redisson.create(config);
        this.nodeAddrs = addresses;
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedisCacheSource.class.getSimpleName() + ": addrs=" + addresses + ", db=" + db);

    }

    @Override //ServiceLoader时判断配置是否符合当前实现类
    public boolean match(AnyValue config) {
        if (config == null) return false;
        AnyValue[] nodes = config.getAnyValues("node");
        if (nodes == null || nodes.length == 0) return false;
        for (AnyValue node : nodes) {
            String val = node.getValue("addr");
            if (val != null && val.startsWith("redis://")) return true;
        }
        return false;
    }

    @Override
    @Deprecated
    public final void initValueType(Type valueType) {
        this.objValueType = valueType == null ? String.class : valueType;
    }

    @Override
    @Deprecated
    public final void initTransient(boolean flag) {
    }

    @Override
    public final String getType() {
        return "redis";
    }

    protected <T> CompletableFuture<T> completableFuture(CompletionStage<T> rf) {
        CompletableFuture future = new CompletableFuture();
        rf.whenComplete((r, t) -> {
            if (t != null) {
                future.completeExceptionally(t);
            } else {
                future.complete(r);
            }
        });
        return future;
    }

    public static void main(String[] args) throws Exception {
        DefaultAnyValue conf = new DefaultAnyValue().addValue("maxconns", "1");
        conf.addValue("node", new DefaultAnyValue().addValue("addr", "redis://127.0.0.1:6363"));

        RedissionCacheSource source = new RedissionCacheSource();
        source.defaultConvert = JsonFactory.root().getConvert();
        source.init(conf);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 7788);
        try {
            System.out.println("------------------------------------");
            source.removeAsync("stritem1");
            source.removeAsync("stritem2");
            source.setStringAsync("stritem1", "value1");
            source.setStringAsync("stritem2", "value2");
            System.out.println("stritem开头的key有两个: " + source.queryKeysStartsWith("stritem"));
            System.out.println("[有值] MGET : " + source.getStringMap("stritem1", "stritem2"));
            System.out.println("[有值] MGET : " + Arrays.toString(source.getStringArray("stritem1", "stritem2")));

            source.remove("intitem1");
            source.remove("intitem2");
            source.setLong("intitem1", 333);
            source.setLong("intitem2", 444);
            System.out.println("[有值] MGET : " + source.getStringMap("intitem1", "intitem22", "intitem2"));
            System.out.println("[有值] MGET : " + Arrays.toString(source.getStringArray("intitem1", "intitem22", "intitem2")));
            source.remove("objitem1");
            source.remove("objitem2");
            source.set("objitem1", Flipper.class, new Flipper(10));
            source.set("objitem2", Flipper.class, new Flipper(20));
            System.out.println("[有值] MGET : " + source.getMap(Flipper.class, "objitem1", "objitem2"));

            source.remove("key1");
            source.remove("key2");
            source.remove("300");
            source.set(1000, "key1", String.class, "value1");
            source.set("key1", String.class, "value1");
            source.setString("keystr1", "strvalue1");
            source.setLong("keylong1", 333L);
            source.set("300", String.class, "4000");
            source.getAndRefresh("key1", 3500, String.class);
            System.out.println("[有值] 300 GET : " + source.get("300", String.class));
            System.out.println("[有值] key1 GET : " + source.get("key1", String.class));
            System.out.println("[无值] key2 GET : " + source.get("key2", String.class));
            System.out.println("[有值] keystr1 GET : " + source.getString("keystr1"));
            System.out.println("[有值] keylong1 GET : " + source.getLong("keylong1", 0L));
            System.out.println("[有值] key1 EXISTS : " + source.exists("key1"));
            System.out.println("[无值] key2 EXISTS : " + source.exists("key2"));

            source.remove("keys3");
            source.appendListItem("keys3", String.class, "vals1");
            source.appendListItem("keys3", String.class, "vals2");
            System.out.println("-------- keys3 追加了两个值 --------");
            System.out.println("[两值] keys3 VALUES : " + source.getCollection("keys3", String.class));
            System.out.println("[有值] keys3 EXISTS : " + source.exists("keys3"));
            source.removeListItem("keys3", String.class, "vals1");
            System.out.println("[一值] keys3 VALUES : " + source.getCollection("keys3", String.class));
            source.getCollectionAndRefresh("keys3", 3000, String.class);

            source.remove("stringmap");
            source.appendSetItem("stringmap", JsonConvert.TYPE_MAP_STRING_STRING, Utility.ofMap("a", "aa", "b", "bb"));
            source.appendSetItem("stringmap", JsonConvert.TYPE_MAP_STRING_STRING, Utility.ofMap("c", "cc", "d", "dd"));
            System.out.println("[两值] stringmap VALUES : " + source.getCollectionAsync("stringmap", JsonConvert.TYPE_MAP_STRING_STRING).join());

            source.remove("sets3");
            source.remove("sets4");
            source.appendSetItem("sets3", String.class, "setvals1");
            source.appendSetItem("sets3", String.class, "setvals2");
            source.appendSetItem("sets3", String.class, "setvals1");
            source.appendSetItem("sets4", String.class, "setvals2");
            source.appendSetItem("sets4", String.class, "setvals1");
            System.out.println("[两值] sets3 VALUES : " + source.getCollection("sets3", String.class));
            System.out.println("[有值] sets3 EXISTS : " + source.exists("sets3"));
            System.out.println("[有值] sets3-setvals2 EXISTSITEM : " + source.existsSetItem("sets3", String.class, "setvals2"));
            System.out.println("[有值] sets3-setvals3 EXISTSITEM : " + source.existsSetItem("sets3", String.class, "setvals3"));
            source.removeSetItem("sets3", String.class, "setvals1");
            System.out.println("[一值] sets3 VALUES : " + source.getCollection("sets3", String.class));
            System.out.println("sets3 大小 : " + source.getCollectionSize("sets3"));
            System.out.println("all keys: " + source.queryKeys());
            System.out.println("key startkeys: " + source.queryKeysStartsWith("key"));
            System.out.println("newnum 值 : " + source.incr("newnum"));
            System.out.println("newnum 值 : " + source.decr("newnum"));
            System.out.println("sets3&sets4:  " + source.getStringCollectionMap(true, "sets3", "sets4"));
            System.out.println("------------------------------------");
            source.set("myaddr", InetSocketAddress.class, addr);
            System.out.println("myaddrstr:  " + source.getString("myaddr"));
            System.out.println("myaddr:  " + source.get("myaddr", InetSocketAddress.class));
            source.remove("myaddrs");
            source.remove("myaddrs2");
            source.appendSetItem("myaddrs", InetSocketAddress.class, new InetSocketAddress("127.0.0.1", 7788));
            source.appendSetItem("myaddrs", InetSocketAddress.class, new InetSocketAddress("127.0.0.1", 7799));
            System.out.println("myaddrs:  " + source.getCollection("myaddrs", InetSocketAddress.class));
            source.removeSetItem("myaddrs", InetSocketAddress.class, new InetSocketAddress("127.0.0.1", 7788));
            System.out.println("myaddrs:  " + source.getCollection("myaddrs", InetSocketAddress.class));
            source.appendSetItem("myaddrs2", InetSocketAddress.class, new InetSocketAddress("127.0.0.1", 7788));
            source.appendSetItem("myaddrs2", InetSocketAddress.class, new InetSocketAddress("127.0.0.1", 7799));
            System.out.println("myaddrs&myaddrs2:  " + source.getCollectionMap(true, InetSocketAddress.class, "myaddrs", "myaddrs2"));
            System.out.println("------------------------------------");
            source.remove("myaddrs");
            Type mapType = new TypeToken<Map<String, Integer>>() {
            }.getType();
            Map<String, Integer> map = new HashMap<>();
            map.put("a", 1);
            map.put("b", 2);
            source.set("mapvals", mapType, map);
            System.out.println("mapvals:  " + source.get("mapvals", mapType));

            source.remove("byteskey");
            source.setBytes("byteskey", new byte[]{1, 2, 3});
            System.out.println("byteskey 值 : " + Arrays.toString(source.getBytes("byteskey")));
            //h
            source.remove("hmap");
            source.hincr("hmap", "key1");
            System.out.println("hmap.key1 值 : " + source.hgetLong("hmap", "key1", -1));
            source.hmset("hmap", "key2", "haha", "key3", 333);
            source.hmset("hmap", "sm", (HashMap) Utility.ofMap("a", "aa", "b", "bb"));
            System.out.println("hmap.sm 值 : " + source.hget("hmap", "sm", JsonConvert.TYPE_MAP_STRING_STRING));
            System.out.println("hmap.[key1,key2,key3] 值 : " + source.hmget("hmap", String.class, "key1", "key2", "key3"));
            System.out.println("hmap.keys 四值 : " + source.hkeys("hmap"));
            source.hremove("hmap", "key1", "key3");
            System.out.println("hmap.keys 两值 : " + source.hkeys("hmap"));
            System.out.println("hmap.key2 值 : " + source.hgetString("hmap", "key2"));
            System.out.println("hmap列表(2)大小 : " + source.hsize("hmap"));

            source.remove("hmaplong");
            source.hincr("hmaplong", "key1", 10);
            source.hsetLong("hmaplong", "key2", 30);
            System.out.println("hmaplong.所有两值 : " + source.hmap("hmaplong", long.class, 0, 10));

            source.remove("hmapstr");
            source.hsetString("hmapstr", "key1", "str10");
            source.hsetString("hmapstr", "key2", null);
            System.out.println("hmapstr.所有一值 : " + source.hmap("hmapstr", String.class, 0, 10));

            source.remove("hmapstrmap");
            source.hset("hmapstrmap", "key1", JsonConvert.TYPE_MAP_STRING_STRING, (HashMap) Utility.ofMap("ks11", "vv11"));
            source.hset("hmapstrmap", "key2", JsonConvert.TYPE_MAP_STRING_STRING, null);
            System.out.println("hmapstrmap.无值 : " + source.hmap("hmapstrmap", JsonConvert.TYPE_MAP_STRING_STRING, 0, 10, "key2*"));

            source.remove("popset");
            source.appendStringSetItem("popset", "111");
            source.appendStringSetItem("popset", "222");
            source.appendStringSetItem("popset", "333");
            source.appendStringSetItem("popset", "444");
            source.appendStringSetItem("popset", "555");
            System.out.println("SPOP一个元素：" + source.spopStringSetItem("popset"));
            System.out.println("SPOP两个元素：" + source.spopStringSetItem("popset", 2));
            System.out.println("SPOP五个元素：" + source.spopStringSetItem("popset", 5));
            source.appendLongSetItem("popset", 111);
            source.appendLongSetItem("popset", 222);
            source.appendLongSetItem("popset", 333);
            source.appendLongSetItem("popset", 444);
            source.appendLongSetItem("popset", 555);
            System.out.println("SPOP一个元素：" + source.spopLongSetItem("popset"));
            System.out.println("SPOP两个元素：" + source.spopLongSetItem("popset", 2));
            System.out.println("SPOP五个元素：" + source.spopLongSetItem("popset", 5));
            System.out.println("SPOP一个元素：" + source.spopLongSetItem("popset"));

            //清除
            int rs = source.remove("stritem1");
            System.out.println("删除stritem1个数: " + rs);
            source.remove("popset");
            source.remove("stritem2");
            source.remove("intitem1");
            source.remove("intitem2");
            source.remove("keylong1");
            source.remove("keystr1");
            source.remove("mapvals");
            source.remove("myaddr");
            source.remove("myaddrs2");
            source.remove("newnum");
            source.remove("objitem1");
            source.remove("objitem2");
            source.remove("key1");
            source.remove("key2");
            source.remove("keys3");
            source.remove("sets3");
            source.remove("sets4");
            source.remove("myaddrs");
            source.remove("300");
            source.remove("stringmap");
            source.remove("hmap");
            source.remove("hmaplong");
            source.remove("hmapstr");
            source.remove("hmapstrmap");
            source.remove("byteskey");
            System.out.println("------------------------------------");
//        System.out.println("--------------测试大文本---------------");
//        HashMap<String, String> bigmap = new HashMap<>();
//        StringBuilder sb = new StringBuilder();
//        sb.append("起始");
//        for (int i = 0; i < 1024 * 1024; i++) {
//            sb.append("abcde");
//        }
//        sb.append("结束");
//        bigmap.put("val", sb.toString());
//        System.out.println("文本长度: " + sb.length());
//        source.set("bigmap", JsonConvert.TYPE_MAP_STRING_STRING, bigmap);
//        System.out.println("写入完成");
//        for (int i = 0; i < 1; i++) {
//            HashMap<String, String> fs = (HashMap) source.get("bigmap", JsonConvert.TYPE_MAP_STRING_STRING);
//            System.out.println("内容长度: " + fs.get("val").length());
//        }
            source.remove("bigmap");

        } finally {
            source.close();
        }
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
    public String toString() {
        return getClass().getSimpleName() + "{addrs = " + this.nodeAddrs + ", db=" + this.db + "}";
    }

    @Override
    public void destroy(AnyValue conf) {
        if (redisson != null) redisson.shutdown();
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.isExistsAsync());
    }

    @Override
    public boolean exists(String key) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.isExists();
    }

    //--------------------- get ------------------------------
    @Override
    @Deprecated
    public CompletableFuture<V> getAsync(String key) {
        return getAsync(key, objValueType);
    }

    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenApply(bs -> bs == null ? null : convert.convertFrom(type, bs)));
    }

    @Override
    public CompletableFuture<String> getStringAsync(String key) {
        final RBucket<String> bucket = redisson.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.getAsync());
    }

    @Override
    public CompletableFuture<Long> getLongAsync(String key, long defValue) {
        final RAtomicLong bucket = redisson.getAtomicLong(key);
        return completableFuture(bucket.getAsync());
    }

    @Override
    @Deprecated
    public V get(String key) {
        return get(key, objValueType);
    }

    @Override
    public <T> T get(String key, final Type type) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        byte[] bs = bucket.get();
        return bs == null ? null : convert.convertFrom(type, bs);
    }

    @Override
    public String getString(String key) {
        final RBucket<String> bucket = redisson.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return bucket.get();
    }

    @Override
    public long getLong(String key, long defValue) {
        final RAtomicLong bucket = redisson.getAtomicLong(key);
        return bucket.get();
    }

    //--------------------- getAndRefresh ------------------------------
    @Override
    @Deprecated
    public CompletableFuture<V> getAndRefreshAsync(String key, int expireSeconds) {
        return getAndRefreshAsync(key, expireSeconds, objValueType);
    }

    @Override
    public <T> CompletableFuture<T> getAndRefreshAsync(String key, int expireSeconds, final Type type) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenCompose(bs -> {
            T rs = convert.convertFrom(type, bs);
            if (rs == null) return CompletableFuture.completedFuture(null);
            return bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> rs);
        }));
    }

    @Override
    @Deprecated
    public V getAndRefresh(String key, final int expireSeconds) {
        return getAndRefresh(key, expireSeconds, objValueType);
    }

    @Override
    public <T> T getAndRefresh(String key, final int expireSeconds, final Type type) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        T rs = convert.convertFrom(type, bucket.get());
        if (rs == null) return rs;
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
        return rs;
    }

    @Override
    public CompletableFuture<String> getStringAndRefreshAsync(String key, int expireSeconds) {
        final RBucket<String> bucket = redisson.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenCompose(rs -> {
            if (rs == null) return CompletableFuture.completedFuture(null);
            return bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> rs);
        }));
    }

    @Override
    public String getStringAndRefresh(String key, final int expireSeconds) {
        final RBucket<String> bucket = redisson.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        String rs = bucket.get();
        if (rs == null) return rs;
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
        return rs;
    }

    @Override
    public CompletableFuture<Long> getLongAndRefreshAsync(String key, int expireSeconds, long defValue) {
        final RAtomicLong bucket = redisson.getAtomicLong(key);
        return completableFuture(bucket.getAsync().thenCompose(rs -> {
            if (rs == null) return CompletableFuture.completedFuture(defValue);
            return bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> rs);
        }));
    }

    @Override
    public long getLongAndRefresh(String key, final int expireSeconds, long defValue) {
        final RAtomicLong bucket = redisson.getAtomicLong(key);
        long rs = bucket.get();
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
        return rs;
    }

    //--------------------- refresh ------------------------------
    @Override
    public CompletableFuture<Void> refreshAsync(String key, int expireSeconds) {
        final RBucket bucket = redisson.getBucket(key);
        return completableFuture(bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public void refresh(String key, final int expireSeconds) {
        final RBucket bucket = redisson.getBucket(key);
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    //--------------------- set ------------------------------
    @Override
    @Deprecated
    public CompletableFuture<Void> setAsync(String key, V value) {
        return setAsync(key, objValueType, value);
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert0, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? this.convert : convert0).convertToBytes(value)));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, final Type type, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(this.convert.convertToBytes(type, value)));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? this.convert : convert0).convertToBytes(type, value)));
    }

    @Override
    @Deprecated
    public void set(final String key, V value) {
        set(key, objValueType, value);
    }

    @Override
    public <T> void set(final String key, final Convert convert0, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? this.convert : convert0).convertToBytes(value));
    }

    @Override
    public <T> void set(final String key, final Type type, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(this.convert.convertToBytes(type, value));
    }

    @Override
    public <T> void set(String key, final Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? this.convert : convert0).convertToBytes(type, value));
    }

    @Override
    public CompletableFuture<Void> setStringAsync(String key, String value) {
        return completableFuture(redisson.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE).setAsync(value));
    }

    @Override
    public void setString(String key, String value) {
        redisson.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE).set(value);
    }

    @Override
    public CompletableFuture<Void> setLongAsync(String key, long value) {
        return completableFuture(redisson.getAtomicLong(key).setAsync(value));
    }

    @Override
    public void setLong(String key, long value) {
        redisson.getAtomicLong(key).set(value);
    }

    //--------------------- set ------------------------------    
    @Override
    @Deprecated
    public CompletableFuture<Void> setAsync(int expireSeconds, String key, V value) {
        return setAsync(expireSeconds, key, objValueType, value);
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, Convert convert0, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? convert : convert0).convertToBytes(value)).thenCompose(v -> bucket.expireAsync(expireSeconds, TimeUnit.SECONDS)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, final Type type, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(convert.convertToBytes(type, value)).thenCompose(v -> bucket.expireAsync(expireSeconds, TimeUnit.SECONDS)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? convert : convert0).convertToBytes(type, value)).thenCompose(v -> bucket.expireAsync(expireSeconds, TimeUnit.SECONDS)).thenApply(r -> null));
    }

    @Override
    @Deprecated
    public void set(int expireSeconds, String key, V value) {
        set(expireSeconds, key, objValueType, value);
    }

    @Override
    public <T> void set(int expireSeconds, String key, Convert convert0, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? convert : convert0).convertToBytes(value));
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public <T> void set(int expireSeconds, String key, final Type type, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(convert.convertToBytes(type, value));
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public <T> void set(int expireSeconds, String key, Convert convert0, final Type type, T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? convert : convert0).convertToBytes(type, value));
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Void> setStringAsync(int expireSeconds, String key, String value) {
        final RBucket<String> bucket = redisson.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.setAsync(value).thenCompose(v -> bucket.expireAsync(expireSeconds, TimeUnit.SECONDS)).thenApply(r -> null));
    }

    @Override
    public void setString(int expireSeconds, String key, String value) {
        final RBucket<String> bucket = redisson.getBucket(key, org.redisson.client.codec.StringCodec.INSTANCE);
        bucket.set(value);
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Void> setLongAsync(int expireSeconds, String key, long value) {
        final RAtomicLong bucket = redisson.getAtomicLong(key);
        return completableFuture(bucket.setAsync(value).thenCompose(v -> bucket.expireAsync(expireSeconds, TimeUnit.SECONDS)).thenApply(r -> null));
    }

    @Override
    public void setLong(int expireSeconds, String key, long value) {
        final RAtomicLong bucket = redisson.getAtomicLong(key);
        bucket.set(value);
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    //--------------------- setExpireSeconds ------------------------------    
    @Override
    public CompletableFuture<Void> setExpireSecondsAsync(String key, int expireSeconds) {
        return completableFuture(redisson.getBucket(key).expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(r -> null));
    }

    @Override
    public void setExpireSeconds(String key, int expireSeconds) {
        redisson.getBucket(key).expire(expireSeconds, TimeUnit.SECONDS);
    }

    //--------------------- remove ------------------------------    
    @Override
    public CompletableFuture<Integer> removeAsync(String key) {
        return completableFuture(redisson.getBucket(key).deleteAsync().thenApply(rs -> rs ? 1 : 0));
    }

    @Override
    public int remove(String key) {
        return redisson.getBucket(key).delete() ? 1 : 0;
    }

    //--------------------- incr ------------------------------    
    @Override
    public long incr(final String key) {
        return redisson.getAtomicLong(key).incrementAndGet();
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return completableFuture(redisson.getAtomicLong(key).incrementAndGetAsync());
    }

    @Override
    public long incr(final String key, long num) {
        return redisson.getAtomicLong(key).addAndGet(num);
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key, long num) {
        return completableFuture(redisson.getAtomicLong(key).addAndGetAsync(num));
    }

    //--------------------- decr ------------------------------    
    @Override
    public long decr(final String key) {
        return redisson.getAtomicLong(key).decrementAndGet();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return completableFuture(redisson.getAtomicLong(key).decrementAndGetAsync());
    }

    @Override
    public long decr(final String key, long num) {
        return redisson.getAtomicLong(key).addAndGet(-num);
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key, long num) {
        return completableFuture(redisson.getAtomicLong(key).addAndGetAsync(-num));
    }

    @Override
    public int hremove(final String key, String... fields) {
        RMap<String, ?> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return (int) map.fastRemove(fields);
    }

    @Override
    public int hsize(final String key) {
        return redisson.getMap(key, MapByteArrayCodec.instance).size();
    }

    @Override
    public List<String> hkeys(final String key) {
        return (List) new ArrayList<>(redisson.getMap(key, MapStringCodec.instance).keySet());
    }

    @Override
    public long hincr(final String key, String field) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, 1L);
    }

    @Override
    public long hincr(final String key, String field, long num) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, num);
    }

    @Override
    public long hdecr(final String key, String field) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, -1L);
    }

    @Override
    public long hdecr(final String key, String field, long num) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return map.addAndGet(field, num);
    }

    @Override
    public boolean hexists(final String key, String field) {
        return redisson.getMap(key, MapByteArrayCodec.instance).containsKey(field);
    }

    @Override
    public <T> void hset(final String key, final String field, final Convert convert0, final T value) {
        if (value == null) return;
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        map.fastPut(field, (convert0 == null ? convert : convert0).convertToBytes(objValueType, value));
    }

    @Override
    public <T> void hset(final String key, final String field, final Type type, final T value) {
        if (value == null) return;
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        map.fastPut(field, this.convert.convertToBytes(type, value));
    }

    @Override
    public <T> void hset(final String key, final String field, final Convert convert0, final Type type, final T value) {
        if (value == null) return;
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        map.fastPut(field, (convert0 == null ? convert : convert0).convertToBytes(type, value));
    }

    @Override
    public void hsetString(final String key, final String field, final String value) {
        if (value == null) return;
        RMap<String, String> map = redisson.getMap(key, MapStringCodec.instance);
        map.fastPut(field, value);
    }

    @Override
    public void hsetLong(final String key, final String field, final long value) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        map.fastPut(field, value);
    }

    @Override
    public void hmset(final String key, final Serializable... values) {
        Map<String, byte[]> vals = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            vals.put(String.valueOf(values[i]), this.convert.convertToBytes(values[i + 1]));
        }
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        map.putAll(vals);
    }

    @Override
    public List<Serializable> hmget(final String key, final Type type, final String... fields) {
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        Map<String, byte[]> rs = map.getAll(Set.of(fields));
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
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        Iterator<String> it = map.keySet(pattern, offset + limit).iterator();
        final Map<String, T> rs = new HashMap<>();
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
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        Iterator<String> it = map.keySet(offset + limit).iterator();
        final Map<String, T> rs = new HashMap<>();
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
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        byte[] bs = map.get(field);
        return bs == null ? null : convert.convertFrom(type, bs);
    }

    @Override
    public String hgetString(final String key, final String field) {
        RMap<String, String> map = redisson.getMap(key, MapStringCodec.instance);
        return map.get(field);
    }

    @Override
    public long hgetLong(final String key, final String field, long defValue) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        Long rs = map.get(field);
        return rs == null ? defValue : rs;
    }

    @Override
    public CompletableFuture<Integer> hremoveAsync(final String key, String... fields) {
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastRemoveAsync(fields).thenApply(r -> r.intValue()));
    }

    @Override
    public CompletableFuture<Integer> hsizeAsync(final String key) {
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.sizeAsync());
    }

    @Override
    public CompletableFuture<List<String>> hkeysAsync(final String key) {
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.readAllKeySetAsync().thenApply(set -> set == null ? null : new ArrayList(set)));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, 1L));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field, long num) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, num));
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(final String key, String field) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, -1L));
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(final String key, String field, long num) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return completableFuture(map.addAndGetAsync(field, -num));
    }

    @Override
    public CompletableFuture<Boolean> hexistsAsync(final String key, String field) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return completableFuture(map.containsKeyAsync(field));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert0, final T value) {
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutAsync(field, (convert0 == null ? convert : convert0).convertToBytes(objValueType, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Type type, final T value) {
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutAsync(field, convert.convertToBytes(type, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert0, final Type type, final T value) {
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.fastPutAsync(field, (convert0 == null ? convert : convert0).convertToBytes(type, value)).thenApply(r -> null));
    }

    @Override
    public CompletableFuture<Void> hsetStringAsync(final String key, final String field, final String value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        RMap<String, String> map = redisson.getMap(key, MapStringCodec.instance);
        return completableFuture(map.fastPutAsync(field, value).thenApply(r -> null));
    }

    @Override
    public CompletableFuture<Void> hsetLongAsync(final String key, final String field, final long value) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return completableFuture(map.fastPutAsync(field, value).thenApply(r -> null));
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Serializable... values) {
        Map<String, byte[]> vals = new HashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            vals.put(String.valueOf(values[i]), this.convert.convertToBytes(values[i + 1]));
        }
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.putAllAsync(vals));
    }

    @Override
    public CompletableFuture<List<Serializable>> hmgetAsync(final String key, final Type type, final String... fields) {
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.getAllAsync(Set.of(fields)).thenApply(rs -> {
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
            RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);

            Iterator<String> it = map.keySet(offset + limit).iterator();
            final Map<String, T> rs = new HashMap<>();
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
            RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);

            Iterator<String> it = map.keySet(pattern, offset + limit).iterator();
            final Map<String, T> rs = new HashMap<>();
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
        RMap<String, byte[]> map = redisson.getMap(key, MapByteArrayCodec.instance);
        return completableFuture(map.getAsync(field).thenApply(r -> r == null ? null : convert.convertFrom(type, r)));
    }

    @Override
    public CompletableFuture<String> hgetStringAsync(final String key, final String field) {
        RMap<String, String> map = redisson.getMap(key, MapStringCodec.instance);
        return completableFuture(map.getAsync(field));
    }

    @Override
    public CompletableFuture<Long> hgetLongAsync(final String key, final String field, long defValue) {
        RMap<String, Long> map = redisson.getMap(key, MapLongCodec.instance);
        return completableFuture(map.getAsync(field).thenApply(r -> r == null ? defValue : r.longValue()));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Integer> getCollectionSizeAsync(String key) {
        return completableFuture(redisson.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return redisson.getList(key).sizeAsync();
            } else {
                return redisson.getSet(key).sizeAsync();
            }
        }));
    }

    @Override
    public int getCollectionSize(String key) {
        String type = redisson.getScript().eval(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE);
        if (String.valueOf(type).contains("list")) {
            return redisson.getList(key).size();
        } else {
            return redisson.getSet(key).size();
        }
    }

    @Override
    @Deprecated
    public CompletableFuture<Collection<V>> getCollectionAsync(String key) {
        return getCollectionAsync(key, objValueType);
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return completableFuture(redisson.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) redisson.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
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
                    return rs;
                });
            } else {
                return (CompletionStage) redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).readAllAsync().thenApply(set -> {
                    if (set == null || set.isEmpty()) return set;
                    Set<T> rs = new HashSet<>();
                    for (Object item : set) {
                        byte[] bs = (byte[]) item;
                        if (bs == null) {
                            rs.add(null);
                        } else {
                            rs.add(convert.convertFrom(componentType, bs));
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
        return completableFuture(redisson.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).getAsync(keys));
    }

    @Override
    public CompletableFuture<Long[]> getLongArrayAsync(String... keys) {
        return completableFuture(redisson.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            Long[] rs = new Long[keys.length];
            for (int i = 0; i < rs.length; i++) {
                rs[i] = (Long) map.get(keys[i]);
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<String[]> getStringArrayAsync(String... keys) {
        return completableFuture(redisson.getBuckets(org.redisson.client.codec.StringCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            String[] rs = new String[keys.length];
            for (int i = 0; i < rs.length; i++) {
                rs[i] = (String) map.get(keys[i]);
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<Map<String, String>> getStringMapAsync(String... keys) {
        return completableFuture(redisson.getBuckets(org.redisson.client.codec.StringCodec.INSTANCE).getAsync(keys));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> getMapAsync(final Type componentType, String... keys) {
        return completableFuture(redisson.getBuckets(org.redisson.client.codec.ByteArrayCodec.INSTANCE).getAsync(keys).thenApply(map -> {
            Map rs = new HashMap();
            map.forEach((k, v) -> rs.put(k, convert.convertFrom(componentType, (byte[]) v)));
            return rs;
        }));
    }

    @Override
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(final boolean set, final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Collection<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<T>> map = new HashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(redisson.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
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
                futures[i] = completableFuture(redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).readAllAsync().thenApply(list -> {
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
    @Deprecated
    public Collection<V> getCollection(String key) {
        return getCollection(key, objValueType);
    }

    @Override
    public <T> Collection<T> getCollection(String key, final Type componentType) {
        return (Collection) getCollectionAsync(key, componentType).join();
    }

    @Override
    public Map<String, Long> getLongMap(final String... keys) {
        return redisson.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).get(keys);
    }

    @Override
    public Long[] getLongArray(final String... keys) {
        Map<String, Long> map = redisson.getBuckets(org.redisson.client.codec.LongCodec.INSTANCE).get(keys);
        Long[] rs = new Long[keys.length];
        for (int i = 0; i < rs.length; i++) {
            rs[i] = map.get(keys[i]);
        }
        return rs;
    }

    @Override
    public Map<String, String> getStringMap(final String... keys) {
        return redisson.getBuckets(org.redisson.client.codec.StringCodec.INSTANCE).get(keys);
    }

    @Override
    public String[] getStringArray(final String... keys) {
        Map<String, String> map = redisson.getBuckets(org.redisson.client.codec.StringCodec.INSTANCE).get(keys);
        String[] rs = new String[keys.length];
        for (int i = 0; i < rs.length; i++) {
            rs[i] = map.get(keys[i]);
        }
        return rs;
    }

    @Override
    public <T> Map<String, T> getMap(final Type componentType, final String... keys) {
        Map<String, byte[]> map = redisson.getBuckets(org.redisson.client.codec.ByteArrayCodec.INSTANCE).get(keys);
        Map<String, T> rs = new HashMap(map.size());
        map.forEach((k, v) -> rs.put(k, convert.convertFrom(componentType, v)));
        return rs;
    }

    @Override
    public <T> Map<String, Collection<T>> getCollectionMap(final boolean set, final Type componentType, String... keys) {
        return (Map) getCollectionMapAsync(set, componentType, keys).join();
    }

    @Override
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return completableFuture(redisson.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) redisson.getList(key, org.redisson.client.codec.StringCodec.INSTANCE).readAllAsync();
            } else {
                return (CompletionStage) redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE).readAllAsync().thenApply(s -> s == null ? null : new ArrayList(s));
            }
        }));
    }

    @Override
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<String>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<String>> map = new HashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(redisson.getList(key, org.redisson.client.codec.StringCodec.INSTANCE).readAllAsync().thenApply(r -> {
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
                futures[i] = completableFuture(redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE).readAllAsync().thenApply(r -> {
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
        return completableFuture(redisson.getScript().evalAsync(RScript.Mode.READ_ONLY, "return redis.call('TYPE', '" + key + "')", RScript.ReturnType.VALUE).thenCompose(type -> {
            if (String.valueOf(type).contains("list")) {
                return (CompletionStage) redisson.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync();
            } else {
                return (CompletionStage) redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync().thenApply(s -> s == null ? null : new ArrayList(s));
            }
        }));
    }

    @Override
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<Long>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<Long>> map = new HashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list    
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = completableFuture(redisson.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync().thenApply(r -> {
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
                futures[i] = completableFuture(redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).readAllAsync().thenApply(r -> {
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
    @Deprecated
    public CompletableFuture<Collection<V>> getCollectionAndRefreshAsync(String key, int expireSeconds) {
        return getCollectionAndRefreshAsync(key, expireSeconds, objValueType);
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAndRefreshAsync(String key, int expireSeconds, final Type componentType) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key, componentType));
    }

    @Override
    @Deprecated
    public Collection<V> getCollectionAndRefresh(String key, final int expireSeconds) {
        return getCollectionAndRefresh(key, expireSeconds, objValueType);
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
    @Deprecated
    public boolean existsSetItem(String key, V value) {
        return existsSetItem(key, objValueType, value);
    }

    @Override
    public <T> boolean existsSetItem(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.contains(convert.convertToBytes(componentType, value));
    }

    @Override
    @Deprecated
    public CompletableFuture<Boolean> existsSetItemAsync(String key, V value) {
        return existsSetItemAsync(key, objValueType, value);
    }

    @Override
    public <T> CompletableFuture<Boolean> existsSetItemAsync(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(convert.convertToBytes(componentType, value)));
    }

    @Override
    public boolean existsStringSetItem(String key, String value) {
        final RSet<String> bucket = redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return bucket.contains(value);
    }

    @Override
    public CompletableFuture<Boolean> existsStringSetItemAsync(String key, String value) {
        final RSet<String> bucket = redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(value));
    }

    @Override
    public boolean existsLongSetItem(String key, long value) {
        final RSet<Long> bucket = redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return bucket.contains(value);
    }

    @Override
    public CompletableFuture<Boolean> existsLongSetItemAsync(String key, long value) {
        final RSet<Long> bucket = redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.containsAsync(value));
    }

    //--------------------- appendListItem ------------------------------  
    @Override
    @Deprecated
    public CompletableFuture<Void> appendListItemAsync(String key, V value) {
        return appendListItemAsync(key, objValueType, value);
    }

    @Override
    public <T> CompletableFuture<Void> appendListItemAsync(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = redisson.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAsync(convert.convertToBytes(componentType, value)).thenApply(r -> null));
    }

    @Override
    @Deprecated
    public void appendListItem(String key, V value) {
        appendListItem(key, objValueType, value);
    }

    @Override
    public <T> void appendListItem(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = redisson.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.add(convert.convertToBytes(componentType, value));
    }

    @Override
    public CompletableFuture<Void> appendStringListItemAsync(String key, String value) {
        final RList<String> bucket = redisson.getList(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void appendStringListItem(String key, String value) {
        final RList<String> bucket = redisson.getList(key, org.redisson.client.codec.StringCodec.INSTANCE);
        bucket.add(value);
    }

    @Override
    public CompletableFuture<Void> appendLongListItemAsync(String key, long value) {
        final RList<Long> bucket = redisson.getList(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void appendLongListItem(String key, long value) {
        final RList<Long> bucket = redisson.getList(key, org.redisson.client.codec.LongCodec.INSTANCE);
        bucket.add(value);
    }

    //--------------------- removeListItem ------------------------------  
    @Override
    @Deprecated
    public CompletableFuture<Integer> removeListItemAsync(String key, V value) {
        return removeListItemAsync(key, objValueType, value);
    }

    @Override
    public <T> CompletableFuture<Integer> removeListItemAsync(String key, final Type componentType, T value) {
        return completableFuture(redisson.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).removeAsync(convert.convertToBytes(componentType, value)).thenApply(r -> r ? 1 : 0));
    }

    @Override
    @Deprecated
    public int removeListItem(String key, V value) {
        return removeListItem(key, objValueType, value);
    }

    @Override
    public <T> int removeListItem(String key, final Type componentType, T value) {
        final RList<byte[]> bucket = redisson.getList(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.remove(convert.convertToBytes(componentType, value)) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> removeStringListItemAsync(String key, String value) {
        return completableFuture(redisson.getList(key, org.redisson.client.codec.StringCodec.INSTANCE).removeAsync(value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int removeStringListItem(String key, String value) {
        return redisson.getList(key, org.redisson.client.codec.StringCodec.INSTANCE).remove(value) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> removeLongListItemAsync(String key, long value) {
        return completableFuture(redisson.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).removeAsync((Object) value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int removeLongListItem(String key, long value) {
        return redisson.getList(key, org.redisson.client.codec.LongCodec.INSTANCE).remove((Object) value) ? 1 : 0;
    }

    //--------------------- appendSetItem ------------------------------  
    @Override
    @Deprecated
    public CompletableFuture<Void> appendSetItemAsync(String key, V value) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAsync(convert.convertToBytes(objValueType, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<Void> appendSetItemAsync(String key, Type componentType, T value) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.addAsync(convert.convertToBytes(componentType, value)).thenApply(r -> null));
    }

    @Override
    public <T> CompletableFuture<T> spopSetItemAsync(String key, Type componentType) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync().thenApply(bs -> bs == null ? null : convert.convertFrom(componentType, bs)));
    }

    @Override
    public <T> CompletableFuture<List<T>> spopSetItemAsync(String key, int count, Type componentType) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count).thenApply((Set<byte[]> bslist) -> {
            if (bslist == null || bslist.isEmpty()) return new ArrayList<>();
            List<T> rs = new ArrayList<>();
            for (byte[] bs : bslist) {
                rs.add(convert.convertFrom(componentType, bs));
            }
            return rs;
        }));
    }

    @Override
    public CompletableFuture<String> spopStringSetItemAsync(String key) {
        final RSet<String> bucket = redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync());
    }

    @Override
    public CompletableFuture<List<String>> spopStringSetItemAsync(String key, int count) {
        final RSet<String> bucket = redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count).thenApply(r -> r == null ? null : new ArrayList<>(r)));
    }

    @Override
    public CompletableFuture<Long> spopLongSetItemAsync(String key) {
        final RSet<Long> bucket = redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync());
    }

    @Override
    public CompletableFuture<List<Long>> spopLongSetItemAsync(String key, int count) {
        final RSet<Long> bucket = redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.removeRandomAsync(count).thenApply(r -> r == null ? null : new ArrayList<>(r)));
    }

    @Override
    @Deprecated
    public void appendSetItem(String key, V value) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.add(convert.convertToBytes(objValueType, value));
    }

    @Override
    public <T> void appendSetItem(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.add(convert.convertToBytes(componentType, value));
    }

    @Override
    public <T> T spopSetItem(String key, final Type componentType) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        byte[] bs = bucket.removeRandom();
        return bs == null ? null : convert.convertFrom(componentType, bs);
    }

    @Override
    public <T> List<T> spopSetItem(String key, int count, final Type componentType) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        Set< byte[]> bslist = bucket.removeRandom(count);
        List<T> rs = new ArrayList<>();
        if (bslist == null) return rs;
        for (byte[] bs : bslist) {
            rs.add(convert.convertFrom(componentType, bs));
        }
        return rs;
    }

    @Override
    public String spopStringSetItem(String key) {
        final RSet<String> bucket = redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return bucket.removeRandom();
    }

    @Override
    public List<String> spopStringSetItem(String key, int count) {
        final RSet<String> bucket = redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        Set<String> rs = bucket.removeRandom(count);
        return rs == null ? null : new ArrayList<>(rs);
    }

    @Override
    public Long spopLongSetItem(String key) {
        final RSet<Long> bucket = redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return bucket.removeRandom();
    }

    @Override
    public List<Long> spopLongSetItem(String key, int count) {
        final RSet<Long> bucket = redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        Set<Long> rs = bucket.removeRandom(count);
        return rs == null ? null : new ArrayList<>(rs);
    }

    @Override
    public CompletableFuture<Void> appendStringSetItemAsync(String key, String value) {
        final RSet<String> bucket = redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void appendStringSetItem(String key, String value) {
        final RSet<String> bucket = redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE);
        bucket.add(value);
    }

    @Override
    public CompletableFuture<Void> appendLongSetItemAsync(String key, long value) {
        final RSet<Long> bucket = redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        return completableFuture(bucket.addAsync(value).thenApply(r -> null));
    }

    @Override
    public void appendLongSetItem(String key, long value) {
        final RSet<Long> bucket = redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE);
        bucket.add(value);
    }

    //--------------------- removeSetItem ------------------------------  
    @Override
    @Deprecated
    public CompletableFuture<Integer> removeSetItemAsync(String key, V value) {
        return removeSetItemAsync(key, objValueType, value);
    }

    @Override
    public <T> CompletableFuture<Integer> removeSetItemAsync(String key, final Type componentType, T value) {
        return completableFuture(redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE).removeAsync(convert.convertToBytes(componentType, value)).thenApply(r -> r ? 1 : 0));
    }

    @Override
    @Deprecated
    public int removeSetItem(String key, V value) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.remove(convert.convertToBytes(objValueType, value)) ? 1 : 0;
    }

    @Override
    public <T> int removeSetItem(String key, final Type componentType, T value) {
        final RSet<byte[]> bucket = redisson.getSet(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.remove(convert.convertToBytes(componentType, value)) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> removeStringSetItemAsync(String key, String value) {
        return completableFuture(redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE).removeAsync(value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int removeStringSetItem(String key, String value) {
        return redisson.getSet(key, org.redisson.client.codec.StringCodec.INSTANCE).remove(value) ? 1 : 0;
    }

    @Override
    public CompletableFuture<Integer> removeLongSetItemAsync(String key, long value) {
        return completableFuture(redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).removeAsync(value).thenApply(r -> r ? 1 : 0));
    }

    @Override
    public int removeLongSetItem(String key, long value) {
        return redisson.getSet(key, org.redisson.client.codec.LongCodec.INSTANCE).remove(value) ? 1 : 0;
    }

    //--------------------- queryKeys ------------------------------  
    @Override
    public List<String> queryKeys() {
        return redisson.getKeys().getKeysStream().collect(Collectors.toList());
    }

    @Override
    public List<String> queryKeysStartsWith(String startsWith) {
        return redisson.getKeys().getKeysStreamByPattern(startsWith + "*").collect(Collectors.toList());
    }

    @Override
    public List<String> queryKeysEndsWith(String endsWith) {
        return redisson.getKeys().getKeysStreamByPattern("*" + endsWith).collect(Collectors.toList());
    }

    @Override
    public byte[] getBytes(final String key) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return bucket.get();
    }

    @Override
    public byte[] getBytesAndRefresh(final String key, final int expireSeconds) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        byte[] bs = bucket.get();
        if (bs == null) return bs;
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
        return bs;
    }

    @Override
    public void setBytes(final String key, final byte[] value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(value);
    }

    @Override
    public void setBytes(final int expireSeconds, final String key, final byte[] value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set(value);
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public <T> void setBytes(final String key, final Convert convert0, final Type type, final T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? convert : convert0).convertToBytes(type, value));
    }

    @Override
    public <T> void setBytes(final int expireSeconds, final String key, final Convert convert0, final Type type, final T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        bucket.set((convert0 == null ? convert : convert0).convertToBytes(type, value));
        bucket.expire(expireSeconds, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<byte[]> getBytesAsync(final String key) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync());
    }

    @Override
    public CompletableFuture<byte[]> getBytesAndRefreshAsync(final String key, final int expireSeconds) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.getAsync().thenCompose(bs -> bs == null ? CompletableFuture.completedFuture(null) : bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> bs)));
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(final String key, final byte[] value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(value));
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(final int expireSeconds, final String key, final byte[] value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync(value).thenCompose(r -> bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> null)));
    }

    @Override
    public <T> CompletableFuture<Void> setBytesAsync(final String key, final Convert convert0, final Type type, final T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? convert : convert0).convertToBytes(type, value)).thenApply(v -> null));
    }

    @Override
    public <T> CompletableFuture<Void> setBytesAsync(final int expireSeconds, final String key, final Convert convert0, final Type type, final T value) {
        final RBucket<byte[]> bucket = redisson.getBucket(key, org.redisson.client.codec.ByteArrayCodec.INSTANCE);
        return completableFuture(bucket.setAsync((convert0 == null ? convert : convert0).convertToBytes(type, value)).thenCompose(r -> bucket.expireAsync(expireSeconds, TimeUnit.SECONDS).thenApply(v -> null)));
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
        return (int) redisson.getKeys().count();
    }

    @Override
    public CompletableFuture<Integer> getKeySizeAsync() {
        return completableFuture(redisson.getKeys().countAsync().thenApply(r -> r.intValue()));
    }

    //--------------------- queryList ------------------------------  
    @Override
    public List<CacheEntry<Object>> queryList() {
        return queryListAsync().join();
    }

    @Override
    public CompletableFuture<List<CacheEntry<Object>>> queryListAsync() {
        return CompletableFuture.completedFuture(new ArrayList<>()); //不返回数据
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
