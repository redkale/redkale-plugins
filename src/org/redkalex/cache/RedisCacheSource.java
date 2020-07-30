/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache;

import java.io.*;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import javax.annotation.Resource;
import org.redkale.convert.Convert;
import org.redkale.convert.bson.BsonByteBufferWriter;
import org.redkale.convert.json.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.*;
import org.redkale.service.*;
import org.redkale.source.*;
import org.redkale.util.*;
import org.redkale.util.AnyValue.DefaultAnyValue;

/**
 * 详情见: https://redkale.org
 *
 * @param <V> Value
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@ResourceType(CacheSource.class)
public final class RedisCacheSource<V extends Object> extends AbstractService implements CacheSource<V>, Service, AutoCloseable, Resourcable {

    protected static final String UTF8_NAME = "UTF-8";

    protected static final Charset UTF8 = Charset.forName(UTF8_NAME);

    protected static final byte DOLLAR_BYTE = '$';

    protected static final byte ASTERISK_BYTE = '*';

    protected static final byte PLUS_BYTE = '+';

    protected static final byte MINUS_BYTE = '-';

    protected static final byte COLON_BYTE = ':';

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Resource
    protected JsonConvert defaultConvert;

    @Resource(name = "$_convert")
    protected JsonConvert convert;

    protected Type objValueType = String.class;

    protected Map<SocketAddress, byte[]> passwords;

    protected List<InetSocketAddress> nodeAddrs;

    protected int db;

    protected Transport transport;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = this.defaultConvert;
        if (conf == null) conf = new AnyValue.DefaultAnyValue();

        AnyValue prop = conf.getAnyValue("properties");
        if (prop != null) {
            String storeValueStr = prop.getValue("value-type");
            if (storeValueStr != null) {
                try {
                    this.initValueType(Thread.currentThread().getContextClassLoader().loadClass(storeValueStr));
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, this.getClass().getSimpleName() + " load key & value store class (" + storeValueStr + ") error", e);
                }
            }
        }
        final int bufferCapacity = conf.getIntValue("bufferCapacity", 8 * 1024);
        final int bufferPoolSize = conf.getIntValue("bufferPoolSize", Runtime.getRuntime().availableProcessors() * 8);
        final int threads = conf.getIntValue("threads", Runtime.getRuntime().availableProcessors() * 8);
        final int readTimeoutSeconds = conf.getIntValue("readTimeoutSeconds", TransportFactory.DEFAULT_READTIMEOUTSECONDS);
        final int writeTimeoutSeconds = conf.getIntValue("writeTimeoutSeconds", TransportFactory.DEFAULT_WRITETIMEOUTSECONDS);
        final List<InetSocketAddress> addresses = new ArrayList<>();
        Map<SocketAddress, byte[]> passwords0 = new HashMap<>();
        for (AnyValue node : conf.getAnyValues("node")) {
            InetSocketAddress addr = new InetSocketAddress(node.getValue("addr"), node.getIntValue("port"));
            addresses.add(addr);
            String password = node.getValue("password", "").trim();
            if (!password.isEmpty()) passwords0.put(addr, password.getBytes(UTF8));
            String db0 = node.getValue("db", "").trim();
            if (!db0.isEmpty()) this.db = Integer.valueOf(db0);
        }
        if (!passwords0.isEmpty()) this.passwords = passwords0;
        this.nodeAddrs = addresses;
        TransportFactory transportFactory = TransportFactory.create(threads, bufferPoolSize, bufferCapacity, readTimeoutSeconds, writeTimeoutSeconds);
        this.transport = transportFactory.createTransportTCP("Redis-Transport", null, addresses);
        this.transport.setSemaphore(new Semaphore(conf.getIntValue("maxconns", threads)));
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedisCacheSource.class.getSimpleName() + ": addrs=" + addresses + ", db=" + db);

    }

    @Override //ServiceLoader时判断配置是否符合当前实现类
    public boolean match(AnyValue config) {
        if (config == null) return false;
        AnyValue[] nodes = config.getAnyValues("node");
        if (nodes == null || nodes.length == 0) return false;
        for (AnyValue node : nodes) {
            if (node.getValue("addr") != null && node.getValue("port") != null) return true;
        }
        return false;
    }

    public void updateRemoteAddresses(final Collection<InetSocketAddress> addresses) {
        this.transport.updateRemoteAddresses(addresses);
    }

    @Override
    public final void initValueType(Type valueType) {
        this.objValueType = valueType == null ? String.class : valueType;
    }

    @Override
    public final void initTransient(boolean flag) {
    }

    @Override
    public final String getType() {
        return "redis";
    }

    public static void main(String[] args) throws Exception {
        DefaultAnyValue conf = new DefaultAnyValue().addValue("maxconns", "1");
        conf.addValue("node", new DefaultAnyValue().addValue("addr", "127.0.0.1").addValue("port", "6363"));
 
        RedisCacheSource source = new RedisCacheSource();
        source.defaultConvert = JsonFactory.root().getConvert();
        source.initValueType(String.class); //value用String类型
        source.init(conf);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 7788);

        System.out.println("------------------------------------");
        source.removeAsync("stritem1");
        source.removeAsync("stritem2");
        source.setStringAsync("stritem1", "value1");
        source.setStringAsync("stritem2", "value2");
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
        source.set(1000, "key1", "value1");
        source.set("key1", "value1");
        source.setString("keystr1", "strvalue1");
        source.setLong("keylong1", 333L);
        source.set("300", "4000");
        source.getAndRefresh("key1", 3500);
        System.out.println("[有值] 300 GET : " + source.get("300"));
        System.out.println("[有值] key1 GET : " + source.get("key1"));
        System.out.println("[无值] key2 GET : " + source.get("key2"));
        System.out.println("[有值] keystr1 GET : " + source.getString("keystr1"));
        System.out.println("[有值] keylong1 GET : " + source.getLong("keylong1", 0L));
        System.out.println("[有值] key1 EXISTS : " + source.exists("key1"));
        System.out.println("[无值] key2 EXISTS : " + source.exists("key2"));

        source.remove("keys3");
        source.appendListItem("keys3", "vals1");
        source.appendListItem("keys3", "vals2");
        System.out.println("-------- keys3 追加了两个值 --------");
        System.out.println("[两值] keys3 VALUES : " + source.getCollection("keys3"));
        System.out.println("[有值] keys3 EXISTS : " + source.exists("keys3"));
        source.removeListItem("keys3", "vals1");
        System.out.println("[一值] keys3 VALUES : " + source.getCollection("keys3"));
        source.getCollectionAndRefresh("keys3", 3000);

        source.remove("stringmap");
        source.appendSetItem("stringmap", JsonConvert.TYPE_MAP_STRING_STRING, Utility.ofMap("a", "aa", "b", "bb"));
        source.appendSetItem("stringmap", JsonConvert.TYPE_MAP_STRING_STRING, Utility.ofMap("c", "cc", "d", "dd"));
        System.out.println("[两值] stringmap VALUES : " + source.getCollectionAsync("stringmap", JsonConvert.TYPE_MAP_STRING_STRING).join());

        source.remove("sets3");
        source.remove("sets4");
        source.appendSetItem("sets3", "setvals1");
        source.appendSetItem("sets3", "setvals2");
        source.appendSetItem("sets3", "setvals1");
        source.appendSetItem("sets4", "setvals2");
        source.appendSetItem("sets4", "setvals1");
        System.out.println("[两值] sets3 VALUES : " + source.getCollection("sets3"));
        System.out.println("[有值] sets3 EXISTS : " + source.exists("sets3"));
        System.out.println("[有值] sets3-setvals2 EXISTSITEM : " + source.existsSetItem("sets3", "setvals2"));
        System.out.println("[有值] sets3-setvals3 EXISTSITEM : " + source.existsSetItem("sets3", "setvals3"));
        source.removeSetItem("sets3", "setvals1");
        System.out.println("[一值] sets3 VALUES : " + source.getCollection("sets3"));
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

        //h
        source.remove("hmap");
        source.hincr("hmap", "key1");
        System.out.println("hmap.key1 值 : " + source.hgetLong("hmap", "key1", -1));
        source.hmset("hmap", "key2", "haha", "key3", 333);
        source.hmset("hmap", "sm", (HashMap) Utility.ofMap("a", "aa", "b", "bb"));
        System.out.println("hmap.sm 值 : " + source.hget("hmap", "sm", JsonConvert.TYPE_MAP_STRING_STRING));
        System.out.println("hmap.[key1,key2,key3] 值 : " + source.hmget("hmap", String.class, "key1", "key2", "key3"));
        System.out.println("hmap.keys 值 : " + source.hkeys("hmap"));
        source.hremove("hmap", "key1", "key3");
        System.out.println("hmap.keys 值 : " + source.hkeys("hmap"));
        System.out.println("hmap.key2 值 : " + source.hgetString("hmap", "key2"));

        source.remove("hmaplong");
        source.hincr("hmaplong", "key1", 10);
        source.hsetLong("hmaplong", "key2", 30);
        System.out.println("hmaplong.所有值 : " + source.hmap("hmaplong", long.class, 0, 10));

        source.remove("hmapstr");
        source.hsetString("hmapstr", "key1", "str10");
        source.hsetString("hmapstr", "key2", null);
        System.out.println("hmapstr.所有值 : " + source.hmap("hmapstr", String.class, 0, 10));

        source.remove("hmapstrmap");
        source.hset("hmapstrmap", "key1", JsonConvert.TYPE_MAP_STRING_STRING, (HashMap) Utility.ofMap("ks11", "vv11"));
        source.hset("hmapstrmap", "key2", JsonConvert.TYPE_MAP_STRING_STRING, null);
        System.out.println("hmapstrmap.所有值 : " + source.hmap("hmapstrmap", JsonConvert.TYPE_MAP_STRING_STRING, 0, 10, "key2*"));

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
        System.out.println("------------------------------------");

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
        if (transport != null) transport.close();
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        return (CompletableFuture) send("EXISTS", null, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public boolean exists(String key) {
        return existsAsync(key).join();
    }

    //--------------------- get ------------------------------
    @Override
    public CompletableFuture<V> getAsync(String key) {
        return (CompletableFuture) send("GET", CacheEntryType.OBJECT, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        return (CompletableFuture) send("GET", CacheEntryType.OBJECT, type, key, key.getBytes(UTF8));
    }

    @Override
    public CompletableFuture<String> getStringAsync(String key) {
        return (CompletableFuture) send("GET", CacheEntryType.STRING, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public CompletableFuture<Long> getLongAsync(String key, long defValue) {
        return ((CompletableFuture) send("GET", CacheEntryType.LONG, (Type) null, key, key.getBytes(UTF8))).thenApplyAsync(v -> v == null ? defValue : v);
    }

    @Override
    public V get(String key) {
        return getAsync(key).join();
    }

    @Override
    public <T> T get(String key, final Type type) {
        return (T) getAsync(key, type).join();
    }

    @Override
    public String getString(String key) {
        return getStringAsync(key).join();
    }

    @Override
    public long getLong(String key, long defValue) {
        return getLongAsync(key, defValue).join();
    }

    //--------------------- getAndRefresh ------------------------------
    @Override
    public CompletableFuture<V> getAndRefreshAsync(String key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getAsync(key));
    }

    @Override
    public <T> CompletableFuture<T> getAndRefreshAsync(String key, int expireSeconds, final Type type) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getAsync(key, type));
    }

    @Override
    public V getAndRefresh(String key, final int expireSeconds) {
        return getAndRefreshAsync(key, expireSeconds).join();
    }

    @Override
    public <T> T getAndRefresh(String key, final int expireSeconds, final Type type) {
        return (T) getAndRefreshAsync(key, expireSeconds, type).join();
    }

    @Override
    public CompletableFuture<String> getStringAndRefreshAsync(String key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getAsync(key));
    }

    @Override
    public String getStringAndRefresh(String key, final int expireSeconds) {
        return getStringAndRefreshAsync(key, expireSeconds).join();
    }

    @Override
    public CompletableFuture<Long> getLongAndRefreshAsync(String key, int expireSeconds, long defValue) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getLongAsync(key, defValue));
    }

    @Override
    public long getLongAndRefresh(String key, final int expireSeconds, long defValue) {
        return getLongAndRefreshAsync(key, expireSeconds, defValue).join();
    }

    //--------------------- refresh ------------------------------
    @Override
    public CompletableFuture<Void> refreshAsync(String key, int expireSeconds) {
        return setExpireSecondsAsync(key, expireSeconds);
    }

    @Override
    public void refresh(String key, final int expireSeconds) {
        setExpireSeconds(key, expireSeconds);
    }

    //--------------------- set ------------------------------
    @Override
    public CompletableFuture<Void> setAsync(String key, V value) {
        return (CompletableFuture) send("SET", CacheEntryType.OBJECT, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert, T value) {
        return (CompletableFuture) send("SET", CacheEntryType.OBJECT, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, convert, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, final Type type, T value) {
        return (CompletableFuture) send("SET", CacheEntryType.OBJECT, type, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, type, value));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert, final Type type, T value) {
        return (CompletableFuture) send("SET", CacheEntryType.OBJECT, type, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, convert, type, value));
    }

    @Override
    public void set(final String key, V value) {
        setAsync(key, value).join();
    }

    @Override
    public <T> void set(final String key, final Convert convert, T value) {
        setAsync(key, convert, value).join();
    }

    @Override
    public <T> void set(final String key, final Type type, T value) {
        setAsync(key, type, value).join();
    }

    @Override
    public <T> void set(String key, final Convert convert, final Type type, T value) {
        setAsync(key, convert, type, value).join();
    }

    @Override
    public CompletableFuture<Void> setStringAsync(String key, String value) {
        return (CompletableFuture) send("SET", CacheEntryType.STRING, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Convert) null, (Type) null, value));
    }

    @Override
    public void setString(String key, String value) {
        setStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> setLongAsync(String key, long value) {
        return (CompletableFuture) send("SET", CacheEntryType.LONG, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Convert) null, (Type) null, value));
    }

    @Override
    public void setLong(String key, long value) {
        setLongAsync(key, value).join();
    }

    //--------------------- set ------------------------------    
    @Override
    public CompletableFuture<Void> setAsync(int expireSeconds, String key, V value) {
        return (CompletableFuture) setAsync(key, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, Convert convert, T value) {
        return (CompletableFuture) setAsync(key, convert, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, final Type type, T value) {
        return (CompletableFuture) setAsync(key, type, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, Convert convert, final Type type, T value) {
        return (CompletableFuture) setAsync(key, convert, type, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public void set(int expireSeconds, String key, V value) {
        setAsync(expireSeconds, key, value).join();
    }

    @Override
    public <T> void set(int expireSeconds, String key, Convert convert, T value) {
        setAsync(expireSeconds, key, convert, value).join();
    }

    @Override
    public <T> void set(int expireSeconds, String key, final Type type, T value) {
        setAsync(expireSeconds, key, type, value).join();
    }

    @Override
    public <T> void set(int expireSeconds, String key, Convert convert, final Type type, T value) {
        setAsync(expireSeconds, key, convert, type, value).join();
    }

    @Override
    public CompletableFuture<Void> setStringAsync(int expireSeconds, String key, String value) {
        return (CompletableFuture) setStringAsync(key, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public void setString(int expireSeconds, String key, String value) {
        setStringAsync(expireSeconds, key, value).join();
    }

    @Override
    public CompletableFuture<Void> setLongAsync(int expireSeconds, String key, long value) {
        return (CompletableFuture) setLongAsync(key, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public void setLong(int expireSeconds, String key, long value) {
        setLongAsync(expireSeconds, key, value).join();
    }

    //--------------------- setExpireSeconds ------------------------------    
    @Override
    public CompletableFuture<Void> setExpireSecondsAsync(String key, int expireSeconds) {
        return (CompletableFuture) send("EXPIRE", null, (Type) null, key, key.getBytes(UTF8), String.valueOf(expireSeconds).getBytes(UTF8));
    }

    @Override
    public void setExpireSeconds(String key, int expireSeconds) {
        setExpireSecondsAsync(key, expireSeconds).join();
    }

    //--------------------- remove ------------------------------    
    @Override
    public CompletableFuture<Integer> removeAsync(String key) {
        return (CompletableFuture) send("DEL", null, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public int remove(String key) {
        return removeAsync(key).join();
    }

    //--------------------- incr ------------------------------    
    @Override
    public long incr(final String key) {
        return incrAsync(key).join();
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return (CompletableFuture) send("INCR", CacheEntryType.ATOMIC, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public long incr(final String key, long num) {
        return incrAsync(key, num).join();
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key, long num) {
        return (CompletableFuture) send("INCRBY", CacheEntryType.ATOMIC, (Type) null, key, key.getBytes(UTF8), String.valueOf(num).getBytes(UTF8));
    }

    //--------------------- decr ------------------------------    
    @Override
    public long decr(final String key) {
        return decrAsync(key).join();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return (CompletableFuture) send("DECR", CacheEntryType.ATOMIC, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public long decr(final String key, long num) {
        return decrAsync(key, num).join();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key, long num) {
        return (CompletableFuture) send("DECRBY", CacheEntryType.ATOMIC, (Type) null, key, key.getBytes(UTF8), String.valueOf(num).getBytes(UTF8));
    }

    @Override
    public int hremove(final String key, String... fields) {
        return hremoveAsync(key, fields).join();
    }

    @Override
    public List<String> hkeys(final String key) {
        return hkeysAsync(key).join();
    }

    @Override
    public long hincr(final String key, String field) {
        return hincrAsync(key, field).join();
    }

    @Override
    public long hincr(final String key, String field, long num) {
        return hincrAsync(key, field, num).join();
    }

    @Override
    public long hdecr(final String key, String field) {
        return hdecrAsync(key, field).join();
    }

    @Override
    public long hdecr(final String key, String field, long num) {
        return hdecrAsync(key, field, num).join();
    }

    @Override
    public boolean hexists(final String key, String field) {
        return hexistsAsync(key, field).join();
    }

    @Override
    public <T> void hset(final String key, final String field, final Convert convert, final T value) {
        hsetAsync(key, field, convert, value).join();
    }

    @Override
    public <T> void hset(final String key, final String field, final Type type, final T value) {
        hsetAsync(key, field, type, value).join();
    }

    @Override
    public <T> void hset(final String key, final String field, final Convert convert, final Type type, final T value) {
        hsetAsync(key, field, convert, type, value).join();
    }

    @Override
    public void hsetString(final String key, final String field, final String value) {
        hsetStringAsync(key, field, value).join();
    }

    @Override
    public void hsetLong(final String key, final String field, final long value) {
        hsetLongAsync(key, field, value).join();
    }

    @Override
    public void hmset(final String key, final Serializable... values) {
        hmsetAsync(key, values).join();
    }

    @Override
    public List<Serializable> hmget(final String key, final Type type, final String... fields) {
        return hmgetAsync(key, type, fields).join();
    }

    @Override
    public <T> Map<String, T> hmap(final String key, final Type type, int offset, int limit, String pattern) {
        return (Map) hmapAsync(key, type, offset, limit, pattern).join();
    }

    @Override
    public <T> Map<String, T> hmap(final String key, final Type type, int offset, int limit) {
        return (Map) hmapAsync(key, type, offset, limit).join();
    }

    @Override
    public <T> T hget(final String key, final String field, final Type type) {
        return (T) hgetAsync(key, field, type).join();
    }

    @Override
    public String hgetString(final String key, final String field) {
        return hgetStringAsync(key, field).join();
    }

    @Override
    public long hgetLong(final String key, final String field, long defValue) {
        return hgetLongAsync(key, field, defValue).join();
    }

    @Override
    public CompletableFuture<Integer> hremoveAsync(final String key, String... fields) {
        byte[][] bs = new byte[fields.length + 1][];
        bs[0] = key.getBytes(UTF8);
        for (int i = 0; i < fields.length; i++) {
            bs[i + 1] = fields[i].getBytes(UTF8);
        }
        return (CompletableFuture) send("HDEL", CacheEntryType.MAP, (Type) null, key, bs);
    }

    @Override
    public CompletableFuture<List<String>> hkeysAsync(final String key) {
        return (CompletableFuture) send("HKEYS", CacheEntryType.MAP, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field) {
        return hincrAsync(key, field, 1);
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field, long num) {
        return (CompletableFuture) send("HINCRBY", CacheEntryType.MAP, (Type) null, key, key.getBytes(UTF8), field.getBytes(UTF8), String.valueOf(num).getBytes(UTF8));
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(final String key, String field) {
        return hincrAsync(key, field, -1);
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(final String key, String field, long num) {
        return hincrAsync(key, field, -num);
    }

    @Override
    public CompletableFuture<Boolean> hexistsAsync(final String key, String field) {
        return (CompletableFuture) send("HEXISTS", CacheEntryType.MAP, (Type) null, key, key.getBytes(UTF8), field.getBytes(UTF8));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert, final T value) {
        return (CompletableFuture) send("HSET", CacheEntryType.MAP, (Type) null, key, key.getBytes(UTF8), field.getBytes(UTF8), formatValue(CacheEntryType.MAP, convert, null, value));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Type type, final T value) {
        return (CompletableFuture) send("HSET", CacheEntryType.MAP, type, key, key.getBytes(UTF8), field.getBytes(UTF8), formatValue(CacheEntryType.MAP, null, type, value));
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert, final Type type, final T value) {
        return (CompletableFuture) send("HSET", CacheEntryType.MAP, type, key, key.getBytes(UTF8), field.getBytes(UTF8), formatValue(CacheEntryType.MAP, convert, type, value));
    }

    @Override
    public CompletableFuture<Void> hsetStringAsync(final String key, final String field, final String value) {
        return (CompletableFuture) send("HSET", CacheEntryType.MAP, (Type) null, key, key.getBytes(UTF8), field.getBytes(UTF8), formatValue(CacheEntryType.STRING, null, null, value));
    }

    @Override
    public CompletableFuture<Void> hsetLongAsync(final String key, final String field, final long value) {
        return (CompletableFuture) send("HSET", CacheEntryType.MAP, (Type) null, key, key.getBytes(UTF8), field.getBytes(UTF8), formatValue(CacheEntryType.LONG, null, null, value));
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Serializable... values) {
        byte[][] bs = new byte[values.length + 1][];
        bs[0] = key.getBytes(UTF8);
        for (int i = 0; i < values.length; i += 2) {
            bs[i + 1] = String.valueOf(values[i]).getBytes(UTF8);
            bs[i + 2] = formatValue(CacheEntryType.MAP, null, null, values[i + 1]);
        }
        return (CompletableFuture) send("HMSET", CacheEntryType.MAP, (Type) null, key, bs);
    }

    @Override
    public CompletableFuture<List<Serializable>> hmgetAsync(final String key, final Type type, final String... fields) {
        byte[][] bs = new byte[fields.length + 1][];
        bs[0] = key.getBytes(UTF8);
        for (int i = 0; i < fields.length; i++) {
            bs[i + 1] = fields[i].getBytes(UTF8);
        }
        return (CompletableFuture) send("HMGET", CacheEntryType.MAP, type, key, bs);
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(final String key, final Type type, int offset, int limit) {
        return hmapAsync(key, type, offset, limit, null);
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(final String key, final Type type, int offset, int limit, String pattern) {
        byte[][] bs = new byte[pattern == null || pattern.isEmpty() ? 4 : 6][limit];
        int index = -1;
        bs[++index] = key.getBytes(UTF8);
        bs[++index] = String.valueOf(offset).getBytes(UTF8);
        if (pattern != null && !pattern.isEmpty()) {
            bs[++index] = "MATCH".getBytes(UTF8);
            bs[++index] = pattern.getBytes(UTF8);
        }
        bs[++index] = "COUNT".getBytes(UTF8);
        bs[++index] = String.valueOf(limit).getBytes(UTF8);
        return (CompletableFuture) send("HSCAN", CacheEntryType.MAP, type, key, bs);
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(final String key, final String field, final Type type) {
        return (CompletableFuture) send("HGET", CacheEntryType.OBJECT, type, key, key.getBytes(UTF8), field.getBytes(UTF8));
    }

    @Override
    public CompletableFuture<String> hgetStringAsync(final String key, final String field) {
        return (CompletableFuture) send("HGET", CacheEntryType.STRING, (Type) null, key, key.getBytes(UTF8), field.getBytes(UTF8));
    }

    @Override
    public CompletableFuture<Long> hgetLongAsync(final String key, final String field, long defValue) {
        return (CompletableFuture) send("HGET", CacheEntryType.LONG, (Type) null, key, key.getBytes(UTF8), field.getBytes(UTF8)).thenApplyAsync(v -> v == null ? defValue : v);
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Integer> getCollectionSizeAsync(String key) {
        return (CompletableFuture) send("OBJECT", null, (Type) null, key, "ENCODING".getBytes(UTF8), key.getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LLEN", null, (Type) null, key, key.getBytes(UTF8));
            } else {
                return send("SCARD", null, (Type) null, key, key.getBytes(UTF8));
            }
        });
    }

    @Override
    public int getCollectionSize(String key) {
        return getCollectionSizeAsync(key).join();
    }

    @Override
    public CompletableFuture<Collection<V>> getCollectionAsync(String key) {
        return (CompletableFuture) send("OBJECT", null, (Type) null, key, "ENCODING".getBytes(UTF8), key.getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LRANGE", CacheEntryType.OBJECT, (Type) null, false, key, key.getBytes(UTF8), new byte[]{'0'}, new byte[]{'-', '1'});
            } else {
                return send("SMEMBERS", CacheEntryType.OBJECT, (Type) null, true, key, key.getBytes(UTF8));
            }
        });
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return (CompletableFuture) send("OBJECT", null, componentType, key, "ENCODING".getBytes(UTF8), key.getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LRANGE", CacheEntryType.OBJECT, componentType, false, key, key.getBytes(UTF8), new byte[]{'0'}, new byte[]{'-', '1'});
            } else {
                return send("SMEMBERS", CacheEntryType.OBJECT, componentType, true, key, key.getBytes(UTF8));
            }
        });
    }

    @Override
    public CompletableFuture<Map<String, Long>> getLongMapAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(UTF8);
        }
        return (CompletableFuture) send("MGET", CacheEntryType.LONG, null, false, keys[0], bs).thenApply(r -> {
            List list = (List) r;
            Map map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                if (obj != null) map.put(keys[i], list.get(i));
            }
            return map;
        });
    }

    @Override
    public CompletableFuture<Long[]> getLongArrayAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(UTF8);
        }
        return (CompletableFuture) send("MGET", CacheEntryType.LONG, null, false, keys[0], bs).thenApply(r -> {
            List list = (List) r;
            Long[] rs = new Long[keys.length];
            for (int i = 0; i < keys.length; i++) {
                Number obj = (Number) list.get(i);
                rs[i] = obj == null ? null : obj.longValue();
            }
            return rs;
        });
    }

    @Override
    public CompletableFuture<String[]> getStringArrayAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(UTF8);
        }
        return (CompletableFuture) send("MGET", CacheEntryType.STRING, null, false, keys[0], bs).thenApply(r -> {
            List list = (List) r;
            String[] rs = new String[keys.length];
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                rs[i] = obj == null ? null : obj.toString();
            }
            return rs;
        });
    }

    @Override
    public CompletableFuture<Map<String, String>> getStringMapAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(UTF8);
        }
        return (CompletableFuture) send("MGET", CacheEntryType.STRING, null, false, keys[0], bs).thenApply(r -> {
            List list = (List) r;
            Map map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                if (obj != null) map.put(keys[i], list.get(i));
            }
            return map;
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> getMapAsync(final Type componentType, String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(UTF8);
        }
        return (CompletableFuture) send("MGET", CacheEntryType.OBJECT, componentType, false, keys[0], bs).thenApply(r -> {
            List list = (List) r;
            Map map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                if (obj != null) map.put(keys[i], list.get(i));
            }
            return map;
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(final boolean set, final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Collection<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<T>> map = new HashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list        
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = send("LRANGE", CacheEntryType.OBJECT, componentType, false, key, key.getBytes(UTF8), new byte[]{'0'}, new byte[]{'-', '1'}).thenAccept(c -> {
                    if (c != null) {
                        synchronized (map) {
                            map.put(key, (Collection) c);
                        }
                    }
                });
            }
        } else {
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = send("SMEMBERS", CacheEntryType.OBJECT, componentType, true, key, key.getBytes(UTF8)).thenAccept(c -> {
                    if (c != null) {
                        synchronized (map) {
                            map.put(key, (Collection) c);
                        }
                    }
                });
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
    public Collection<V> getCollection(String key) {
        return getCollectionAsync(key).join();
    }

    @Override
    public <T> Collection<T> getCollection(String key, final Type componentType) {
        return (Collection) getCollectionAsync(key, componentType).join();
    }

    @Override
    public Map<String, Long> getLongMap(final String... keys) {
        return getLongMapAsync(keys).join();
    }

    @Override
    public Long[] getLongArray(final String... keys) {
        return getLongArrayAsync(keys).join();
    }

    @Override
    public Map<String, String> getStringMap(final String... keys) {
        return getStringMapAsync(keys).join();
    }

    @Override
    public String[] getStringArray(final String... keys) {
        return getStringArrayAsync(keys).join();
    }

    @Override
    public <T> Map<String, T> getMap(final Type componentType, final String... keys) {
        return (Map) getMapAsync(componentType, keys).join();
    }

    @Override
    public <T> Map<String, Collection<T>> getCollectionMap(final boolean set, final Type componentType, String... keys) {
        return (Map) getCollectionMapAsync(set, componentType, keys).join();
    }

    @Override
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return (CompletableFuture) send("OBJECT", null, (Type) null, key, "ENCODING".getBytes(UTF8), key.getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LRANGE", CacheEntryType.STRING, (Type) null, false, key, key.getBytes(UTF8), new byte[]{'0'}, new byte[]{'-', '1'});
            } else {
                return send("SMEMBERS", CacheEntryType.STRING, (Type) null, true, key, key.getBytes(UTF8));
            }
        });
    }

    @Override
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<String>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<String>> map = new HashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list        
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = send("LRANGE", CacheEntryType.STRING, (Type) null, false, key, key.getBytes(UTF8), new byte[]{'0'}, new byte[]{'-', '1'}).thenAccept(c -> {
                    if (c != null) {
                        synchronized (map) {
                            map.put(key, (Collection) c);
                        }
                    }
                });
            }
        } else {
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = send("SMEMBERS", CacheEntryType.STRING, (Type) null, true, key, key.getBytes(UTF8)).thenAccept(c -> {
                    if (c != null) {
                        synchronized (map) {
                            map.put(key, (Collection) c);
                        }
                    }
                });
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
        return (CompletableFuture) send("OBJECT", null, (Type) null, key, "ENCODING".getBytes(UTF8), key.getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LRANGE", CacheEntryType.LONG, (Type) null, false, key, key.getBytes(UTF8), new byte[]{'0'}, new byte[]{'-', '1'});
            } else {
                return send("SMEMBERS", CacheEntryType.LONG, (Type) null, true, key, key.getBytes(UTF8));
            }
        });
    }

    @Override
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<Long>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<Long>> map = new HashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        if (!set) { //list        
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = send("LRANGE", CacheEntryType.LONG, (Type) null, false, key, key.getBytes(UTF8), new byte[]{'0'}, new byte[]{'-', '1'}).thenAccept(c -> {
                    if (c != null) {
                        synchronized (map) {
                            map.put(key, (Collection) c);
                        }
                    }
                });
            }
        } else {
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = send("SMEMBERS", CacheEntryType.LONG, (Type) null, true, key, key.getBytes(UTF8)).thenAccept(c -> {
                    if (c != null) {
                        synchronized (map) {
                            map.put(key, (Collection) c);
                        }
                    }
                });
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
    public CompletableFuture<Collection<V>> getCollectionAndRefreshAsync(String key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key));
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAndRefreshAsync(String key, int expireSeconds, final Type componentType) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key, componentType));
    }

    @Override
    public Collection<V> getCollectionAndRefresh(String key, final int expireSeconds) {
        return getCollectionAndRefreshAsync(key, expireSeconds).join();
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
    public boolean existsSetItem(String key, V value) {
        return existsSetItemAsync(key, value).join();
    }

    @Override
    public <T> boolean existsSetItem(String key, final Type componentType, T value) {
        return existsSetItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Boolean> existsSetItemAsync(String key, V value) {
        return (CompletableFuture) send("SISMEMBER", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Boolean> existsSetItemAsync(String key, final Type componentType, T value) {
        return (CompletableFuture) send("SISMEMBER", null, componentType, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, componentType, value));
    }

    @Override
    public boolean existsStringSetItem(String key, String value) {
        return existsStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> existsStringSetItemAsync(String key, String value) {
        return (CompletableFuture) send("SISMEMBER", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Convert) null, (Type) null, value));
    }

    @Override
    public boolean existsLongSetItem(String key, long value) {
        return existsLongSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> existsLongSetItemAsync(String key, long value) {
        return (CompletableFuture) send("SISMEMBER", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Convert) null, (Type) null, value));
    }

    //--------------------- appendListItem ------------------------------  
    @Override
    public CompletableFuture<Void> appendListItemAsync(String key, V value) {
        return (CompletableFuture) send("RPUSH", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> appendListItemAsync(String key, final Type componentType, T value) {
        return (CompletableFuture) send("RPUSH", null, componentType, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, componentType, value));
    }

    @Override
    public void appendListItem(String key, V value) {
        appendListItemAsync(key, value).join();
    }

    @Override
    public <T> void appendListItem(String key, final Type componentType, T value) {
        appendListItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Void> appendStringListItemAsync(String key, String value) {
        return (CompletableFuture) send("RPUSH", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Convert) null, (Type) null, value));
    }

    @Override
    public void appendStringListItem(String key, String value) {
        appendStringListItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> appendLongListItemAsync(String key, long value) {
        return (CompletableFuture) send("RPUSH", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Convert) null, (Type) null, value));
    }

    @Override
    public void appendLongListItem(String key, long value) {
        appendLongListItemAsync(key, value).join();
    }

    //--------------------- removeListItem ------------------------------  
    @Override
    public CompletableFuture<Integer> removeListItemAsync(String key, V value) {
        return (CompletableFuture) send("LREM", null, (Type) null, key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(CacheEntryType.OBJECT, (Convert) null, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Integer> removeListItemAsync(String key, final Type componentType, T value) {
        return (CompletableFuture) send("LREM", null, componentType, key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(CacheEntryType.OBJECT, (Convert) null, componentType, value));
    }

    @Override
    public int removeListItem(String key, V value) {
        return removeListItemAsync(key, value).join();
    }

    @Override
    public <T> int removeListItem(String key, final Type componentType, T value) {
        return removeListItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Integer> removeStringListItemAsync(String key, String value) {
        return (CompletableFuture) send("LREM", null, (Type) null, key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(CacheEntryType.STRING, (Convert) null, (Type) null, value));
    }

    @Override
    public int removeStringListItem(String key, String value) {
        return removeStringListItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Integer> removeLongListItemAsync(String key, long value) {
        return (CompletableFuture) send("LREM", null, (Type) null, key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(CacheEntryType.LONG, (Convert) null, (Type) null, value));
    }

    @Override
    public int removeLongListItem(String key, long value) {
        return removeLongListItemAsync(key, value).join();
    }

    //--------------------- appendSetItem ------------------------------  
    @Override
    public CompletableFuture<Void> appendSetItemAsync(String key, V value) {
        return (CompletableFuture) send("SADD", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> appendSetItemAsync(String key, Type componentType, T value) {
        return (CompletableFuture) send("SADD", null, componentType, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, componentType, value));
    }

    @Override
    public <T> CompletableFuture<T> spopSetItemAsync(String key, Type componentType) {
        return (CompletableFuture) send("SPOP", CacheEntryType.OBJECT, componentType, key, key.getBytes(UTF8));
    }

    @Override
    public <T> CompletableFuture<List<T>> spopSetItemAsync(String key, int count, Type componentType) {
        return (CompletableFuture) send("SPOP", CacheEntryType.OBJECT, componentType, key, key.getBytes(UTF8), String.valueOf(count).getBytes(UTF8));
    }

    @Override
    public CompletableFuture<String> spopStringSetItemAsync(String key) {
        return (CompletableFuture) send("SPOP", CacheEntryType.STRING, String.class, key, key.getBytes(UTF8));
    }

    @Override
    public CompletableFuture<List<String>> spopStringSetItemAsync(String key, int count) {
        return (CompletableFuture) send("SPOP", CacheEntryType.STRING, String.class, key, key.getBytes(UTF8), String.valueOf(count).getBytes(UTF8));
    }

    @Override
    public CompletableFuture<Long> spopLongSetItemAsync(String key) {
        return (CompletableFuture) send("SPOP", CacheEntryType.LONG, long.class, key, key.getBytes(UTF8));
    }

    @Override
    public CompletableFuture<List<Long>> spopLongSetItemAsync(String key, int count) {
        return (CompletableFuture) send("SPOP", CacheEntryType.LONG, long.class, key, key.getBytes(UTF8), String.valueOf(count).getBytes(UTF8));
    }

    @Override
    public void appendSetItem(String key, V value) {
        appendSetItemAsync(key, value).join();
    }

    @Override
    public <T> void appendSetItem(String key, final Type componentType, T value) {
        appendSetItemAsync(key, componentType, value).join();
    }

    @Override
    public <T> T spopSetItem(String key, final Type componentType) {
        return (T) spopSetItemAsync(key, componentType).join();
    }

    @Override
    public <T> List<T> spopSetItem(String key, int count, final Type componentType) {
        return (List) spopSetItemAsync(key, count, componentType).join();
    }

    @Override
    public String spopStringSetItem(String key) {
        return spopStringSetItemAsync(key).join();
    }

    @Override
    public List<String> spopStringSetItem(String key, int count) {
        return spopStringSetItemAsync(key, count).join();
    }

    @Override
    public Long spopLongSetItem(String key) {
        return spopLongSetItemAsync(key).join();
    }

    @Override
    public List<Long> spopLongSetItem(String key, int count) {
        return spopLongSetItemAsync(key, count).join();
    }

    @Override
    public CompletableFuture<Void> appendStringSetItemAsync(String key, String value) {
        return (CompletableFuture) send("SADD", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Convert) null, (Type) null, value));
    }

    @Override
    public void appendStringSetItem(String key, String value) {
        appendStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> appendLongSetItemAsync(String key, long value) {
        return (CompletableFuture) send("SADD", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Convert) null, (Type) null, value));
    }

    @Override
    public void appendLongSetItem(String key, long value) {
        appendLongSetItemAsync(key, value).join();
    }

    //--------------------- removeSetItem ------------------------------  
    @Override
    public CompletableFuture<Integer> removeSetItemAsync(String key, V value) {
        return (CompletableFuture) send("SREM", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Integer> removeSetItemAsync(String key, final Type componentType, T value) {
        return (CompletableFuture) send("SREM", null, componentType, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Convert) null, componentType, value));
    }

    @Override
    public int removeSetItem(String key, V value) {
        return removeSetItemAsync(key, value).join();
    }

    @Override
    public <T> int removeSetItem(String key, final Type componentType, T value) {
        return removeSetItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Integer> removeStringSetItemAsync(String key, String value) {
        return (CompletableFuture) send("SREM", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Convert) null, (Type) null, value));
    }

    @Override
    public int removeStringSetItem(String key, String value) {
        return removeStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Integer> removeLongSetItemAsync(String key, long value) {
        return (CompletableFuture) send("SREM", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Convert) null, (Type) null, value));
    }

    @Override
    public int removeLongSetItem(String key, long value) {
        return removeLongSetItemAsync(key, value).join();
    }

    //--------------------- queryKeys ------------------------------  
    @Override
    public List<String> queryKeys() {
        return queryKeysAsync().join();
    }

    @Override
    public List<String> queryKeysStartsWith(String startsWith) {
        return queryKeysStartsWithAsync(startsWith).join();
    }

    @Override
    public List<String> queryKeysEndsWith(String endsWith) {
        return queryKeysEndsWithAsync(endsWith).join();
    }

    @Override
    public CompletableFuture<List<String>> queryKeysAsync() {
        return (CompletableFuture) send("KEYS", null, (Type) null, "*", new byte[]{(byte) '*'});
    }

    @Override
    public CompletableFuture<List<String>> queryKeysStartsWithAsync(String startsWith) {
        if (startsWith == null) return queryKeysAsync();
        String key = startsWith + "*";
        return (CompletableFuture) send("KEYS", null, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public CompletableFuture<List<String>> queryKeysEndsWithAsync(String endsWith) {
        if (endsWith == null) return queryKeysAsync();
        String key = "*" + endsWith;
        return (CompletableFuture) send("KEYS", null, (Type) null, key, key.getBytes(UTF8));
    }

    //--------------------- getKeySize ------------------------------  
    @Override
    public int getKeySize() {
        return getKeySizeAsync().join();
    }

    @Override
    public CompletableFuture<Integer> getKeySizeAsync() {
        return (CompletableFuture) send("DBSIZE", null, (Type) null, null);
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

    //--------------------- send ------------------------------  
    private byte[] formatValue(CacheEntryType cacheType, Convert convert0, Type resultType, Object value) {
        if (value == null) return "null".getBytes(UTF8);
        if (convert0 == null) convert0 = convert;
        if (cacheType == CacheEntryType.MAP) {
            if ((value instanceof CharSequence) || (value instanceof Number)) {
                return String.valueOf(value).getBytes(UTF8);
            }
            if (objValueType == String.class && !(value instanceof CharSequence)) resultType = value.getClass();
            return convert0.convertToBytes(resultType == null ? objValueType : resultType, value);
        }
        if (cacheType == CacheEntryType.LONG || cacheType == CacheEntryType.ATOMIC) return String.valueOf(value).getBytes(UTF8);
        if (cacheType == CacheEntryType.STRING) return convert0.convertToBytes(String.class, value);
        return convert0.convertToBytes(resultType == null ? objValueType : resultType, value);
    }

    private CompletableFuture<Serializable> send(final String command, final CacheEntryType cacheType, final Type resultType, final String key, final byte[]... args) {
        return send(command, cacheType, resultType, false, key, args);
    }

    private CompletableFuture<Serializable> send(final String command, final CacheEntryType cacheType, final Type resultType, final boolean set, final String key, final byte[]... args) {
        return send(null, command, cacheType, resultType, set, key, args);
    }

    private CompletableFuture<Serializable> send(final CompletionHandler callback, final String command, final CacheEntryType cacheType, final Type resultType, final boolean set, final String key, final byte[]... args) {
        final BsonByteBufferWriter writer = new BsonByteBufferWriter(transport.getBufferSupplier());
        writer.writeTo(ASTERISK_BYTE);
        writer.writeTo(String.valueOf(args.length + 1).getBytes(UTF8));
        writer.writeTo((byte) '\r', (byte) '\n');
        writer.writeTo(DOLLAR_BYTE);
        writer.writeTo(String.valueOf(command.length()).getBytes(UTF8));
        writer.writeTo((byte) '\r', (byte) '\n');
        writer.writeTo(command.getBytes(UTF8));
        writer.writeTo((byte) '\r', (byte) '\n');

        for (final byte[] arg : args) {
            writer.writeTo(DOLLAR_BYTE);
            writer.writeTo(String.valueOf(arg.length).getBytes(UTF8));
            writer.writeTo((byte) '\r', (byte) '\n');
            writer.writeTo(arg);
            writer.writeTo((byte) '\r', (byte) '\n');
        }

        final ByteBuffer[] buffers = writer.toBuffers();

        final CompletableFuture<Serializable> future = callback == null ? new CompletableFuture<>() : null;
        CompletableFuture<AsyncConnection> connFuture = this.transport.pollConnection(null);
        if (passwords != null) {
            connFuture = connFuture.thenCompose(conn -> {
                if (conn.getSubobject() != null) return CompletableFuture.completedFuture(conn);
                byte[] password = passwords.get(conn.getRemoteAddress());
                if (password == null) return CompletableFuture.completedFuture(conn);
                CompletableFuture<AsyncConnection> rs = auth(conn, password, command);
                if (db > 0) {
                    rs = rs.thenCompose(conn2 -> {
                        if (conn2.getSubobject() != null) return CompletableFuture.completedFuture(conn2);
                        return selectdb(conn2, db, command);
                    });
                }
                return rs;
            });
        } else if (db > 0) {
            connFuture = connFuture.thenCompose(conn2 -> {
                if (conn2.getSubobject() != null) return CompletableFuture.completedFuture(conn2);
                return selectdb(conn2, db, command);
            });
        }
        connFuture.whenComplete((conn, ex) -> {
            if (ex != null) {
                transport.offerBuffer(buffers);
                if (future == null) {
                    callback.failed(ex, null);
                } else {
                    future.completeExceptionally(ex);
                }
                return;
            }
            conn.write(buffers, buffers, new CompletionHandler<Integer, ByteBuffer[]>() {
                @Override
                public void completed(Integer result, ByteBuffer[] attachments) {
                    int index = -1;
                    try {
                        for (int i = 0; i < attachments.length; i++) {
                            if (attachments[i].hasRemaining()) {
                                index = i;
                                break;
                            } else {
                                transport.offerBuffer(attachments[i]);
                            }
                        }
                        if (index == 0) {
                            conn.write(attachments, attachments, this);
                            return;
                        } else if (index > 0) {
                            ByteBuffer[] newattachs = new ByteBuffer[attachments.length - index];
                            System.arraycopy(attachments, index, newattachs, 0, newattachs.length);
                            conn.write(newattachs, newattachs, this);
                            return;
                        }
                        //----------------------- 读取返回结果 -------------------------------------
                        conn.read(new ReplyCompletionHandler(conn) {
                            @Override
                            public void completed(Integer result, ByteBuffer buffer) {
                                buffer.flip();
                                try {
                                    final byte sign = buffer.get();
                                    if (sign == PLUS_BYTE) { // +
                                        byte[] bs = readBytes(buffer);
                                        if (future == null) {
                                            transport.offerConnection(false, conn); //必须在complete之前，防止并发是conn还没回收完毕
                                            callback.completed(null, key);
                                        } else {
                                            transport.offerConnection(false, conn);
                                            future.complete(("SET".equals(command) || "HSET".equals(command)) ? null : bs);
                                        }
                                    } else if (sign == MINUS_BYTE) { // -
                                        String bs = readString(buffer);
                                        if (future == null) {
                                            transport.offerConnection(false, conn);
                                            callback.failed(new RuntimeException(bs), key);
                                        } else {
                                            transport.offerConnection(false, conn);
                                            future.completeExceptionally(new RuntimeException("command : " + command + ", error: " + bs));
                                        }
                                    } else if (sign == COLON_BYTE) { // :
                                        long rs = readLong(buffer);
                                        if (future == null) {
                                            if (command.startsWith("INCR") || command.startsWith("DECR") || command.startsWith("HINCR")) {
                                                transport.offerConnection(false, conn);
                                                callback.completed(rs, key);
                                            } else {
                                                transport.offerConnection(false, conn);
                                                callback.completed(("EXISTS".equals(command) || "SISMEMBER".equals(command)) ? (rs > 0) : (("LLEN".equals(command) || "SCARD".equals(command) || "SREM".equals(command) || "LREM".equals(command) || "DEL".equals(command) || "HDEL".equals(command) || "DBSIZE".equals(command)) ? (int) rs : null), key);
                                            }
                                        } else {
                                            if (command.startsWith("INCR") || command.startsWith("DECR") || command.startsWith("HINCR") || command.startsWith("HGET")) {
                                                transport.offerConnection(false, conn);
                                                future.complete(rs);
                                            } else {
                                                transport.offerConnection(false, conn);
                                                future.complete(("EXISTS".equals(command) || "SISMEMBER".equals(command)) ? (rs > 0) : (("LLEN".equals(command) || "SCARD".equals(command) || "SREM".equals(command) || "LREM".equals(command) || "DEL".equals(command) || "HDEL".equals(command) || "DBSIZE".equals(command)) ? (int) rs : null));
                                            }
                                        }
                                    } else if (sign == DOLLAR_BYTE) { // $
                                        long val = readLong(buffer);
                                        byte[] rs = val <= 0 ? null : readBytes(buffer);
                                        Type ct = cacheType == CacheEntryType.LONG ? long.class : (cacheType == CacheEntryType.STRING ? String.class : (resultType == null ? objValueType : resultType));
                                        if (future == null) {
                                            transport.offerConnection(false, conn);
                                            callback.completed((("SPOP".equals(command) || command.endsWith("GET") || rs == null) ? (ct == String.class && rs != null ? new String(rs, UTF8) : convert.convertFrom(ct, new String(rs, UTF8))) : null), key);
                                        } else {
                                            transport.offerConnection(false, conn);
                                            future.complete(("SPOP".equals(command) || command.endsWith("GET") ? (ct == String.class && rs != null ? new String(rs, UTF8) : convert.convertFrom(ct, rs == null ? null : new String(rs, UTF8))) : rs));
                                        }
                                    } else if (sign == ASTERISK_BYTE) { // *
                                        final int len = readInt(buffer);
                                        if (len < 0) {
                                            if (future == null) {
                                                transport.offerConnection(false, conn);
                                                callback.completed(null, key);
                                            } else {
                                                transport.offerConnection(false, conn);
                                                future.complete((byte[]) null);
                                            }
                                        } else {
                                            Object rsobj;
                                            if (command.endsWith("SCAN")) {
                                                LinkedHashMap rs = new LinkedHashMap();
                                                rsobj = rs;
                                                for (int i = 0; i < len; i++) {
                                                    int l = readInt(buffer);
                                                    if (l > 0) {
                                                        readBytes(buffer);
                                                    } else {
                                                        while (buffer.hasRemaining()) {
                                                            readBytes(buffer);
                                                            String field = new String(readBytes(buffer), UTF8);
                                                            String value = null;
                                                            if (readInt(buffer) > 0) {
                                                                value = new String(readBytes(buffer), UTF8);
                                                            }
                                                            Type ct = cacheType == CacheEntryType.LONG ? long.class : (cacheType == CacheEntryType.STRING ? String.class : (resultType == null ? objValueType : resultType));
                                                            rs.put(field, value == null ? null : (ct == String.class ? value : convert.convertFrom(ct, value)));
                                                        }
                                                    }
                                                }
                                            } else {
                                                Collection rs = set ? new HashSet() : new ArrayList();
                                                rsobj = rs;
                                                boolean keys = "KEYS".equals(command) || "HKEYS".equals(command);
                                                boolean mget = !keys && ("MGET".equals(command) || "HMGET".equals(command));
                                                Type ct = cacheType == CacheEntryType.LONG ? long.class : (cacheType == CacheEntryType.STRING ? String.class : (resultType == null ? objValueType : resultType));
                                                for (int i = 0; i < len; i++) {
                                                    int l = readInt(buffer);
                                                    if (l > 0) {
                                                        rs.add(keys ? new String(readBytes(buffer), UTF8) : (ct == String.class ? new String(readBytes(buffer), UTF8) : convert.convertFrom(ct, new String(readBytes(buffer), UTF8))));
                                                    } else if (mget) {
                                                        rs.add(null);
                                                    }
                                                }
                                            }
                                            if (future == null) {
                                                transport.offerConnection(false, conn);
                                                callback.completed(rsobj, key);
                                            } else {
                                                transport.offerConnection(false, conn);
                                                future.complete((Serializable) rsobj);
                                            }
                                        }
                                    } else {
                                        String exstr = "Unknown reply: " + (char) sign;
                                        if (future == null) {
                                            transport.offerConnection(false, conn);
                                            callback.failed(new RuntimeException(exstr), key);
                                        } else {
                                            transport.offerConnection(false, conn);
                                            future.completeExceptionally(new RuntimeException(exstr));
                                        }
                                    }
                                } catch (Exception e) {
                                    failed(e, buffer);
                                }
                            }

                            @Override
                            public void failed(Throwable exc, ByteBuffer attachment) {
                                conn.offerBuffer(attachment);
                                transport.offerConnection(true, conn);
                                if (future == null) {
                                    callback.failed(exc, attachments);
                                } else {
                                    future.completeExceptionally(exc);
                                }
                            }

                        });
                    } catch (Exception e) {
                        failed(e, attachments);
                    }
                }

                @Override
                public void failed(Throwable exc, ByteBuffer[] attachments) {
                    transport.offerConnection(true, conn);
                    if (future == null) {
                        callback.failed(exc, attachments);
                    } else {
                        future.completeExceptionally(exc);
                    }
                }
            });
        });
        return future; //.orTimeout(3, TimeUnit.SECONDS)  JDK9以上才支持
    }

    private CompletableFuture<AsyncConnection> selectdb(final AsyncConnection conn, final int db, final String command) {
        final CompletableFuture<AsyncConnection> rsfuture = new CompletableFuture();
        try {
            final BsonByteBufferWriter dbwriter = new BsonByteBufferWriter(transport.getBufferSupplier());
            dbwriter.writeTo(ASTERISK_BYTE);
            dbwriter.writeTo((byte) '2');
            dbwriter.writeTo((byte) '\r', (byte) '\n');
            dbwriter.writeTo(DOLLAR_BYTE);
            dbwriter.writeTo((byte) '6');
            dbwriter.writeTo((byte) '\r', (byte) '\n');
            dbwriter.writeTo("SELECT".getBytes(UTF8));
            dbwriter.writeTo((byte) '\r', (byte) '\n');

            dbwriter.writeTo(DOLLAR_BYTE);
            dbwriter.writeTo(String.valueOf(String.valueOf(db).length()).getBytes(UTF8));
            dbwriter.writeTo((byte) '\r', (byte) '\n');
            dbwriter.writeTo(String.valueOf(db).getBytes(UTF8));
            dbwriter.writeTo((byte) '\r', (byte) '\n');

            final ByteBuffer[] authbuffers = dbwriter.toBuffers();
            conn.write(authbuffers, authbuffers, new CompletionHandler<Integer, ByteBuffer[]>() {
                @Override
                public void completed(Integer result, ByteBuffer[] attachments) {
                    int index = -1;
                    try {
                        for (int i = 0; i < attachments.length; i++) {
                            if (attachments[i].hasRemaining()) {
                                index = i;
                                break;
                            } else {
                                transport.offerBuffer(attachments[i]);
                            }
                        }
                        if (index == 0) {
                            conn.write(attachments, attachments, this);
                            return;
                        } else if (index > 0) {
                            ByteBuffer[] newattachs = new ByteBuffer[attachments.length - index];
                            System.arraycopy(attachments, index, newattachs, 0, newattachs.length);
                            conn.write(newattachs, newattachs, this);
                            return;
                        }
                        //----------------------- 读取返回结果 -------------------------------------
                        conn.read(new ReplyCompletionHandler(conn) {
                            @Override
                            public void completed(Integer result, ByteBuffer buffer) {
                                buffer.flip();
                                try {
                                    final byte sign = buffer.get();
                                    if (sign == PLUS_BYTE) { // +
                                        byte[] bs = readBytes(buffer);
                                        if ("OK".equalsIgnoreCase(new String(bs))) {
                                            conn.setSubobject("authed+db");
                                            rsfuture.complete(conn);
                                        } else {
                                            transport.offerConnection(false, conn);
                                            rsfuture.completeExceptionally(new RuntimeException("command : " + command + ", error: " + bs));
                                        }
                                    } else if (sign == MINUS_BYTE) { // -   异常
                                        String bs = readString(buffer);
                                        transport.offerConnection(false, conn);
                                        rsfuture.completeExceptionally(new RuntimeException("command : " + command + ", error: " + bs));
                                    } else {
                                        String exstr = "Unknown reply: " + (char) sign;
                                        transport.offerConnection(false, conn);
                                        rsfuture.completeExceptionally(new RuntimeException(exstr));
                                    }
                                } catch (Exception e) {
                                    failed(e, buffer);
                                }
                            }

                            @Override
                            public void failed(Throwable exc, ByteBuffer buffer) {
                                conn.offerBuffer(buffer);
                                transport.offerConnection(true, conn);
                                rsfuture.completeExceptionally(exc);
                            }

                        });
                    } catch (Exception e) {
                        failed(e, attachments);
                    }
                }

                @Override
                public void failed(Throwable exc, ByteBuffer[] attachments) {
                    transport.offerConnection(true, conn);
                    rsfuture.completeExceptionally(exc);
                }
            });
        } catch (Exception e) {
            rsfuture.completeExceptionally(e);
        }
        return rsfuture;
    }

    private CompletableFuture<AsyncConnection> auth(final AsyncConnection conn, final byte[] password, final String command) {
        final CompletableFuture<AsyncConnection> rsfuture = new CompletableFuture();
        try {
            final BsonByteBufferWriter authwriter = new BsonByteBufferWriter(transport.getBufferSupplier());
            authwriter.writeTo(ASTERISK_BYTE);
            authwriter.writeTo((byte) '2');
            authwriter.writeTo((byte) '\r', (byte) '\n');
            authwriter.writeTo(DOLLAR_BYTE);
            authwriter.writeTo((byte) '4');
            authwriter.writeTo((byte) '\r', (byte) '\n');
            authwriter.writeTo("AUTH".getBytes(UTF8));
            authwriter.writeTo((byte) '\r', (byte) '\n');

            authwriter.writeTo(DOLLAR_BYTE);
            authwriter.writeTo(String.valueOf(password.length).getBytes(UTF8));
            authwriter.writeTo((byte) '\r', (byte) '\n');
            authwriter.writeTo(password);
            authwriter.writeTo((byte) '\r', (byte) '\n');

            final ByteBuffer[] authbuffers = authwriter.toBuffers();
            conn.write(authbuffers, authbuffers, new CompletionHandler<Integer, ByteBuffer[]>() {
                @Override
                public void completed(Integer result, ByteBuffer[] attachments) {
                    int index = -1;
                    try {
                        for (int i = 0; i < attachments.length; i++) {
                            if (attachments[i].hasRemaining()) {
                                index = i;
                                break;
                            } else {
                                transport.offerBuffer(attachments[i]);
                            }
                        }
                        if (index == 0) {
                            conn.write(attachments, attachments, this);
                            return;
                        } else if (index > 0) {
                            ByteBuffer[] newattachs = new ByteBuffer[attachments.length - index];
                            System.arraycopy(attachments, index, newattachs, 0, newattachs.length);
                            conn.write(newattachs, newattachs, this);
                            return;
                        }
                        //----------------------- 读取返回结果 -------------------------------------
                        conn.read(new ReplyCompletionHandler(conn) {
                            @Override
                            public void completed(Integer result, ByteBuffer buffer) {
                                buffer.flip();
                                try {
                                    final byte sign = buffer.get();
                                    if (sign == PLUS_BYTE) { // +
                                        byte[] bs = readBytes(buffer);
                                        if ("OK".equalsIgnoreCase(new String(bs))) {
                                            conn.setSubobject("authed");
                                            rsfuture.complete(conn);
                                        } else {
                                            transport.offerConnection(false, conn);
                                            rsfuture.completeExceptionally(new RuntimeException("command : " + command + ", error: " + bs));
                                        }
                                    } else if (sign == MINUS_BYTE) { // -   异常
                                        String bs = readString(buffer);
                                        transport.offerConnection(false, conn);
                                        rsfuture.completeExceptionally(new RuntimeException("command : " + command + ", error: " + bs));
                                    } else {
                                        String exstr = "Unknown reply: " + (char) sign;
                                        transport.offerConnection(false, conn);
                                        rsfuture.completeExceptionally(new RuntimeException(exstr));
                                    }
                                } catch (Exception e) {
                                    failed(e, buffer);
                                }
                            }

                            @Override
                            public void failed(Throwable exc, ByteBuffer buffer) {
                                conn.offerBuffer(buffer);
                                transport.offerConnection(true, conn);
                                rsfuture.completeExceptionally(exc);
                            }

                        });
                    } catch (Exception e) {
                        failed(e, attachments);
                    }
                }

                @Override
                public void failed(Throwable exc, ByteBuffer[] attachments) {
                    transport.offerConnection(true, conn);
                    rsfuture.completeExceptionally(exc);
                }
            });
        } catch (Exception e) {
            rsfuture.completeExceptionally(e);
        }
        return rsfuture;
    }
}

abstract class ReplyCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {

    protected final ByteArray out = new ByteArray();

    protected final AsyncConnection conn;

    public ReplyCompletionHandler(AsyncConnection conn) {
        this.conn = conn;
    }

    protected byte[] readBytes(ByteBuffer buffer) throws IOException {
        readLine(buffer);
        return out.getBytesAndClear();
    }

    protected String readString(ByteBuffer buffer) throws IOException {
        readLine(buffer);
        return out.toStringAndClear(null);//传null则表示使用UTF8 
    }

    protected int readInt(ByteBuffer buffer) throws IOException {
        return (int) readLong(buffer);
    }

    protected long readLong(ByteBuffer buffer) throws IOException {
        readLine(buffer);
        int start = 0;
        if (out.get(0) == '$') start = 1;
        boolean negative = out.get(start) == '-';
        long value = negative ? 0 : (out.get(start) - '0');
        for (int i = 1 + start; i < out.size(); i++) {
            value = value * 10 + (out.get(i) - '0');
        }
        out.clear();
        return negative ? -value : value;
    }

    private void readLine(ByteBuffer buffer) throws IOException {
        boolean has = buffer.hasRemaining();
        byte lasted = has ? buffer.get() : 0;
        if (lasted == '\n' && !out.isEmpty() && out.getLastByte() == '\r') {
            out.removeLastByte();//读掉 \r
            buffer.get();//读掉 \n
            return;//传null则表示使用UTF8
        }
        if (has) out.write(lasted);
        while (buffer.hasRemaining()) {
            byte b = buffer.get();
            if (b == '\n' && lasted == '\r') {
                out.removeLastByte();
                return;
            }
            out.write(lasted = b);
        }
        //说明数据还没读取完
        buffer.clear();
        conn.readableByteChannel().read(buffer);
        buffer.flip();
        readLine(buffer);
    }

}
