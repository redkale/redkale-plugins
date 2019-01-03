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
import org.redkale.convert.bson.BsonByteBufferWriter;
import org.redkale.convert.json.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.*;
import org.redkale.service.*;
import org.redkale.source.CacheSource;
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
public class RedisCacheSource<V extends Object> extends AbstractService implements CacheSource<V>, Service, AutoCloseable, Resourcable {

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
        }
        if (!passwords0.isEmpty()) this.passwords = passwords0;
        TransportFactory transportFactory = TransportFactory.create(threads, bufferPoolSize, bufferCapacity, readTimeoutSeconds, writeTimeoutSeconds);
        this.transport = transportFactory.createTransportTCP("Redis-Transport", null, addresses);

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
        DefaultAnyValue conf = new DefaultAnyValue();
        conf.addValue("node", new DefaultAnyValue().addValue("addr", "127.0.0.1").addValue("port", "6379"));

        RedisCacheSource source = new RedisCacheSource();
        source.defaultConvert = JsonFactory.root().getConvert();
        source.initValueType(String.class); //value用String类型
        source.init(conf);
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 7788);

        System.out.println("------------------------------------");
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
        System.out.println("sets3&sets4:  " + source.getStringCollectionMap("sets3", "sets4"));
        System.out.println("------------------------------------");
        source.set("myaddr", InetSocketAddress.class, addr);
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
        System.out.println("myaddrs&myaddrs2:  " + source.getCollectionMap(InetSocketAddress.class, "myaddrs", "myaddrs2"));
        System.out.println("------------------------------------");
        source.remove("myaddrs");
        Type mapType = new TypeToken<Map<String, Integer>>() {
        }.getType();
        Map<String, Integer> map = new HashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        source.set("mapvals", mapType, map);
        System.out.println("mapvals:  " + source.get("mapvals", mapType));
        System.out.println("------------------------------------");

    }

    @Override
    public void close() throws Exception {  //在 Application 关闭时调用
        destroy(null);
    }

    @Override
    public String resourceName() {
        Resource res = this.getClass().getAnnotation(Resource.class);
        return res == null ? null : res.name();
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
        return (CompletableFuture) send("SET", CacheEntryType.OBJECT, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, final Type type, T value) {
        return (CompletableFuture) send("SET", CacheEntryType.OBJECT, type, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, type, value));
    }

    @Override
    public void set(String key, V value) {
        setAsync(key, value).join();
    }

    @Override
    public <T> void set(String key, final Type type, T value) {
        setAsync(key, type, value).join();
    }

    @Override
    public CompletableFuture<Void> setStringAsync(String key, String value) {
        return (CompletableFuture) send("SET", CacheEntryType.STRING, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Type) null, value));
    }

    @Override
    public void setString(String key, String value) {
        setStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> setLongAsync(String key, long value) {
        return (CompletableFuture) send("SET", CacheEntryType.LONG, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Type) null, value));
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
    public <T> CompletableFuture<Void> setAsync(int expireSeconds, String key, final Type type, T value) {
        return (CompletableFuture) setAsync(key, type, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public void set(int expireSeconds, String key, V value) {
        setAsync(expireSeconds, key, value).join();
    }

    @Override
    public <T> void set(int expireSeconds, String key, final Type type, T value) {
        setAsync(expireSeconds, key, type, value).join();
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
    public CompletableFuture<Void> removeAsync(String key) {
        return (CompletableFuture) send("DEL", null, (Type) null, key, key.getBytes(UTF8));
    }

    @Override
    public void remove(String key) {
        removeAsync(key).join();
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
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(final Type componentType, final String... keys) {
        return (CompletableFuture) send("OBJECT", null, componentType, keys[0], "ENCODING".getBytes(UTF8), keys[0].getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            final CompletableFuture<Map<String, Collection<T>>> rsFuture = new CompletableFuture<>();
            final Map<String, Collection<T>> map = new HashMap<>();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (new String((byte[]) t).contains("list")) { //list        
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
        });
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
    public <T> Map<String, Collection<T>> getCollectionMap(final Type componentType, String... keys) {
        return (Map) getCollectionMapAsync(componentType, keys).join();
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
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(String... keys) {
        return (CompletableFuture) send("OBJECT", null, (Type) null, keys[0], "ENCODING".getBytes(UTF8), keys[0].getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            final CompletableFuture<Map<String, Collection<String>>> rsFuture = new CompletableFuture<>();
            final Map<String, Collection<String>> map = new HashMap<>();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (new String((byte[]) t).contains("list")) { //list        
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
        });
    }

    @Override
    public Collection<String> getStringCollection(String key) {
        return getStringCollectionAsync(key).join();
    }

    @Override
    public Map<String, Collection<String>> getStringCollectionMap(String... keys) {
        return getStringCollectionMapAsync(keys).join();
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
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(String... keys) {
        return (CompletableFuture) send("OBJECT", null, (Type) null, keys[0], "ENCODING".getBytes(UTF8), keys[0].getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            final CompletableFuture<Map<String, Collection<Long>>> rsFuture = new CompletableFuture<>();
            final Map<String, Collection<Long>> map = new HashMap<>();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (new String((byte[]) t).contains("list")) { //list        
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
        });
    }

    @Override
    public Collection<Long> getLongCollection(String key) {
        return getLongCollectionAsync(key).join();
    }

    @Override
    public Map<String, Collection<Long>> getLongCollectionMap(String... keys) {
        return getLongCollectionMapAsync(keys).join();
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
        return (CompletableFuture) send("SISMEMBER", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Boolean> existsSetItemAsync(String key, final Type componentType, T value) {
        return (CompletableFuture) send("SISMEMBER", null, componentType, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, componentType, value));
    }

    @Override
    public boolean existsStringSetItem(String key, String value) {
        return existsStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> existsStringSetItemAsync(String key, String value) {
        return (CompletableFuture) send("SISMEMBER", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Type) null, value));
    }

    @Override
    public boolean existsLongSetItem(String key, long value) {
        return existsLongSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> existsLongSetItemAsync(String key, long value) {
        return (CompletableFuture) send("SISMEMBER", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Type) null, value));
    }

    //--------------------- appendListItem ------------------------------  
    @Override
    public CompletableFuture<Void> appendListItemAsync(String key, V value) {
        return (CompletableFuture) send("RPUSH", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> appendListItemAsync(String key, final Type componentType, T value) {
        return (CompletableFuture) send("RPUSH", null, componentType, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, componentType, value));
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
        return (CompletableFuture) send("RPUSH", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Type) null, value));
    }

    @Override
    public void appendStringListItem(String key, String value) {
        appendStringListItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> appendLongListItemAsync(String key, long value) {
        return (CompletableFuture) send("RPUSH", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Type) null, value));
    }

    @Override
    public void appendLongListItem(String key, long value) {
        appendLongListItemAsync(key, value).join();
    }

    //--------------------- removeListItem ------------------------------  
    @Override
    public CompletableFuture<Void> removeListItemAsync(String key, V value) {
        return (CompletableFuture) send("LREM", null, (Type) null, key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(CacheEntryType.OBJECT, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> removeListItemAsync(String key, final Type componentType, T value) {
        return (CompletableFuture) send("LREM", null, componentType, key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(CacheEntryType.OBJECT, componentType, value));
    }

    @Override
    public void removeListItem(String key, V value) {
        removeListItemAsync(key, value).join();
    }

    @Override
    public <T> void removeListItem(String key, final Type componentType, T value) {
        removeListItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Void> removeStringListItemAsync(String key, String value) {
        return (CompletableFuture) send("LREM", null, (Type) null, key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(CacheEntryType.STRING, (Type) null, value));
    }

    @Override
    public void removeStringListItem(String key, String value) {
        removeStringListItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> removeLongListItemAsync(String key, long value) {
        return (CompletableFuture) send("LREM", null, (Type) null, key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(CacheEntryType.LONG, (Type) null, value));
    }

    @Override
    public void removeLongListItem(String key, long value) {
        removeLongListItemAsync(key, value).join();
    }

    //--------------------- appendSetItem ------------------------------  
    @Override
    public CompletableFuture<Void> appendSetItemAsync(String key, V value) {
        return (CompletableFuture) send("SADD", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> appendSetItemAsync(String key, Type componentType, T value) {
        return (CompletableFuture) send("SADD", null, componentType, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, componentType, value));
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
    public CompletableFuture<Void> appendStringSetItemAsync(String key, String value) {
        return (CompletableFuture) send("SADD", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Type) null, value));
    }

    @Override
    public void appendStringSetItem(String key, String value) {
        appendStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> appendLongSetItemAsync(String key, long value) {
        return (CompletableFuture) send("SADD", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Type) null, value));
    }

    @Override
    public void appendLongSetItem(String key, long value) {
        appendLongSetItemAsync(key, value).join();
    }

    //--------------------- removeSetItem ------------------------------  
    @Override
    public CompletableFuture<Void> removeSetItemAsync(String key, V value) {
        return (CompletableFuture) send("SREM", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, (Type) null, value));
    }

    @Override
    public <T> CompletableFuture<Void> removeSetItemAsync(String key, final Type componentType, T value) {
        return (CompletableFuture) send("SREM", null, componentType, key, key.getBytes(UTF8), formatValue(CacheEntryType.OBJECT, componentType, value));
    }

    @Override
    public void removeSetItem(String key, V value) {
        removeSetItemAsync(key, value).join();
    }

    @Override
    public <T> void removeSetItem(String key, final Type componentType, T value) {
        removeSetItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Void> removeStringSetItemAsync(String key, String value) {
        return (CompletableFuture) send("SREM", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.STRING, (Type) null, value));
    }

    @Override
    public void removeStringSetItem(String key, String value) {
        removeStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> removeLongSetItemAsync(String key, long value) {
        return (CompletableFuture) send("SREM", null, (Type) null, key, key.getBytes(UTF8), formatValue(CacheEntryType.LONG, (Type) null, value));
    }

    @Override
    public void removeLongSetItem(String key, long value) {
        removeLongSetItemAsync(key, value).join();
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
    private byte[] formatValue(CacheEntryType cacheType, Type resultType, Object value) {
        if (value == null) return "null".getBytes(UTF8);
        if (cacheType == CacheEntryType.LONG || cacheType == CacheEntryType.ATOMIC) return String.valueOf(value).getBytes(UTF8);
        if (cacheType == CacheEntryType.STRING) return convert.convertTo(String.class, value).getBytes(UTF8);
        return convert.convertTo(resultType == null ? objValueType : resultType, value).getBytes(UTF8);
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
                                            future.complete("SET".equals(command) ? null : bs);
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
                                            if (command.startsWith("INCR") || command.startsWith("DECR")) {
                                                transport.offerConnection(false, conn);
                                                callback.completed(rs, key);
                                            } else {
                                                transport.offerConnection(false, conn);
                                                callback.completed(("EXISTS".equals(command) || "SISMEMBER".equals(command)) ? (rs > 0) : (("LLEN".equals(command) || "SCARD".equals(command) || "DBSIZE".equals(command)) ? (int) rs : null), key);
                                            }
                                        } else {
                                            if (command.startsWith("INCR") || command.startsWith("DECR")) {
                                                transport.offerConnection(false, conn);
                                                future.complete(rs);
                                            } else {
                                                transport.offerConnection(false, conn);
                                                future.complete(("EXISTS".equals(command) || "SISMEMBER".equals(command)) ? (rs > 0) : (("LLEN".equals(command) || "SCARD".equals(command) || "DBSIZE".equals(command)) ? (int) rs : null));
                                            }
                                        }
                                    } else if (sign == DOLLAR_BYTE) { // $
                                        long val = readLong(buffer);
                                        byte[] rs = val <= 0 ? null : readBytes(buffer);
                                        Type ct = cacheType == CacheEntryType.LONG ? long.class : (cacheType == CacheEntryType.STRING ? String.class : (resultType == null ? objValueType : resultType));
                                        if (future == null) {
                                            transport.offerConnection(false, conn);
                                            callback.completed(("GET".equals(command) || rs == null) ? convert.convertFrom(ct, new String(rs, UTF8)) : null, key);
                                        } else {
                                            transport.offerConnection(false, conn);
                                            future.complete("GET".equals(command) ? convert.convertFrom(ct, rs == null ? null : new String(rs, UTF8)) : rs);
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
                                            Collection rs = set ? new HashSet() : new ArrayList();
                                            boolean keys = "KEYS".equals(command);
                                            Type ct = cacheType == CacheEntryType.LONG ? long.class : (cacheType == CacheEntryType.STRING ? String.class : (resultType == null ? objValueType : resultType));
                                            for (int i = 0; i < len; i++) {
                                                if (readInt(buffer) > 0) rs.add(keys ? new String(readBytes(buffer), UTF8) : convert.convertFrom(ct, new String(readBytes(buffer), UTF8)));
                                            }
                                            if (future == null) {
                                                transport.offerConnection(false, conn);
                                                callback.completed(rs, key);
                                            } else {
                                                transport.offerConnection(false, conn);
                                                future.complete((Serializable) rs);
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
        return future;
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
        conn.read(buffer);
        buffer.flip();
        readLine(buffer);
    }

}
