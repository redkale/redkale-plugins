/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.*;
import org.redkale.annotation.ResourceListener;
import org.redkale.annotation.ResourceType;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import static org.redkale.boot.Application.RESNAME_APP_EXECUTOR;
import org.redkale.convert.Convert;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.*;
import org.redkale.net.client.ClientAddress;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkale.util.Utility.*;

/**
 * 详情见: https://redkale.org
 *
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@ResourceType(CacheSource.class)
public final class RedisCacheSource extends AbstractRedisSource {

    static final boolean debug = false; //System.getProperty("os.name").contains("Window") || System.getProperty("os.name").contains("Mac");

    protected static final byte FRAME_TYPE_BULK = '$';  //块字符串

    protected static final byte FRAME_TYPE_ARRAY = '*'; //数组

    protected static final byte FRAME_TYPE_STRING = '+';  //简单字符串(不包含CRLF)类型

    protected static final byte FRAME_TYPE_ERROR = '-'; //错误(不包含CRLF)类型

    protected static final byte FRAME_TYPE_NUMBER = ':'; //整型

    protected static final byte[] NX = "NX".getBytes();

    protected static final byte[] EX = "EX".getBytes();

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Resource(name = RESNAME_APP_CLIENT_ASYNCGROUP, required = false)
    protected AsyncGroup clientAsyncGroup;

    //配置<executor threads="0"> APP_EXECUTOR资源为null
    @Resource(name = RESNAME_APP_EXECUTOR, required = false)
    protected ExecutorService workExecutor;

    protected RedisCacheClient client;

    protected InetSocketAddress address;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (conf == null) {
            conf = AnyValue.create();
        }
        initClient(conf);
    }

    private void initClient(AnyValue conf) {
        String password = null;
        int urlmaxconns = Utility.cpus();
        int urlpipelines = org.redkale.net.client.Client.DEFAULT_MAX_PIPELINES;
        for (AnyValue node : getNodes(conf)) {
            String urluser = "";
            String urlpwd = "";
            String urldb = "";
            String addrstr = node.getValue(CACHE_SOURCE_URL, node.getValue("addr"));  //兼容addr
            if (addrstr.startsWith("redis://")) { //兼容 redis://:1234@127.0.0.1:6379?db=2
                URI uri = URI.create(addrstr);
                address = new InetSocketAddress(uri.getHost(), uri.getPort() > 0 ? uri.getPort() : 6379);
                String userInfo = uri.getUserInfo();
                if (isEmpty(userInfo)) {
                    String authority = uri.getAuthority();
                    if (authority != null && authority.indexOf('@') > 0) {
                        userInfo = authority.substring(0, authority.indexOf('@'));
                    }
                }
                if (isNotEmpty(userInfo)) {
                    urlpwd = userInfo;
                    if (urlpwd.startsWith(":")) {
                        urlpwd = urlpwd.substring(1);
                    } else {
                        int index = urlpwd.indexOf(':');
                        if (index > 0) {
                            urluser = urlpwd.substring(0, index);
                            urlpwd = urlpwd.substring(index + 1);
                        }
                    }
                }
                if (isNotEmpty(uri.getQuery())) {
                    String[] qrys = uri.getQuery().split("&|=");
                    for (int i = 0; i < qrys.length; i += 2) {
                        if (CACHE_SOURCE_USER.equals(qrys[i])) {
                            urluser = i == qrys.length - 1 ? "" : qrys[i + 1];
                        } else if (CACHE_SOURCE_PASSWORD.equals(qrys[i])) {
                            urlpwd = i == qrys.length - 1 ? "" : qrys[i + 1];
                        } else if (CACHE_SOURCE_DB.equals(qrys[i])) {
                            urldb = i == qrys.length - 1 ? "" : qrys[i + 1];
                        } else if (CACHE_SOURCE_MAXCONNS.equals(qrys[i])) {
                            urlmaxconns = i == qrys.length - 1 ? Utility.cpus() : Integer.parseInt(qrys[i + 1]);
                        } else if (CACHE_SOURCE_PIPELINES.equals(qrys[i])) {
                            urlpipelines = i == qrys.length - 1 ? org.redkale.net.client.Client.DEFAULT_MAX_PIPELINES : Integer.parseInt(qrys[i + 1]);
                        }
                    }
                }
            } else { //兼容addr和port分开
                address = new InetSocketAddress(addrstr, node.getIntValue("port"));
            }
            password = node.getValue(CACHE_SOURCE_PASSWORD, urlpwd).trim();
            String db0 = node.getValue(CACHE_SOURCE_DB, urldb).trim();
            if (!db0.isEmpty()) {
                db = Integer.valueOf(db0);
            }
            break;
        }
        AsyncGroup ioGroup = clientAsyncGroup;
        if (clientAsyncGroup == null) {
            String f = "Redkalex-Redis-IOThread-" + resourceName() + "-%s";
            ioGroup = AsyncGroup.create(f, workExecutor, 16 * 1024, Utility.cpus() * 4).start();
        }
        int maxconns = conf.getIntValue(CACHE_SOURCE_MAXCONNS, urlmaxconns);
        int pipelines = conf.getIntValue(CACHE_SOURCE_PIPELINES, urlpipelines);
        RedisCacheClient old = this.client;
        this.client = new RedisCacheClient(appName, resourceName(), ioGroup, resourceName() + "." + db, new ClientAddress(address),
            maxconns, pipelines, isEmpty(password) ? null : new RedisCacheReqAuth(password), db > 0 ? new RedisCacheReqDB(db) : null);
        if (old != null) {
            old.close();
        }
        //if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedisCacheSource.class.getSimpleName() + ": addr=" + address + ", db=" + db);
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
        return getClass().getSimpleName() + "{name=" + resourceName() + ", addrs=" + this.address + ", db=" + this.db + "}";
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (client != null) {
            client.close();
        }
    }

    @Override
    public CompletableFuture<Boolean> isOpenAsync() {
        return CompletableFuture.completedFuture(client != null);
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        return sendReadAsync("EXISTS", key, keyArgs(key)).thenApply(v -> v.getIntValue(0) > 0);
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        return sendReadAsync("GET", key, keyArgs(key)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    //--------------------- getex ------------------------------
    @Override
    public <T> CompletableFuture<T> getexAsync(String key, int expireSeconds, final Type type) {
        return sendReadAsync("GETEX", key, keyArgs(key, "EX", expireSeconds)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    @Override
    public CompletableFuture<Void> msetAsync(final Serializable... keyVals) {
        if (keyVals.length % 2 != 0) {
            throw new RedkaleException("key value must be paired");
        }
        return sendWriteAsync("MSET", keyVals[0].toString(), keymArgs(keyVals)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<Void> msetAsync(final Map map) {
        if (isEmpty(map)) {
            return CompletableFuture.completedFuture(null);
        }
        return sendReadAsync("MSET", map.keySet().stream().findFirst().orElse("").toString(), keymArgs(map)).thenApply(v -> v.getVoidValue());
    }

    //--------------------- setex ------------------------------
    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert, final Type type, T value) {
        return sendWriteAsync("SET", key, keyArgs(key, convert, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxAsync(String key, Convert convert, final Type type, T value) {
        return sendWriteAsync("SETNX", key, keyArgs(key, convert, type, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, Convert convert, final Type type, T value) {
        return sendWriteAsync("GETSET", key, keyArgs(key, convert, type, value)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, Convert convert, final Type type, T value) {
        return sendWriteAsync("SETEX", key, keyArgs(key, expireSeconds, convert, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxexAsync(String key, int expireSeconds, Convert convert, final Type type, T value) {
        return sendWriteAsync("SET", key, keyArgs(key, expireSeconds, NX, EX, convert, type, value)).thenApply(v -> v.getBoolValue());
    }

    //--------------------- expire ------------------------------    
    @Override
    public CompletableFuture<Void> expireAsync(String key, int expireSeconds) {
        return sendWriteAsync("EXPIRE", key, keyArgs(key, expireSeconds)).thenApply(v -> v.getVoidValue());
    }

    //--------------------- persist ------------------------------    
    @Override
    public CompletableFuture<Boolean> persistAsync(String key) {
        return sendWriteAsync("PERSIST", key, keyArgs(key)).thenApply(v -> v.getBoolValue());
    }

    //--------------------- rename ------------------------------    
    @Override
    public CompletableFuture<Boolean> renameAsync(String oldKey, String newKey) {
        return sendWriteAsync("RENAME", oldKey, keysArgs(oldKey, newKey)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public CompletableFuture<Boolean> renamenxAsync(String oldKey, String newKey) {
        return sendWriteAsync("RENAMENX", oldKey, keysArgs(oldKey, newKey)).thenApply(v -> v.getBoolValue());
    }

    //--------------------- del ------------------------------    
    @Override
    public CompletableFuture<Long> delAsync(String... keys) {
        if (keys.length == 0) {
            return CompletableFuture.completedFuture(0L);
        }
        return sendWriteAsync("DEL", keys[0], keysArgs(keys)).thenApply(v -> v.getLongValue(0L));
    }

    //--------------------- incrby ------------------------------    
    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return sendWriteAsync("INCR", key, keyArgs(key)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Long> incrbyAsync(final String key, long num) {
        return sendWriteAsync("INCRBY", key, keyArgs(key, num)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Double> incrbyFloatAsync(final String key, double num) {
        return sendWriteAsync("INCRBYFLOAT", key, keyArgs(key, num)).thenApply(v -> v.getDoubleValue(0.d));
    }

    //--------------------- decrby ------------------------------    
    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return sendWriteAsync("DECR", key, keyArgs(key)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Long> decrbyAsync(final String key, long num) {
        return sendWriteAsync("DECRBY", key, keyArgs(key, num)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Long> hdelAsync(final String key, String... fields) {
        return sendWriteAsync("HDEL", key, keysArgs(key, fields)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Long> hlenAsync(final String key) {
        return sendReadAsync("HLEN", key, keyArgs(key)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<List<String>> hkeysAsync(final String key) {
        return sendReadAsync("HKEYS", key, keyArgs(key)).thenApply(v -> (List) v.getListValue(key, cryptor, String.class));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field) {
        return hincrbyAsync(key, field, 1);
    }

    @Override
    public CompletableFuture<Long> hincrbyAsync(final String key, String field, long num) {
        return sendWriteAsync("HINCRBY", key, keysArgs(key, field, String.valueOf(num))).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Double> hincrbyFloatAsync(final String key, String field, double num) {
        return sendWriteAsync("HINCRBYFLOAT", key, keysArgs(key, field, String.valueOf(num))).thenApply(v -> v.getDoubleValue(0.d));
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(final String key, String field) {
        return hincrbyAsync(key, field, -1);
    }

    @Override
    public CompletableFuture<Long> hdecrbyAsync(final String key, String field, long num) {
        return hincrbyAsync(key, field, -num);
    }

    @Override
    public CompletableFuture<Boolean> hexistsAsync(final String key, String field) {
        return sendReadAsync("HEXISTS", key, keysArgs(key, field)).thenApply(v -> v.getIntValue(0) > 0);
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendWriteAsync("HSET", key, keyArgs(key, field, convert, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> hsetnxAsync(final String key, final String field, final Convert convert, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendWriteAsync("HSETNX", key, keyArgs(key, field, convert, type, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Serializable... values) {
        return sendWriteAsync("HMSET", key, keyMapArgs(key, values)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Map map) {
        if (isEmpty(map)) {
            return CompletableFuture.completedFuture(null);
        }
        return sendWriteAsync("HMSET", key, keyMapArgs(key, map)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<List<Serializable>> hmgetAsync(final String key, final Type type, final String... fields) {
        return sendReadAsync("HMGET", key, keysArgs(key, fields)).thenApply(v -> (List) v.getListValue(key, cryptor, type));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hscanAsync(final String key, final Type type, AtomicLong cursor, int limit, String pattern) {
        return sendReadAsync("HSCAN", key, keyArgs(key, cursor, limit, pattern)).thenApply(v -> {
            Map map = v.getMapValue(key, cryptor, type);
            cursor.set(v.getCursor());
            return map;
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hgetallAsync(final String key, final Type type) {
        return sendReadAsync("HGETALL", key, keyArgs(key)).thenApply(v -> v.getMapValue(key, cryptor, type));
    }

    @Override
    public <T> CompletableFuture<List<T>> hvalsAsync(final String key, final Type type) {
        return sendReadAsync("HVALS", key, keyArgs(key)).thenApply(v -> v.getListValue(key, cryptor, type));
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(final String key, final String field, final Type type) {
        return sendReadAsync("HGET", key, keysArgs(key, field)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    @Override
    public CompletableFuture<Long> hstrlenAsync(String key, final String field) {
        return sendReadAsync("HSTRLEN", key, keysArgs(key, field)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public <T> CompletableFuture<Set<T>> smembersAsync(String key, final Type componentType) {
        return sendReadAsync("SMEMBERS", key, keyArgs(key)).thenApply(v -> v.getSetValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<List<T>> lrangeAsync(String key, final Type componentType, int start, int stop) {
        return sendReadAsync("LRANGE", key, keyArgs(key, start, stop)).thenApply(v -> v.getListValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<T> lindexAsync(String key, Type componentType, int index) {
        return sendReadAsync("LINDEX", key, keyArgs(key, index)).thenApply(v -> v.getObjectValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<Long> linsertBeforeAsync(String key, Type componentType, T pivot, T value) {
        return sendReadAsync("LINSERT", key, keyArgs(key, "BEFORE", componentType, pivot, value)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public <T> CompletableFuture<Long> linsertAfterAsync(String key, Type componentType, T pivot, T value) {
        return sendReadAsync("LINSERT", key, keyArgs(key, "AFTER", componentType, pivot, value)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Void> ltrimAsync(final String key, int start, int stop) {
        return sendReadAsync("LTRIM", key, keyArgs(key, start, stop)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<T> lpopAsync(final String key, final Type componentType) {
        return sendReadAsync("LPOP", key, keyArgs(key)).thenApply(v -> v.getObjectValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<Void> lpushAsync(final String key, final Type componentType, T... values) {
        return sendReadAsync("LPUSH", key, keyArgs(key, componentType, values)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> lpushxAsync(final String key, final Type componentType, T... values) {
        return sendReadAsync("LPUSHX", key, keyArgs(key, componentType, values)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<T> rpopAsync(final String key, final Type componentType) {
        return sendReadAsync("RPOP", key, keyArgs(key)).thenApply(v -> v.getObjectValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<T> rpoplpushAsync(final String key, final String key2, final Type componentType) {
        return sendReadAsync("RPOPLPUSH", key, keysArgs(key, key2)).thenApply(v -> v.getObjectValue(key, cryptor, componentType));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Long> llenAsync(String key) {
        return sendReadAsync("LLEN", key, keyArgs(key)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Long> scardAsync(String key) {
        return sendReadAsync("SCARD", key, keyArgs(key)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public <T> CompletableFuture<List<Boolean>> smismembersAsync(final String key, final String... members) {
        return sendReadAsync("SMISMEMBER", key, keysArgs(key, members)).thenApply(v -> v.getListValue(key, cryptor, Boolean.class));
    }

    @Override
    public <T> CompletableFuture<Boolean> smoveAsync(String key, String key2, Type componentType, T member) {
        return sendReadAsync("SMOVE", key, keyArgs(key, key2, componentType, member)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public <T> CompletableFuture<List<T>> srandmemberAsync(String key, Type componentType, int count) {
        return sendReadAsync("SRANDMEMBER", key, keyArgs(key, count)).thenApply(v -> v.getListValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sdiffAsync(final String key, final Type componentType, final String... key2s) {
        return sendReadAsync("SDIFF", key, keysArgs(key, key2s)).thenApply(v -> v.getSetValue(key, cryptor, componentType));
    }

    @Override
    public CompletableFuture<Long> sdiffstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return sendReadAsync("SDIFFSTORE", key, keysArgs(Utility.append(key, srcKey, srcKey2s))).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sinterAsync(final String key, final Type componentType, final String... key2s) {
        return sendReadAsync("SINTER", key, keysArgs(key, key2s)).thenApply(v -> v.getSetValue(key, cryptor, componentType));
    }

    @Override
    public CompletableFuture<Long> sinterstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return sendReadAsync("SINTERSTORE", key, keysArgs(Utility.append(key, srcKey, srcKey2s))).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sunionAsync(final String key, final Type componentType, final String... key2s) {
        return sendReadAsync("SUNION", key, keysArgs(key, key2s)).thenApply(v -> v.getSetValue(key, cryptor, componentType));
    }

    @Override
    public CompletableFuture<Long> sunionstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return sendReadAsync("SUNIONSTORE", key, keysArgs(Utility.append(key, srcKey, srcKey2s))).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public <T> CompletableFuture<List<T>> mgetAsync(final Type componentType, final String... keys) {
        return sendReadAsync("MGET", keys[0], keysArgs(keys)).thenApply(v -> (List) v.getListValue(keys[0], cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<Map<String, Set<T>>> smembersAsync(final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Set<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Set<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendReadAsync("SMEMBERS", key, keyArgs(key)).thenAccept(v -> {
                Set c = v.getSetValue(key, cryptor, componentType);
                if (c != null) {
                    mapLock.lock();
                    try {
                        map.put(key, c);
                    } finally {
                        mapLock.unlock();
                    }
                }
            });
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
    public <T> CompletableFuture<Map<String, List<T>>> lrangesAsync(final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, List<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, List<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendReadAsync("LRANGE", key, keyArgs(key, 0, -1)).thenAccept(v -> {
                List c = v.getListValue(key, cryptor, componentType);
                if (c != null) {
                    mapLock.lock();
                    try {
                        map.put(key, c);
                    } finally {
                        mapLock.unlock();
                    }
                }
            });
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
    public <T> CompletableFuture<Boolean> sismemberAsync(String key, final Type componentType, T value) {
        return sendReadAsync("SISMEMBER", key, keyArgs(key, componentType, value)).thenApply(v -> v.getIntValue(0) > 0);
    }

    //--------------------- rpush ------------------------------  
    @Override
    public <T> CompletableFuture<Void> rpushAsync(String key, final Type componentType, T... values) {
        return sendWriteAsync("RPUSH", key, keyArgs(key, componentType, values)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Void> rpushxAsync(String key, final Type componentType, T... values) {
        return sendWriteAsync("RPUSHX", key, keyArgs(key, componentType, values)).thenApply(v -> v.getVoidValue());
    }

    //--------------------- lrem ------------------------------  
    @Override
    public <T> CompletableFuture<Long> lremAsync(String key, final Type componentType, T value) {
        return sendReadAsync("LREM", key, keyArgs(key, 0, componentType, value)).thenApply(v -> v.getLongValue(0L));
    }

    //--------------------- sadd ------------------------------  
    @Override
    public <T> CompletableFuture<Void> saddAsync(String key, Type componentType, T... values) {
        return sendWriteAsync("SADD", key, keyArgs(key, componentType, values)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<T> spopAsync(String key, Type componentType) {
        return sendWriteAsync("SPOP", key, keyArgs(key)).thenApply(v -> v.getObjectValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<Set<T>> spopAsync(String key, int count, Type componentType) {
        return sendWriteAsync("SPOP", key, keyArgs(key, count)).thenApply(v -> v.getObjectValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<Set< T>> sscanAsync(final String key, final Type componentType, AtomicLong cursor, int limit, String pattern) {
        return sendReadAsync("SSCAN", key, keyArgs(key, cursor, limit, pattern)).thenApply(v -> {
            Set set = v.getSetValue(key, cryptor, componentType);
            cursor.set(v.getCursor());
            return set;
        });
    }

    @Override
    public <T> CompletableFuture<Long> sremAsync(String key, final Type componentType, T... values) {
        return sendWriteAsync("SREM", key, keyArgs(key, componentType, values)).thenApply(v -> v.getLongValue(0L));
    }

    //--------------------- sorted set ------------------------------ 
    @Override
    public CompletableFuture<Void> zaddAsync(String key, CacheScoredValue... values) {
        return sendWriteAsync("ZADD", key, keyArgs(key, values)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T extends Number> CompletableFuture<T> zincrbyAsync(String key, CacheScoredValue value) {
        return sendWriteAsync("ZINCRBY", key, keyArgs(key, value)).thenApply(v -> v.getObjectValue(key, (RedisCryptor) null, value.getScore().getClass()));
    }

    @Override
    public CompletableFuture<Long> zremAsync(String key, String... members) {
        return sendWriteAsync("ZREM", key, keysArgs(key, members)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public <T extends Number> CompletableFuture<List<T>> zmscoreAsync(String key, Class<T> scoreType, String... members) {
        return sendReadAsync("ZMSCORE", key, keysArgs(key, members)).thenApply(v -> v.getListValue(key, (RedisCryptor) null, scoreType));
    }

    @Override
    public <T extends Number> CompletableFuture<T> zscoreAsync(String key, Class<T> scoreType, String member) {
        return sendReadAsync("ZSCORE", key, keysArgs(key, member)).thenApply(v -> v.getObjectValue(key, (RedisCryptor) null, scoreType));
    }

    @Override
    public CompletableFuture<Long> zcardAsync(String key) {
        return sendReadAsync("ZCARD", key, keyArgs(key)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Long> zrankAsync(String key, String member) {
        return sendReadAsync("ZRANK", key, keysArgs(key, member)).thenApply(v -> v.getLongValue(null));
    }

    @Override
    public CompletableFuture<Long> zrevrankAsync(String key, String member) {
        return sendReadAsync("ZREVRANK", key, keysArgs(key, member)).thenApply(v -> v.getLongValue(null));
    }

    @Override
    public CompletableFuture<List<String>> zrangeAsync(String key, int start, int stop) {
        return sendReadAsync("ZRANGE", key, keyArgs(key, start, stop)).thenApply(v -> v.getListValue(key, (RedisCryptor) null, String.class));
    }

    @Override
    public CompletableFuture<List<CacheScoredValue.NumberScoredValue>> zscanAsync(String key, Type scoreType, AtomicLong cursor, int limit, String pattern) {
        return sendReadAsync("ZSCAN", null, keyArgs(key, cursor, limit, pattern)).thenApply(v -> {
            List<CacheScoredValue.NumberScoredValue> set = v.getScoreListValue(null, (RedisCryptor) null, scoreType);
            cursor.set(v.getCursor());
            return set;
        });
    }

    //--------------------- keys ------------------------------  
    @Override
    public CompletableFuture<List<String>> keysAsync(String pattern) {
        String key = isEmpty(pattern) ? "*" : pattern;
        return sendReadAsync("KEYS", key, keyArgs(key)).thenApply(v -> (List) v.getListValue(key, (RedisCryptor) null, String.class));
    }

    @Override
    public CompletableFuture<List<String>> scanAsync(AtomicLong cursor, int limit, String pattern) {
        return sendReadAsync("SCAN", null, keyArgs(null, cursor, limit, pattern)).thenApply(v -> {
            List<String> list = v.getListValue(null, (RedisCryptor) null, String.class);
            cursor.set(v.getCursor());
            return list;
        });
    }

    //--------------------- dbsize ------------------------------  
    @Override
    public CompletableFuture<Long> dbsizeAsync() {
        return sendReadAsync("DBSIZE", null).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Void> flushdbAsync() {
        return sendReadAsync("FLUSHDB", null).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> flushallAsync() {
        return sendReadAsync("FLUSHALL", null).thenApply(v -> null);
    }

    //--------------------- send ------------------------------  
    @Local
    public CompletableFuture<RedisCacheResult> sendReadAsync(final String command, final String key, final byte[]... args) {
        return client.connect().thenCompose(conn -> conn.writeRequest(conn.pollRequest(WorkThread.currentWorkThread()).prepare(command, key, args))).orTimeout(6, TimeUnit.SECONDS);
    }

    @Local
    public CompletableFuture<RedisCacheResult> sendWriteAsync(final String command, final String key, final byte[]... args) {
        return client.connect().thenCompose(conn -> conn.writeRequest(conn.pollRequest(WorkThread.currentWorkThread()).prepare(command, key, args))).orTimeout(6, TimeUnit.SECONDS);
    }

    private byte[][] keyArgs(String key) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyArgs(String key, byte[] value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), value};
    }

    private byte[][] keyArgs(String key, int expire, byte[] value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8),
            String.valueOf(expire).getBytes(StandardCharsets.UTF_8),
            value};
    }

    private byte[][] keyArgs(final String key, AtomicLong cursor, int limit, String pattern) {
        int c = isNotEmpty(key) ? 2 : 1;
        if (isNotEmpty(pattern)) {
            c += 2;
        }
        if (limit > 0) {
            c += 2;
        }
        byte[][] bss = new byte[c][];
        int index = -1;
        if (isNotEmpty(key)) {
            bss[++index] = key.getBytes(StandardCharsets.UTF_8);
        }
        bss[++index] = cursor.toString().getBytes(StandardCharsets.UTF_8);
        if (isNotEmpty(pattern)) {
            bss[++index] = "MATCH".getBytes(StandardCharsets.UTF_8);
            bss[++index] = pattern.getBytes(StandardCharsets.UTF_8);
        }
        if (limit > 0) {
            bss[++index] = "COUNT".getBytes(StandardCharsets.UTF_8);
            bss[++index] = String.valueOf(limit).getBytes(StandardCharsets.UTF_8);
        }
        return bss;
    }

    private byte[][] keyArgs(String key, Number numValue) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), numValue.toString().getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyArgs(String key, int numValue) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), String.valueOf(numValue).getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyArgs(String key, long numValue) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), String.valueOf(numValue).getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyArgs(String key, String ex, int numValue) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8),
            ex.getBytes(StandardCharsets.UTF_8),
            String.valueOf(numValue).getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyArgs(String key, String ex, long numValue) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8),
            ex.getBytes(StandardCharsets.UTF_8),
            String.valueOf(numValue).getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyArgs(String key, int start, int stop) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8),
            String.valueOf(start).getBytes(StandardCharsets.UTF_8),
            String.valueOf(stop).getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyArgs(String key, Type type, Object value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), encodeValue(key, cryptor, null, type, value)};
    }

    private byte[][] keyArgs(String key, Convert convert, Type type, Object value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), encodeValue(key, cryptor, convert, type, value)};
    }

    private byte[][] keyArgs(String key, String field, Type type, Object value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), encodeValue(key, cryptor, null, type, value)};
    }

    private byte[][] keyArgs(String key, String field, Convert convert, Type type, Object value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), encodeValue(key, cryptor, convert, type, value)};
    }

    private byte[][] keyArgs(String key, int expire, Type type, Object value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8),
            String.valueOf(expire).getBytes(StandardCharsets.UTF_8),
            encodeValue(key, cryptor, null, type, value)};
    }

    private byte[][] keyArgs(String key, int expire, Convert convert, Type type, Object value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8),
            String.valueOf(expire).getBytes(StandardCharsets.UTF_8),
            encodeValue(key, cryptor, convert, type, value)};
    }

    private <T> byte[][] keyArgs(String key, final Type componentType, T... values) {
        byte[][] bss = new byte[values.length + 1][];
        bss[0] = key.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < values.length; i++) {
            bss[i + 1] = encodeValue(key, cryptor, null, componentType, values[i]);
        }
        return bss;
    }

    private <T> byte[][] keyArgs(String key, String arg, final Type componentType, T... values) {
        byte[][] bss = new byte[values.length + 2][];
        bss[0] = key.getBytes(StandardCharsets.UTF_8);
        bss[1] = arg.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < values.length; i++) {
            bss[i + 2] = encodeValue(key, cryptor, null, componentType, values[i]);
        }
        return bss;
    }

    private <T> byte[][] keyArgs(String key, CacheScoredValue... values) {
        byte[][] bss = new byte[values.length * 2 + 1][];
        bss[0] = key.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < values.length * 2; i += 2) {
            bss[i + 1] = values[i].getScore().toString().getBytes(StandardCharsets.UTF_8);
            bss[i + 2] = values[i].getValue().getBytes(StandardCharsets.UTF_8);
        }
        return bss;
    }

    private byte[][] keyArgs(String key, int expire, byte[] nx, byte[] ex, Type type, Object value) {
        return keyArgs(key, expire, nx, ex, null, type, value);
    }

    private byte[][] keyArgs(String key, int expire, byte[] nx, byte[] ex, Convert convert, Type type, Object value) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8),
            encodeValue(key, cryptor, convert, type, value), nx, ex,
            String.valueOf(expire).getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyValueArgs(String key, String strValue) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), encodeValue(key, cryptor, strValue)};
    }

    private byte[][] keyMapArgs(String key, Serializable... keyVals) {
        byte[][] bss = new byte[keyVals.length + (key == null ? 0 : 1)][];
        int start = -1;
        if (key != null) {
            start++;
            bss[0] = key.getBytes(StandardCharsets.UTF_8);
        }
        for (int i = 0; i < keyVals.length; i += 2) {
            String k = keyVals[i].toString();
            bss[i + start + 1] = k.getBytes(StandardCharsets.UTF_8);
            bss[i + start + 2] = encodeValue(k, cryptor, keyVals[i + 1]);
        }
        return bss;
    }

    private byte[][] keyMapArgs(String key, Map map) {
        byte[][] bss = new byte[map.size() * 2 + (key == null ? 0 : 1)][];
        int start = 0;
        if (key != null) {
            start++;
            bss[0] = key.getBytes(StandardCharsets.UTF_8);
        }
        AtomicInteger index = new AtomicInteger(start);
        map.forEach((k, v) -> {
            int i = index.getAndAdd(2);
            bss[i] = k.toString().getBytes(StandardCharsets.UTF_8);
            bss[i + 1] = encodeValue(k.toString(), cryptor, v);
        });
        return bss;
    }

    private byte[][] keymArgs(Serializable... keyVals) {
        return keyMapArgs(null, keyVals);
    }

    private byte[][] keymArgs(Map map) {
        return keyMapArgs(null, map);
    }

    private byte[][] keysArgs(String key, String field) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keysArgs(String key, String field, String num) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8),
            field.getBytes(StandardCharsets.UTF_8),
            num.getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keysArgs(String... keys) {
        byte[][] bss = new byte[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            bss[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return bss;
    }

    private byte[][] keysArgs(String key, String... keys) {
        byte[][] bss = new byte[keys.length + 1][];
        bss[0] = key.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < keys.length; i++) {
            bss[i + 1] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return bss;
    }

    private byte[] encodeValue(String key, RedisCryptor cryptor, String value) {
        if (cryptor != null) {
            value = cryptor.encrypt(key, value);
        }
        if (value == null) {
            throw new NullPointerException();
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] encodeValue(String key, RedisCryptor cryptor, Object value) {
        return encodeValue(key, cryptor, null, null, value);
    }

    private byte[] encodeValue(String key, RedisCryptor cryptor, Convert convert0, Type type, Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? RedisCacheRequest.BYTES_TRUE : RedisCacheRequest.BYTES_FALSE;
        }
        if (value instanceof byte[]) {
            if (cryptor != null) {
                String val = cryptor.encrypt(key, new String((byte[]) value, StandardCharsets.UTF_8));
                return val.getBytes(StandardCharsets.UTF_8);
            }
            return (byte[]) value;
        }
        if (value instanceof CharSequence) {
            if (cryptor != null) {
                value = cryptor.encrypt(key, String.valueOf(value));
            }
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (value.getClass().isPrimitive() || Number.class.isAssignableFrom(value.getClass())) {
            return value.toString().getBytes(StandardCharsets.US_ASCII);
        }
        if (convert0 == null) {
            if (convert == null) { //compile模式下convert可能为null
                convert = JsonConvert.root();
            }
            convert0 = convert;
        }
        Type t = type == null ? value.getClass() : type;
        byte[] bs = convert0.convertToBytes(t, value);
        if (bs.length > 1 && t instanceof Class && !CharSequence.class.isAssignableFrom((Class) t)) {
            if (bs[0] == '"' && bs[bs.length - 1] == '"') {
                bs = Arrays.copyOfRange(bs, 1, bs.length - 1);
            }
        }
        if (cryptor != null) {
            String val = cryptor.encrypt(key, new String(bs, StandardCharsets.UTF_8));
            return val.getBytes(StandardCharsets.UTF_8);
        }
        return bs;
    }

    //-------------------------- 过期方法 ----------------------------------
    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(final boolean set, final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Collection<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendReadAsync(set ? "SMEMBERS" : "LRANGE", key, set ? keyArgs(key) : keyArgs(key, 0, -1)).thenAccept(v -> {
                Collection c = set ? v.getSetValue(key, cryptor, componentType) : v.getListValue(key, cryptor, componentType);
                if (c != null) {
                    mapLock.lock();
                    try {
                        map.put(key, (Collection) c);
                    } finally {
                        mapLock.unlock();
                    }
                }
            });
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
        return sendReadAsync("TYPE", key, keyArgs(key)).thenCompose(t -> {
            String type = t.getObjectValue(key, cryptor, String.class);
            if (type == null) {
                return CompletableFuture.completedFuture(0);
            }
            return sendReadAsync(type.contains("list") ? "LLEN" : "SCARD", key, keyArgs(key)).thenApply(v -> v.getIntValue(0));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return sendReadAsync("TYPE", key, keyArgs(key)).thenCompose(t -> {
            String type = t.getObjectValue(key, cryptor, String.class);
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendReadAsync(set ? "SMEMBERS" : "LRANGE", key, set ? keyArgs(key) : keyArgs(key, 0, -1)).thenApply(v -> set ? v.getSetValue(key, cryptor, componentType) : v.getListValue(key, cryptor, componentType));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Long[]> getLongArrayAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendReadAsync("MGET", keys[0], bs).thenApply(v -> {
            List list = (List) v.getListValue(keys[0], cryptor, long.class);
            Long[] rs = new Long[keys.length];
            for (int i = 0; i < keys.length; i++) {
                Number obj = (Number) list.get(i);
                rs[i] = obj == null ? null : obj.longValue();
            }
            return rs;
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<String[]> getStringArrayAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendReadAsync("MGET", keys[0], bs).thenApply(v -> {
            List list = (List) v.getListValue(keys[0], cryptor, String.class);
            String[] rs = new String[keys.length];
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                rs[i] = obj == null ? null : obj.toString();
            }
            return rs;
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public Long[] getLongArray(final String... keys) {
        return getLongArrayAsync(keys).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
    public String[] getStringArray(final String... keys) {
        return getStringArrayAsync(keys).join();
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return sendReadAsync("TYPE", key, keyArgs(key)).thenCompose(t -> {
            String type = t.getObjectValue(key, cryptor, String.class);
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendReadAsync(set ? "SMEMBERS" : "LRANGE", key, set ? keyArgs(key) : keyArgs(key, 0, -1)).thenApply(v -> set ? v.getSetValue(key, cryptor, String.class) : v.getListValue(key, cryptor, String.class));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<String>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<String>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendReadAsync(set ? "SMEMBERS" : "LRANGE", key, set ? keyArgs(key) : keyArgs(key, 0, -1)).thenAccept(v -> {
                Collection<String> c = set ? v.getSetValue(key, cryptor, String.class) : v.getListValue(key, cryptor, String.class);
                if (c != null) {
                    mapLock.lock();
                    try {
                        map.put(key, (Collection) c);
                    } finally {
                        mapLock.unlock();
                    }
                }
            });
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
        return sendReadAsync("TYPE", key, keyArgs(key)).thenCompose(t -> {
            String type = t.getObjectValue(key, cryptor, String.class);
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendReadAsync(set ? "SMEMBERS" : "LRANGE", key, set ? keyArgs(key) : keyArgs(key, 0, -1))
                .thenApply(v -> set ? v.getSetValue(key, cryptor, long.class) : v.getListValue(key, cryptor, long.class));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<Long>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<Long>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendReadAsync(set ? "SMEMBERS" : "LRANGE", key, set ? keyArgs(key) : keyArgs(key, 0, -1)).thenAccept(v -> {
                Collection<Long> c = set ? v.getSetValue(key, cryptor, long.class) : v.getListValue(key, cryptor, long.class);
                if (c != null) {
                    mapLock.lock();
                    try {
                        map.put(key, (Collection) c);
                    } finally {
                        mapLock.unlock();
                    }
                }
            });
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

}
