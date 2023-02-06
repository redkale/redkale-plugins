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
import org.redkale.source.CacheSource;
import org.redkale.util.*;

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
                if (userInfo == null || userInfo.isEmpty()) {
                    String authority = uri.getAuthority();
                    if (authority != null && authority.indexOf('@') > 0) {
                        userInfo = authority.substring(0, authority.indexOf('@'));
                    }
                }
                if (userInfo != null && !userInfo.isEmpty()) {
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
                if (uri.getQuery() != null && !uri.getQuery().isEmpty()) {
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
        this.client = new RedisCacheClient(resourceName(), ioGroup, resourceName() + "." + db, new ClientAddress(address), maxconns, pipelines,
            password == null || password.isEmpty() ? null : new RedisCacheReqAuth(password), db > 0 ? new RedisCacheReqDB(db) : null);
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

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        return sendAsync("EXISTS", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getIntValue(0) > 0);
    }

    @Override
    public boolean exists(String key) {
        return existsAsync(key).join();
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        return sendAsync("GET", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    @Override
    public CompletableFuture<String> getStringAsync(String key) {
        return sendAsync("GET", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getStringValue(key, cryptor));
    }

    @Override
    public CompletableFuture<String> getSetStringAsync(String key, String value) {
        return sendAsync("GETSET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getStringValue(key, cryptor));
    }

    @Override
    public CompletableFuture<Long> getLongAsync(String key, long defValue) {
        return sendAsync("GET", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(defValue));
    }

    @Override
    public CompletableFuture<Long> getSetLongAsync(String key, long value, long defValue) {
        return sendAsync("GETSET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(value)).thenApply(v -> v.getLongValue(defValue));
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
    public String getSetString(String key, String value) {
        return getSetStringAsync(key, value).join();
    }

    @Override
    public long getLong(String key, long defValue) {
        return getLongAsync(key, defValue).join();
    }

    @Override
    public long getSetLong(String key, long value, long defValue) {
        return getSetLongAsync(key, value, defValue).join();
    }

    //--------------------- getex ------------------------------
    @Override
    public <T> CompletableFuture<T> getexAsync(String key, int expireSeconds, final Type type) {
        return sendAsync("GETEX", key, key.getBytes(StandardCharsets.UTF_8), "EX".getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    @Override
    public <T> T getex(String key, final int expireSeconds, final Type type) {
        return (T) getexAsync(key, expireSeconds, type).join();
    }

    @Override
    public CompletableFuture<String> getexStringAsync(String key, int expireSeconds) {
        return sendAsync("GETEX", key, key.getBytes(StandardCharsets.UTF_8), "EX".getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getStringValue(key, cryptor));
    }

    @Override
    public String getexString(String key, final int expireSeconds) {
        return getexStringAsync(key, expireSeconds).join();
    }

    @Override
    public CompletableFuture<Long> getexLongAsync(String key, int expireSeconds, long defValue) {
        return sendAsync("GETEX", key, key.getBytes(StandardCharsets.UTF_8), "EX".getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(defValue));
    }

    @Override
    public long getexLong(String key, final int expireSeconds, long defValue) {
        return getexLongAsync(key, expireSeconds, defValue).join();
    }

    @Override
    public CompletableFuture<byte[]> getexBytesAsync(final String key, final int expireSeconds) {
        return sendAsync("GETEX", key, key.getBytes(StandardCharsets.UTF_8), "EX".getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getFrameValue());
    }

    @Override
    public byte[] getexBytes(final String key, final int expireSeconds) {
        return getexBytesAsync(key, expireSeconds).join();
    }

    @Override
    public CompletableFuture<Void> msetAsync(final Object... keyVals) {
        if (keyVals.length % 2 != 0) {
            throw new RedkaleException("key value must be paired");
        }
        byte[][] bs = new byte[keyVals.length][];
        for (int i = 0; i < keyVals.length; i += 2) {
            String key = keyVals[i].toString();
            bs[i] = key.getBytes(StandardCharsets.UTF_8);
            bs[i + 1] = formatValue(key, cryptor, keyVals[i + 1]);
        }
        return sendAsync("MSET", keyVals[0].toString(), bs).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<Void> msetAsync(final Map map) {
        if (map == null || map.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<byte[]> bs = new ArrayList<>();
        StringWrapper onekey = new StringWrapper();
        map.forEach((key, val) -> {
            onekey.setValue(key.toString());
            bs.add(key.toString().getBytes(StandardCharsets.UTF_8));
            bs.add(formatValue(key.toString(), cryptor, val));
        });
        return sendAsync("MSET", onekey.getValue(), bs.toArray(new byte[bs.size()][])).thenApply(v -> v.getVoidValue());
    }

    //--------------------- setex ------------------------------
    @Override
    public <T> CompletableFuture<Void> setAsync(String key, final Type type, T value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert, final Type type, T value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, convert, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxAsync(String key, final Type type, T value) {
        return sendAsync("SETNX", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, type, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxAsync(String key, Convert convert, final Type type, T value) {
        return sendAsync("SETNX", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, convert, type, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public boolean setnxBytes(final String key, final byte[] value) {
        return setnxBytesAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> setnxBytesAsync(final String key, byte[] value) {
        return sendAsync("SETNX", key, key.getBytes(StandardCharsets.UTF_8), value).thenApply(v -> v.getBoolValue());
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, final Type type, T value) {
        return sendAsync("GETSET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, type, value)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, Convert convert, final Type type, T value) {
        return sendAsync("GETSET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, convert, type, value)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    @Override
    public void mset(final Object... keyVals) {
        msetAsync(keyVals).join();
    }

    @Override
    public void mset(final Map map) {
        msetAsync(map).join();
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
    public <T> boolean setnx(final String key, final Type type, T value) {
        return setnxAsync(key, type, value).join();
    }

    @Override
    public <T> boolean setnx(String key, final Convert convert, final Type type, T value) {
        return setnxAsync(key, convert, type, value).join();
    }

    @Override
    public <T> T getSet(String key, final Type type, T value) {
        return getSetAsync(key, type, value).join();
    }

    @Override
    public <T> T getSet(String key, Convert convert, final Type type, T value) {
        return getSetAsync(key, convert, type, value).join();
    }

    @Override
    public CompletableFuture<Void> setStringAsync(String key, String value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<Boolean> setnxStringAsync(String key, String value) {
        return sendAsync("SETNX", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public void setString(String key, String value) {
        setStringAsync(key, value).join();
    }

    @Override
    public boolean setnxString(String key, String value) {
        return setnxStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> setLongAsync(String key, long value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<Boolean> setnxLongAsync(String key, long value) {
        return sendAsync("SETNX", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public void setLong(String key, long value) {
        setLongAsync(key, value).join();
    }

    @Override
    public boolean setnxLong(String key, long value) {
        return setnxLongAsync(key, value).join();
    }

    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, final Type type, T value) {
        return sendAsync("SETEX", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, Convert convert, final Type type, T value) {
        return sendAsync("SETEX", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, convert, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> void setex(String key, int expireSeconds, final Type type, T value) {
        setexAsync(key, expireSeconds, type, value).join();
    }

    @Override
    public <T> void setex(String key, int expireSeconds, Convert convert, final Type type, T value) {
        setexAsync(key, expireSeconds, convert, type, value).join();
    }

    @Override
    public CompletableFuture<Void> setexStringAsync(String key, int expireSeconds, String value) {
        return sendAsync("SETEX", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public void setexString(String key, int expireSeconds, String value) {
        setexStringAsync(key, expireSeconds, value).join();
    }

    @Override
    public CompletableFuture<Void> setexLongAsync(String key, int expireSeconds, long value) {
        return sendAsync("SETEX", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public void setexLong(String key, int expireSeconds, long value) {
        setexLongAsync(key, expireSeconds, value).join();
    }

    @Override
    public CompletableFuture<Boolean> setnxexStringAsync(String key, int expireSeconds, String value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value), NX, EX, String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public CompletableFuture<Boolean> setnxexLongAsync(String key, int expireSeconds, long value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value), NX, EX, String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public CompletableFuture<Boolean> setnxexBytesAsync(String key, int expireSeconds, byte[] value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), value, NX, EX, String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxexAsync(String key, int expireSeconds, final Type type, T value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, type, value), NX, EX, String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxexAsync(String key, int expireSeconds, Convert convert, final Type type, T value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, convert, type, value), NX, EX, String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public <T> boolean setnxex(final String key, final int expireSeconds, final Type type, final T value) {
        return setnxexAsync(key, expireSeconds, type, value).join();
    }

    @Override
    public <T> boolean setnxex(final String key, final int expireSeconds, final Convert convert, final Type type, final T value) {
        return setnxexAsync(key, expireSeconds, convert, type, value).join();
    }

    @Override
    public boolean setnxexString(final String key, final int expireSeconds, final String value) {
        return setnxexStringAsync(key, expireSeconds, value).join();
    }

    @Override
    public boolean setnxexLong(final String key, final int expireSeconds, final long value) {
        return setnxexLongAsync(key, expireSeconds, value).join();
    }

    @Override
    public boolean setnxexBytes(final String key, final int expireSeconds, final byte[] value) {
        return setnxexBytesAsync(key, expireSeconds, value).join();
    }

    //--------------------- expire ------------------------------    
    @Override
    public CompletableFuture<Void> expireAsync(String key, int expireSeconds) {
        return sendAsync("EXPIRE", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public void expire(String key, int expireSeconds) {
        expireAsync(key, expireSeconds).join();
    }

    //--------------------- del ------------------------------    
    @Override
    public CompletableFuture<Integer> delAsync(String... keys) {
        if (keys.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        if (keys.length == 1) {
            return sendAsync("DEL", keys[0], keys[0].getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getIntValue(0));
        } else {
            byte[][] bs = new byte[keys.length][];
            for (int i = 0; i < keys.length; i++) {
                bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
            }
            return sendAsync("DEL", keys[0], bs).thenApply(v -> v.getIntValue(0));
        }
    }

    @Override
    public int del(String... keys) {
        return delAsync(keys).join();
    }

    //--------------------- incrby ------------------------------    
    @Override
    public long incr(final String key) {
        return incrAsync(key).join();
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return sendAsync("INCR", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public long incrby(final String key, long num) {
        return incrbyAsync(key, num).join();
    }

    @Override
    public double incrbyFloat(final String key, double num) {
        return incrbyFloatAsync(key, num).join();
    }

    @Override
    public CompletableFuture<Long> incrbyAsync(final String key, long num) {
        return sendAsync("INCRBY", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(num).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Double> incrbyFloatAsync(final String key, double num) {
        return sendAsync("INCRBYFLOAT", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(num).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getDoubleValue(0.d));
    }

    //--------------------- decrby ------------------------------    
    @Override
    public long decr(final String key) {
        return decrAsync(key).join();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return sendAsync("DECR", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public long decrby(final String key, long num) {
        return decrbyAsync(key, num).join();
    }

    @Override
    public CompletableFuture<Long> decrbyAsync(final String key, long num) {
        return sendAsync("DECRBY", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(num).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public int hdel(final String key, String... fields) {
        return hdelAsync(key, fields).join();
    }

    @Override
    public int hlen(final String key) {
        return hlenAsync(key).join();
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
    public long hincrby(final String key, String field, long num) {
        return hincrbyAsync(key, field, num).join();
    }

    @Override
    public double hincrbyFloat(final String key, String field, double num) {
        return hincrbyFloatAsync(key, field, num).join();
    }

    @Override
    public long hdecr(final String key, String field) {
        return hdecrAsync(key, field).join();
    }

    @Override
    public long hdecrby(final String key, String field, long num) {
        return hdecrbyAsync(key, field, num).join();
    }

    @Override
    public boolean hexists(final String key, String field) {
        return hexistsAsync(key, field).join();
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
    public <T> boolean hsetnx(final String key, final String field, final Type type, final T value) {
        return hsetnxAsync(key, field, type, value).join();
    }

    @Override
    public <T> boolean hsetnx(final String key, final String field, final Convert convert, final Type type, final T value) {
        return hsetnxAsync(key, field, convert, type, value).join();
    }

    @Override
    public boolean hsetnxString(final String key, final String field, final String value) {
        return hsetnxStringAsync(key, field, value).join();
    }

    @Override
    public boolean hsetnxLong(final String key, final String field, final long value) {
        return hsetnxLongAsync(key, field, value).join();
    }

    @Override
    public void hmset(final String key, final Serializable... values) {
        hmsetAsync(key, values).join();
    }

    @Override
    public void hmset(final String key, final Map map) {
        hmsetAsync(key, map).join();
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
    public CompletableFuture<Integer> hdelAsync(final String key, String... fields) {
        byte[][] bs = new byte[fields.length + 1][];
        bs[0] = key.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < fields.length; i++) {
            bs[i + 1] = fields[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendAsync("HDEL", key, bs).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public CompletableFuture<Integer> hlenAsync(final String key) {
        return sendAsync("HLEN", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public CompletableFuture<List<String>> hkeysAsync(final String key) {
        return sendAsync("HKEYS", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> (List) v.getListValue(key, cryptor, String.class));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field) {
        return hincrbyAsync(key, field, 1);
    }

    @Override
    public CompletableFuture<Long> hincrbyAsync(final String key, String field, long num) {
        return sendAsync("HINCRBY", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), String.valueOf(num).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Double> hincrbyFloatAsync(final String key, String field, double num) {
        return sendAsync("HINCRBYFLOAT", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), String.valueOf(num).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getDoubleValue(0.d));
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
        return sendAsync("HEXISTS", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getIntValue(0) > 0);
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendAsync("HSET", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, null, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendAsync("HSET", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, convert, type, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<Void> hsetStringAsync(final String key, final String field, final String value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendAsync("HSET", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<Void> hsetLongAsync(final String key, final String field, final long value) {
        return sendAsync("HSET", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> hsetnxAsync(final String key, final String field, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendAsync("HSETNX", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, null, type, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public <T> CompletableFuture<Boolean> hsetnxAsync(final String key, final String field, final Convert convert, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendAsync("HSETNX", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, convert, type, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public CompletableFuture<Boolean> hsetnxStringAsync(final String key, final String field, final String value) {
        if (value == null) {
            return CompletableFuture.completedFuture(false);
        }
        return sendAsync("HSETNX", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public CompletableFuture<Boolean> hsetnxLongAsync(final String key, final String field, final long value) {
        return sendAsync("HSETNX", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getBoolValue());
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Serializable... values) {
        byte[][] bs = new byte[values.length + 1][];
        bs[0] = key.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < values.length; i += 2) {
            bs[i + 1] = String.valueOf(values[i]).getBytes(StandardCharsets.UTF_8);
            bs[i + 2] = formatValue(key, cryptor, values[i + 1]);
        }
        return sendAsync("HMSET", key, bs).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Map map) {
        if (map == null || map.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<byte[]> bs = new ArrayList<>();
        bs.add(key.getBytes(StandardCharsets.UTF_8));
        map.forEach((k, v) -> {
            bs.add(k.toString().getBytes(StandardCharsets.UTF_8));
            bs.add(formatValue(k.toString(), cryptor, v));
        });
        return sendAsync("HMSET", key, bs.toArray(new byte[bs.size()][])).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<List<Serializable>> hmgetAsync(final String key, final Type type, final String... fields) {
        byte[][] bs = new byte[fields.length + 1][];
        bs[0] = key.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < fields.length; i++) {
            bs[i + 1] = fields[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendAsync("HMGET", key, bs).thenApply(v -> (List) v.getListValue(key, cryptor, type));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(final String key, final Type type, int offset, int limit) {
        return hmapAsync(key, type, offset, limit, null);
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(final String key, final Type type, int offset, int limit, String pattern) {
        byte[][] bs = new byte[pattern == null || pattern.isEmpty() ? 4 : 6][limit];
        int index = -1;
        bs[++index] = key.getBytes(StandardCharsets.UTF_8);
        bs[++index] = String.valueOf(offset).getBytes(StandardCharsets.UTF_8);
        if (pattern != null && !pattern.isEmpty()) {
            bs[++index] = "MATCH".getBytes(StandardCharsets.UTF_8);
            bs[++index] = pattern.getBytes(StandardCharsets.UTF_8);
        }
        bs[++index] = "COUNT".getBytes(StandardCharsets.UTF_8);
        bs[++index] = String.valueOf(limit).getBytes(StandardCharsets.UTF_8);
        return sendAsync("HSCAN", key, bs).thenApply(v -> v.getMapValue(key, cryptor, type));
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(final String key, final String field, final Type type) {
        return sendAsync("HGET", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getObjectValue(key, cryptor, type));
    }

    @Override
    public CompletableFuture<String> hgetStringAsync(final String key, final String field) {
        return sendAsync("HGET", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getStringValue(key, cryptor));
    }

    @Override
    public CompletableFuture<Long> hgetLongAsync(final String key, final String field, long defValue) {
        return sendAsync("HGET", key, key.getBytes(StandardCharsets.UTF_8), field.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(defValue));
    }

    @Override
    public <T> CompletableFuture<Set<T>> smembersAsync(String key, final Type componentType) {
        return sendAsync("SMEMBERS", key, keySetArgs(key)).thenApply(v -> v.getSetValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<List<T>> lrangeAsync(String key, final Type componentType) {
        return sendAsync("LRANGE", key, keyListArgs(key)).thenApply(v -> v.getListValue(key, cryptor, componentType));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Integer> llenAsync(String key) {
        return sendAsync("LLEN", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public CompletableFuture<Integer> scardAsync(String key) {
        return sendAsync("SCARD", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public int llen(String key) {
        return llenAsync(key).join();
    }

    @Override
    public int scard(String key) {
        return scardAsync(key).join();
    }

    @Override
    public CompletableFuture<Map<String, Long>> mgetLongAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendAsync("MGET", keys[0], bs).thenApply(v -> {
            List list = (List) v.getListValue(keys[0], cryptor, long.class);
            Map map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                if (obj != null) {
                    map.put(keys[i], list.get(i));
                }
            }
            return map;
        });
    }

    @Override
    public CompletableFuture<Map<String, String>> mgetStringAsync(final String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendAsync("MGET", keys[0], bs).thenApply(v -> {
            List list = (List) v.getListValue(keys[0], cryptor, String.class);
            Map map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                if (obj != null) {
                    map.put(keys[i], list.get(i));
                }
            }
            return map;
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> mgetAsync(final Type componentType, final String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendAsync("MGET", keys[0], bs).thenApply(v -> {
            List list = (List) v.getListValue(keys[0], cryptor, componentType);
            Map map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                if (obj != null) {
                    map.put(keys[i], list.get(i));
                }
            }
            return map;
        });
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> mgetBytesAsync(final String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendAsync("MGET", keys[0], bs).thenApply(v -> {
            List list = (List) v.getListValue(keys[0], cryptor, byte[].class);
            Map map = new LinkedHashMap<>();
            for (int i = 0; i < keys.length; i++) {
                Object obj = list.get(i);
                if (obj != null) {
                    map.put(keys[i], list.get(i));
                }
            }
            return map;
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, Set<T>>> smembersAsync(final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Set<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Set<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync("SMEMBERS", key, keySetArgs(key)).thenAccept(v -> {
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
    public <T> CompletableFuture<Map<String, List<T>>> lrangeAsync(final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, List<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, List<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync("LRANGE", key, keyListArgs(key)).thenAccept(v -> {
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
    public <T> Set<T> smembers(String key, final Type componentType) {
        return (Set) smembersAsync(key, componentType).join();
    }

    @Override
    public <T> List<T> lrange(String key, final Type componentType) {
        return (List) lrangeAsync(key, componentType).join();
    }

    @Override
    public Map<String, byte[]> mgetBytes(final String... keys) {
        return mgetBytesAsync(keys).join();
    }

    @Override
    public Map<String, Long> mgetLong(final String... keys) {
        return mgetLongAsync(keys).join();
    }

    @Override
    public Map<String, String> mgetString(final String... keys) {
        return mgetStringAsync(keys).join();
    }

    @Override
    public <T> Map<String, T> mget(final Type componentType, final String... keys) {
        return (Map) mgetAsync(componentType, keys).join();
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
        return sismemberAsync(key, componentType, value).join();
    }

    @Override
    public <T> CompletableFuture<Boolean> sismemberAsync(String key, final Type componentType, T value) {
        return sendAsync("SISMEMBER", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> v.getIntValue(0) > 0);
    }

    @Override
    public boolean sismemberString(String key, String value) {
        return sismemberStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> sismemberStringAsync(String key, String value) {
        return sendAsync("SISMEMBER", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getIntValue(0) > 0);
    }

    @Override
    public boolean sismemberLong(String key, long value) {
        return sismemberLongAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> sismemberLongAsync(String key, long value) {
        return sendAsync("SISMEMBER", key, key.getBytes(StandardCharsets.UTF_8), formatValue(value)).thenApply(v -> v.getIntValue(0) > 0);
    }

    //--------------------- rpush ------------------------------  
    @Override
    public <T> CompletableFuture<Void> rpushAsync(String key, final Type componentType, T value) {
        return sendAsync("RPUSH", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> void rpush(String key, final Type componentType, T value) {
        rpushAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Void> rpushStringAsync(String key, String value) {
        return sendAsync("RPUSH", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public void rpushString(String key, String value) {
        rpushStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> rpushLongAsync(String key, long value) {
        return sendAsync("RPUSH", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public void rpushLong(String key, long value) {
        rpushLongAsync(key, value).join();
    }

    //--------------------- lrem ------------------------------  
    @Override
    public <T> CompletableFuture<Integer> lremAsync(String key, final Type componentType, T value) {
        return sendAsync("LREM", key, key.getBytes(StandardCharsets.UTF_8), new byte[]{'0'}, formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public <T> int lrem(String key, final Type componentType, T value) {
        return lremAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Integer> lremStringAsync(String key, String value) {
        return sendAsync("LREM", key, key.getBytes(StandardCharsets.UTF_8), new byte[]{'0'}, formatValue(key, cryptor, value)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public int lremString(String key, String value) {
        return lremStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Integer> lremLongAsync(String key, long value) {
        return sendAsync("LREM", key, key.getBytes(StandardCharsets.UTF_8), new byte[]{'0'}, formatValue(key, cryptor, value)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public int lremLong(String key, long value) {
        return lremLongAsync(key, value).join();
    }

    //--------------------- sadd ------------------------------  
    @Override
    public <T> CompletableFuture<Void> saddAsync(String key, Type componentType, T value) {
        return sendAsync("SADD", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public <T> CompletableFuture<T> spopAsync(String key, Type componentType) {
        return sendAsync("SPOP", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getObjectValue(key, cryptor, componentType));
    }

    @Override
    public <T> CompletableFuture<Set<T>> spopAsync(String key, int count, Type componentType) {
        return sendAsync("SPOP", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(count).getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getObjectValue(key, cryptor, componentType));
    }

    @Override
    public CompletableFuture<String> spopStringAsync(String key) {
        return sendAsync("SPOP", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getStringValue(key, cryptor));
    }

    @Override
    public CompletableFuture<Set<String>> spopStringAsync(String key, int count) {
        return sendAsync("SPOP", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(count).getBytes(StandardCharsets.UTF_8)).thenApply(v -> (Set) v.getSetValue(key, cryptor, String.class));
    }

    @Override
    public CompletableFuture<Long> spopLongAsync(String key) {
        return sendAsync("SPOP", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getLongValue(0L));
    }

    @Override
    public CompletableFuture<Set<Long>> spopLongAsync(String key, int count) {
        return sendAsync("SPOP", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(count).getBytes(StandardCharsets.UTF_8)).thenApply(v -> (Set) v.getSetValue(key, cryptor, long.class));
    }

    @Override
    public <T> void sadd(String key, final Type componentType, T value) {
        saddAsync(key, componentType, value).join();
    }

    @Override
    public <T> T spop(String key, final Type componentType) {
        return (T) spopAsync(key, componentType).join();
    }

    @Override
    public <T> Set<T> spop(String key, int count, final Type componentType) {
        return (Set) spopAsync(key, count, componentType).join();
    }

    @Override
    public String spopString(String key) {
        return spopStringAsync(key).join();
    }

    @Override
    public Set<String> spopString(String key, int count) {
        return spopStringAsync(key, count).join();
    }

    @Override
    public Long spopLong(String key) {
        return spopLongAsync(key).join();
    }

    @Override
    public Set<Long> spopLong(String key, int count) {
        return spopLongAsync(key, count).join();
    }

    @Override
    public CompletableFuture<Void> saddStringAsync(String key, String value) {
        return sendAsync("SADD", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public void saddString(String key, String value) {
        saddStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> saddLongAsync(String key, long value) {
        return sendAsync("SADD", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getVoidValue());
    }

    @Override
    public void saddLong(String key, long value) {
        saddLongAsync(key, value).join();
    }

    //--------------------- srem ------------------------------  
    @Override
    public <T> CompletableFuture<Integer> sremAsync(String key, final Type componentType, T value) {
        return sendAsync("SREM", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public <T> int srem(String key, final Type componentType, T value) {
        return sremAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Integer> sremStringAsync(String key, String value) {
        return sendAsync("SREM", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public int sremString(String key, String value) {
        return sremStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Integer> sremLongAsync(String key, long value) {
        return sendAsync("SREM", key, key.getBytes(StandardCharsets.UTF_8), formatValue(key, cryptor, value)).thenApply(v -> v.getIntValue(0));
    }

    @Override
    public int sremLong(String key, long value) {
        return sremLongAsync(key, value).join();
    }

    //--------------------- keys ------------------------------  
    @Override
    public List<String> keys(String pattern) {
        return keysAsync(pattern).join();
    }

    @Override
    public byte[] getBytes(final String key) {
        return getBytesAsync(key).join();
    }

    @Override
    public byte[] getSetBytes(final String key, final byte[] value) {
        return getSetBytesAsync(key, value).join();
    }

    @Override
    public void setBytes(final String key, final byte[] value) {
        setBytesAsync(key, value).join();
    }

    @Override
    public void setexBytes(final String key, final int expireSeconds, final byte[] value) {
        setexBytesAsync(key, expireSeconds, value).join();
    }

    @Override
    public CompletableFuture<byte[]> getBytesAsync(final String key) {
        return sendAsync("GET", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getFrameValue());
    }

    @Override
    public CompletableFuture<byte[]> getSetBytesAsync(final String key, final byte[] value) {
        return sendAsync("GETSET", key, key.getBytes(StandardCharsets.UTF_8), value).thenApply(v -> v.getFrameValue());
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(final String key, final byte[] value) {
        return sendAsync("SET", key, key.getBytes(StandardCharsets.UTF_8), value).thenApply(v -> v.getVoidValue());

    }

    @Override
    public CompletableFuture<Void> setexBytesAsync(final String key, final int expireSeconds, final byte[] value) {
        return sendAsync("SETEX", key, key.getBytes(StandardCharsets.UTF_8), String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8), value).thenApply(v -> v.getVoidValue());
    }

    @Override
    public CompletableFuture<List<String>> keysAsync(String pattern) {
        String key = pattern == null || pattern.isEmpty() ? "*" : pattern;
        return sendAsync("KEYS", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> (List) v.getListValue(key, cryptor, String.class));
    }

    //--------------------- dbsize ------------------------------  
    @Override
    public long dbsize() {
        return dbsizeAsync().join();
    }

    @Override
    public CompletableFuture<Long> dbsizeAsync() {
        return sendAsync("DBSIZE", null).thenApply(v -> v.getLongValue(0L));
    }

    //--------------------- send ------------------------------  
    @Local
    public CompletableFuture<RedisCacheResult> sendAsync(final String command, final String key, final Serializable... args) {
        int start = key == null ? 0 : 1;
        byte[][] bs = new byte[args.length + start][];
        if (key != null) {
            bs[0] = key.getBytes(StandardCharsets.UTF_8);
        }
        for (int i = start; i < bs.length; i++) {
            bs[i] = JsonConvert.root().convertToBytes(args[i - 1]);
        }
        return client.connect().thenCompose(conn -> conn.writeRequest(conn.pollRequest(WorkThread.currWorkThread()).prepare(command, key, bs))).orTimeout(6, TimeUnit.SECONDS);
    }

    @Local
    public CompletableFuture<RedisCacheResult> sendAsync(final String command, final String key, final byte[]... args) {
        return client.connect().thenCompose(conn -> conn.writeRequest(conn.pollRequest(WorkThread.currWorkThread()).prepare(command, key, args))).orTimeout(6, TimeUnit.SECONDS);
    }

    private byte[][] keySetArgs(String key) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8)};
    }

    private byte[][] keyListArgs(String key) {
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), new byte[]{'0'}, new byte[]{'-', '1'}};
    }

    private byte[][] keyArgs(boolean set, String key) {
        if (set) {
            return new byte[][]{key.getBytes(StandardCharsets.UTF_8)};
        }
        return new byte[][]{key.getBytes(StandardCharsets.UTF_8), new byte[]{'0'}, new byte[]{'-', '1'}};
    }

    private byte[] formatValue(long value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] formatValue(String key, RedisCryptor cryptor, String value) {
        if (cryptor != null) {
            value = cryptor.encrypt(key, value);
        }
        if (value == null) {
            throw new NullPointerException();
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] formatValue(String key, RedisCryptor cryptor, Object value) {
        return formatValue(key, cryptor, null, null, value);
    }

    private byte[] formatValue(String key, RedisCryptor cryptor, Convert convert0, Type type, Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? RedisCacheRequest.TRUE : RedisCacheRequest.FALSE;
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
            return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        }
        if (value.getClass().isPrimitive() || Number.class.isAssignableFrom(value.getClass())) {
            return String.valueOf(value).getBytes(StandardCharsets.US_ASCII);
        }
        if (convert0 == null) {
            if (convert == null) { //compile模式下convert可能为null
                convert = JsonConvert.root();
            }
            convert0 = convert;
        }
        byte[] bs = convert0.convertToBytes(type == null ? value.getClass() : type, value);
        if (cryptor != null) {
            String val = cryptor.encrypt(key, new String(bs, StandardCharsets.UTF_8));
            return val.getBytes(StandardCharsets.UTF_8);
        }
        return bs;
    }

    @Override
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(final boolean set, final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Collection<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(set ? "SMEMBERS" : "LRANGE", key, keyArgs(set, key)).thenAccept(v -> {
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
    public CompletableFuture<Integer> getCollectionSizeAsync(String key) {
        return sendAsync("TYPE", key, key.getBytes(StandardCharsets.UTF_8)).thenCompose(t -> {
            String type = t.getStringValue(key, cryptor);
            if (type == null) {
                return CompletableFuture.completedFuture(0);
            }
            return sendAsync(type.contains("list") ? "LLEN" : "SCARD", key, key.getBytes(StandardCharsets.UTF_8)).thenApply(v -> v.getIntValue(0));
        });
    }

    @Override
    public int getCollectionSize(String key) {
        return getCollectionSizeAsync(key).join();
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return sendAsync("TYPE", key, key.getBytes(StandardCharsets.UTF_8)).thenCompose(t -> {
            String type = t.getStringValue(key, cryptor);
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendAsync(set ? "SMEMBERS" : "LRANGE", key, keyArgs(set, key)).thenApply(v -> set ? v.getSetValue(key, cryptor, componentType) : v.getListValue(key, cryptor, componentType));
        });
    }

    @Override
    public CompletableFuture<Long[]> getLongArrayAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendAsync("MGET", keys[0], bs).thenApply(v -> {
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
    public CompletableFuture<String[]> getStringArrayAsync(String... keys) {
        byte[][] bs = new byte[keys.length][];
        for (int i = 0; i < bs.length; i++) {
            bs[i] = keys[i].getBytes(StandardCharsets.UTF_8);
        }
        return sendAsync("MGET", keys[0], bs).thenApply(v -> {
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
    public <T> Collection<T> getCollection(String key, final Type componentType) {
        return (Collection) getCollectionAsync(key, componentType).join();
    }

    @Override
    public Long[] getLongArray(final String... keys) {
        return getLongArrayAsync(keys).join();
    }

    @Override
    public String[] getStringArray(final String... keys) {
        return getStringArrayAsync(keys).join();
    }

    @Override
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return sendAsync("TYPE", key, key.getBytes(StandardCharsets.UTF_8)).thenCompose(t -> {
            String type = t.getStringValue(key, cryptor);
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendAsync(set ? "SMEMBERS" : "LRANGE", key, keyArgs(set, key)).thenApply(v -> set ? v.getSetValue(key, cryptor, String.class) : v.getListValue(key, cryptor, String.class));
        });
    }

    @Override
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<String>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<String>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(set ? "SMEMBERS" : "LRANGE", key, keyArgs(set, key)).thenAccept(v -> {
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
    public Collection<String> getStringCollection(String key) {
        return getStringCollectionAsync(key).join();
    }

    @Override
    public Map<String, Collection<String>> getStringCollectionMap(final boolean set, String... keys) {
        return getStringCollectionMapAsync(set, keys).join();
    }

    @Override
    public CompletableFuture<Collection<Long>> getLongCollectionAsync(String key) {
        return sendAsync("TYPE", key, key.getBytes(StandardCharsets.UTF_8)).thenCompose(t -> {
            String type = t.getStringValue(key, cryptor);
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendAsync(set ? "SMEMBERS" : "LRANGE", key, keyArgs(set, key)).thenApply(v -> set ? v.getSetValue(key, cryptor, long.class) : v.getListValue(key, cryptor, long.class));
        });
    }

    @Override
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<Long>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<Long>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(set ? "SMEMBERS" : "LRANGE", key, keyArgs(set, key)).thenAccept(v -> {
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

    @Override
    public Collection<Long> getLongCollection(String key) {
        return getLongCollectionAsync(key).join();
    }

    @Override
    public Map<String, Collection<Long>> getLongCollectionMap(final boolean set, String... keys) {
        return getLongCollectionMapAsync(set, keys).join();
    }

    //--------------------- getexCollection ------------------------------  
    @Override
    public <T> CompletableFuture<Collection<T>> getexCollectionAsync(String key, int expireSeconds, final Type componentType) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key, componentType));
    }

    @Override
    public <T> Collection<T> getexCollection(String key, final int expireSeconds, final Type componentType) {
        return (Collection) getexCollectionAsync(key, expireSeconds, componentType).join();
    }

    @Override
    public CompletableFuture<Collection<String>> getexStringCollectionAsync(String key, int expireSeconds) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getStringCollectionAsync(key));
    }

    @Override
    public Collection<String> getexStringCollection(String key, final int expireSeconds) {
        return getexStringCollectionAsync(key, expireSeconds).join();
    }

    @Override
    public CompletableFuture<Collection<Long>> getexLongCollectionAsync(String key, int expireSeconds) {
        return (CompletableFuture) expireAsync(key, expireSeconds).thenCompose(v -> getLongCollectionAsync(key));
    }

    @Override
    public Collection<Long> getexLongCollection(String key, final int expireSeconds) {
        return getexLongCollectionAsync(key, expireSeconds).join();
    }

    @Override
    public <T> Map<String, Collection<T>> getCollectionMap(final boolean set, final Type componentType, String... keys) {
        return (Map) getCollectionMapAsync(set, componentType, keys).join();
    }
}
