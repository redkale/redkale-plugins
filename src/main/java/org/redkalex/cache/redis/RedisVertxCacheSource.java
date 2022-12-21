/*
 */
package org.redkalex.cache.redis;

import io.vertx.core.*;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.*;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import org.redkale.annotation.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceListener;
import org.redkale.annotation.ResourceType;
import org.redkale.convert.Convert;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.Local;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_MAXCONNS;
import org.redkale.source.CacheSource;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@ResourceType(CacheSource.class)
public class RedisVertxCacheSource extends AbstractRedisSource {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected Type objValueType = String.class;

    protected List<String> nodeAddrs;

    protected Vertx vertx;

    protected io.vertx.redis.client.RedisAPI client;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (conf == null) conf = AnyValue.create();
        initClient(conf);
    }

    private void initClient(AnyValue conf) {
        String password = null;
        int urlmaxconns = Utility.cpus();
        List<String> addrs = new ArrayList<>();
        for (AnyValue node : getNodes(conf)) {
            String addrstr = node.getValue(CACHE_SOURCE_URL);
            addrs.add(addrstr);
            password = node.getValue(CACHE_SOURCE_PASSWORD, null);
            if (db < 0) {
                String db0 = node.getValue(CACHE_SOURCE_DB, "").trim();
                if (!db0.isEmpty()) db = Integer.valueOf(db0);
            }
            URI uri = URI.create(addrstr);
            if (uri.getQuery() != null && !uri.getQuery().isEmpty()) {
                String[] qrys = uri.getQuery().split("&|=");
                for (int i = 0; i < qrys.length; i += 2) {
                    if (CACHE_SOURCE_MAXCONNS.equals(qrys[i])) {
                        urlmaxconns = i == qrys.length - 1 ? Utility.cpus() : Integer.parseInt(qrys[i + 1]);
                    }
                }
            }
        }
        int maxconns = conf.getIntValue(CACHE_SOURCE_MAXCONNS, urlmaxconns);
        //Redis链接
        RedisOptions config = new RedisOptions();
        if (maxconns > 0) config.setMaxPoolSize(maxconns);
        if (password != null) config.setPassword(password.trim());
        if (maxconns > 0) config.setMaxPoolWaiting(maxconns != Utility.cpus() ? maxconns : maxconns * 10);
        config.setEndpoints(addrs);
        if (this.vertx == null) {
            this.vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(Utility.cpus()).setPreferNativeTransport(true));
        }
        RedisAPI old = this.client;
        this.client = RedisAPI.api(Redis.createClient(this.vertx, config));
        if (old != null) old.close();
    }

    @Override
    @ResourceListener
    public void onResourceChange(ResourceEvent[] events) {
        if (events == null || events.length < 1) return;
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            sb.append("CacheSource(name=").append(resourceName()).append(") change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
        }
        initClient(this.config);
        if (!sb.isEmpty()) {
            logger.log(Level.INFO, sb.toString());
        }
    }

    public boolean acceptsConf(AnyValue config) {
        if (config == null) return false;
        AnyValue[] nodes = getNodes(config);
        if (nodes == null || nodes.length == 0) return false;
        for (AnyValue node : nodes) {
            String val = node.getValue(CACHE_SOURCE_URL);
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

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (client != null) client.close();
        if (this.vertx != null) this.vertx.close();
    }

    protected <T> CompletableFuture<T> completableFuture(io.vertx.core.Future<T> rf) {
        return rf.toCompletionStage().toCompletableFuture();
    }

    protected CompletableFuture<Response> sendAsync(Command cmd, String... args) {
        return completableFuture(redisAPI().send(cmd, args));
    }

    protected RedisAPI redisAPI() {
        return client;
    }

    protected Long orElse(Long v, long def) {
        return v == null ? def : v;
    }

    protected Integer orElse(Integer v, int def) {
        return v == null ? def : v;
    }

    protected Boolean getBooleanValue(Response resp) {
        if (resp == null) return false;
        Boolean v = resp.toBoolean();
        return v == null ? false : v;
    }

    protected String getStringValue(String key, RedisCryptor cryptor, Response resp) {
        if (resp == null) return null;
        String val = resp.toString(StandardCharsets.UTF_8);
        if (cryptor == null) return val;
        return cryptor.decrypt(key, val);
    }

    protected Long getLongValue(Response resp, long defvalue) {
        if (resp == null) return defvalue;
        Long v = resp.toLong();
        return v == null ? defvalue : v;
    }

    protected Integer getIntValue(Response resp, int defvalue) {
        if (resp == null) return defvalue;
        Integer v = resp.toInteger();
        return v == null ? defvalue : v;
    }

    protected <T> T getObjectValue(String key, RedisCryptor cryptor, String bs, Type type) {
        if (bs == null) return null;
        if (type == byte[].class) return (T) bs.getBytes(StandardCharsets.UTF_8);
        if (type == String.class) return (T) decryptValue(key, cryptor, bs);
        if (type == long.class) return (T) (Long) Long.parseLong(bs);
        return (T) JsonConvert.root().convertFrom(type, decryptValue(key, cryptor, bs));
    }

    protected <T> T getObjectValue(String key, RedisCryptor cryptor, Response resp, Type type) {
        return getObjectValue(key, cryptor, resp == null ? null : resp.toString(StandardCharsets.UTF_8), type);
    }

    protected <T> Collection<T> getCollectionValue(String key, RedisCryptor cryptor, Response resp, boolean set, Type type) {
        int size = resp == null ? 0 : resp.size();
        if (size == 0) return set ? new LinkedHashSet<>() : new ArrayList<>();
        Collection<T> list = set ? new LinkedHashSet<>() : new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(getObjectValue(key, cryptor, resp.get(i), type));
        }
        return list;
    }

    protected <T> Map<String, T> getMapValue(String key, RedisCryptor cryptor, Response gresp, Type type) {
        int gsize = gresp.size();
        if (gsize == 0) return new LinkedHashMap<>();
        Map<String, T> map = new LinkedHashMap<>();
        //resp.tostring = [0, [key1, 10, key2, 30]]
        for (int j = 0; j < gsize; j++) {
            Response resp = gresp.get(j);
            if (resp.type() != ResponseType.MULTI) continue;
            int size = resp.size();
            for (int i = 0; i < size; i += 2) {
                String bs1 = resp.get(i).toString(StandardCharsets.UTF_8);
                String bs2 = resp.get(i + 1).toString(StandardCharsets.UTF_8);
                T val = getObjectValue(key, cryptor, bs2, type);
                if (val != null) map.put(getObjectValue(key, cryptor, bs1, String.class).toString(), val);
            }
        }
        return map;
    }

    protected String[] keyArgs(boolean set, String key) {
        if (set) return new String[]{key};
        return new String[]{key, "0", "-1"};
    }

    protected String formatValue(long value) {
        return String.valueOf(value);
    }

    protected String formatValue(String key, RedisCryptor cryptor, String value) {
        return encryptValue(key, cryptor, value);
    }

    protected String formatValue(String key, RedisCryptor cryptor, Object value) {
        return formatValue(key, cryptor, null, null, value);
    }

    protected String formatValue(String key, RedisCryptor cryptor, Convert convert0, Type type, Object value) {
        if (value == null) throw new NullPointerException();
        if (value instanceof byte[]) return new String((byte[]) value, StandardCharsets.UTF_8);
        if (convert0 == null) {
            if (convert == null) convert = JsonConvert.root(); //compile模式下convert可能为null
            convert0 = convert;
        }
        if (type == null) type = value.getClass();
        Class clz = value.getClass();
        if (clz == String.class || clz == Long.class
            || Number.class.isAssignableFrom(clz) || CharSequence.class.isAssignableFrom(clz)) {
            return String.valueOf(value);
        }
        String val = (convert0 instanceof JsonConvert) ? ((JsonConvert) convert0).convertTo(type, value) : new String(convert0.convertToBytes(type, value), StandardCharsets.UTF_8);
        return encryptValue(key, cryptor, val);
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        return sendAsync(Command.EXISTS, key).thenApply(v -> getBooleanValue(v));
    }

    @Override
    public boolean exists(String key) {
        return existsAsync(key).join();
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        return sendAsync(Command.GET, key).thenApply(v -> getObjectValue(key, cryptor, v, type));
    }

    @Override
    public CompletableFuture<String> getStringAsync(String key) {
        return sendAsync(Command.GET, key).thenApply(v -> getStringValue(key, cryptor, v));
    }

    @Override
    public CompletableFuture<Long> getLongAsync(String key, long defValue) {
        return sendAsync(Command.GET, key).thenApply(v -> getLongValue(v, defValue));
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

    //--------------------- getAndRefresh ------------------------------
    @Override
    public <T> CompletableFuture<T> getAndRefreshAsync(String key, int expireSeconds, final Type type) {
        return refreshAsync(key, expireSeconds).thenCompose(v -> getAsync(key, type));
    }

    @Override
    public <T> T getAndRefresh(String key, final int expireSeconds, final Type type) {
        return (T) getAndRefreshAsync(key, expireSeconds, type).join();
    }

    @Override
    public CompletableFuture<String> getStringAndRefreshAsync(String key, int expireSeconds) {
        return refreshAsync(key, expireSeconds).thenCompose(v -> getStringAsync(key));
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
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert, T value) {
        return sendAsync(Command.SET, key, formatValue(key, cryptor, convert, value.getClass(), value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, final Type type, T value) {
        return sendAsync(Command.SET, key, formatValue(key, cryptor, (Convert) null, type, value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert, final Type type, T value) {
        return sendAsync(Command.SET, key, formatValue(key, cryptor, convert, type, value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, final Type type, T value) {
        return sendAsync(Command.GETSET, key, formatValue(key, cryptor, convert, type, value)).thenApply(v -> getObjectValue(key, cryptor, v, type));
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, Convert convert0, final Type type, T value) {
        return sendAsync(Command.GETSET, key, formatValue(key, cryptor, convert0, type, value)).thenApply(v -> getObjectValue(key, cryptor, v, type));
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
    public <T> void set(final String key, final Convert convert, final Type type, T value) {
        setAsync(key, convert, type, value).join();
    }

    @Override
    public <T> T getSet(final String key, final Type type, T value) {
        return getSetAsync(key, type, value).join();
    }

    @Override
    public <T> T getSet(String key, final Convert convert, final Type type, T value) {
        return getSetAsync(key, convert, type, value).join();
    }

    @Override
    public CompletableFuture<Void> setStringAsync(String key, String value) {
        return sendAsync(Command.SET, key, formatValue(key, cryptor, value)).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<String> getSetStringAsync(String key, String value) {
        return sendAsync(Command.GETSET, key, formatValue(key, cryptor, value)).thenApply(v -> getStringValue(key, cryptor, v));
    }

    @Override
    public void setString(String key, String value) {
        setStringAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> setLongAsync(String key, long value) {
        return sendAsync(Command.SET, key, formatValue(value)).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Long> getSetLongAsync(String key, long value, long defvalue) {
        return sendAsync(Command.GETSET, key, formatValue(value)).thenApply(v -> getLongValue(v, defvalue));
    }

    @Override
    public void setLong(String key, long value) {
        setLongAsync(key, value).join();
    }

    //--------------------- set ------------------------------    
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
        return sendAsync(Command.EXPIRE, key, String.valueOf(expireSeconds)).thenApply(v -> null);
    }

    @Override
    public void setExpireSeconds(String key, int expireSeconds) {
        setExpireSecondsAsync(key, expireSeconds).join();
    }

    //--------------------- remove ------------------------------    
    @Override
    public CompletableFuture<Integer> removeAsync(String key) {
        return sendAsync(Command.DEL, key).thenApply(v -> v.toInteger());
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
        return sendAsync(Command.INCR, key).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public long incr(final String key, long num) {
        return incrAsync(key, num).join();
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key, long num) {
        return sendAsync(Command.INCRBY, key, String.valueOf(num)).thenApply(v -> getLongValue(v, 0L));
    }

    //--------------------- decr ------------------------------    
    @Override
    public long decr(final String key) {
        return decrAsync(key).join();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return sendAsync(Command.DECR, key).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public long decr(final String key, long num) {
        return decrAsync(key, num).join();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key, long num) {
        return sendAsync(Command.DECRBY, key, String.valueOf(num)).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public int hremove(final String key, String... fields) {
        return hremoveAsync(key, fields).join();
    }

    @Override
    public int hsize(final String key) {
        return hsizeAsync(key).join();
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
        String[] args = new String[fields.length + 1];
        args[0] = key;
        System.arraycopy(fields, 0, args, 1, fields.length);
        return sendAsync(Command.HDEL, args).thenApply(v -> getIntValue(v, 0));
    }

    @Override
    public CompletableFuture<Integer> hsizeAsync(final String key) {
        return sendAsync(Command.HLEN, key).thenApply(v -> getIntValue(v, 0));
    }

    @Override
    public CompletableFuture<List<String>> hkeysAsync(final String key) {
        return sendAsync(Command.HKEYS, key).thenApply(v -> (List) getCollectionValue(key, cryptor, v, false, String.class));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field) {
        return hincrAsync(key, field, 1);
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field, long num) {
        return sendAsync(Command.HINCRBY, key, field, String.valueOf(num)).thenApply(v -> getLongValue(v, 0L));
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
        return sendAsync(Command.HEXISTS, key, field).thenApply(v -> getIntValue(v, 0) > 0);
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert, final T value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        return sendAsync(Command.HSET, key, field, formatValue(key, cryptor, convert, null, value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Type type, final T value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        return sendAsync(Command.HSET, key, field, formatValue(key, cryptor, null, type, value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert, final Type type, final T value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        return sendAsync(Command.HSET, key, field, formatValue(key, cryptor, convert, type, value)).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> hsetStringAsync(final String key, final String field, final String value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        return sendAsync(Command.HSET, key, field, formatValue(key, cryptor, value)).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> hsetLongAsync(final String key, final String field, final long value) {
        return sendAsync(Command.HSET, key, field, formatValue(value)).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(final String key, final Serializable... values) {
        String[] args = new String[values.length + 1];
        args[0] = key;
        for (int i = 0; i < values.length; i += 2) {
            args[i + 1] = String.valueOf(values[i]);
            args[i + 2] = formatValue(key, cryptor, values[i + 1]);
        }
        return sendAsync(Command.HMSET, args).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<List<Serializable>> hmgetAsync(final String key, final Type type, final String... fields) {
        String[] args = new String[fields.length + 1];
        args[0] = key;
        for (int i = 0; i < fields.length; i++) {
            args[i + 1] = fields[i];
        }
        return sendAsync(Command.HMGET, args).thenApply(v -> (List) getCollectionValue(key, cryptor, v, false, type));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(final String key, final Type type, int offset, int limit) {
        return hmapAsync(key, type, offset, limit, null);
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(final String key, final Type type, int offset, int limit, String pattern) {
        String[] args = new String[pattern == null || pattern.isEmpty() ? 4 : 6];
        int index = -1;
        args[++index] = key;
        args[++index] = String.valueOf(offset);
        if (pattern != null && !pattern.isEmpty()) {
            args[++index] = "MATCH";
            args[++index] = pattern;
        }
        args[++index] = "COUNT";
        args[++index] = String.valueOf(limit);
        return sendAsync(Command.HSCAN, args).thenApply(v -> getMapValue(key, cryptor, v, type));
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(final String key, final String field, final Type type) {
        return sendAsync(Command.HGET, key, field).thenApply(v -> getObjectValue(key, cryptor, v, type));
    }

    @Override
    public CompletableFuture<String> hgetStringAsync(final String key, final String field) {
        return sendAsync(Command.HGET, key, field).thenApply(v -> getStringValue(key, cryptor, v));
    }

    @Override
    public CompletableFuture<Long> hgetLongAsync(final String key, final String field, long defValue) {
        return sendAsync(Command.HGET, key, field).thenApply(v -> getLongValue(v, defValue));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Integer> getCollectionSizeAsync(String key) {
        return sendAsync(Command.TYPE, key).thenCompose(t -> {
            String type = t.toString();
            if (type == null) return CompletableFuture.completedFuture(0);
            return sendAsync(type.contains("list") ? Command.LLEN : Command.SCARD, key).thenApply(v -> getIntValue(v, 0));
        });
    }

    @Override
    public int getCollectionSize(String key) {
        return getCollectionSizeAsync(key).join();
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return sendAsync(Command.TYPE, key).thenCompose(t -> {
            String type = t.toString();
            if (type == null) return CompletableFuture.completedFuture(null);
            boolean set = !type.contains("list");
            return sendAsync(set ? Command.SMEMBERS : Command.LRANGE, keyArgs(set, key)).thenApply(v -> getCollectionValue(key, cryptor, v, set, componentType));
        });
    }

    @Override
    public CompletableFuture<Map<String, Long>> getLongMapAsync(String... keys) {
        return sendAsync(Command.MGET, keys).thenApply(v -> {
            List list = (List) getCollectionValue(null, null, v, false, long.class);
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
        return sendAsync(Command.MGET, keys).thenApply(v -> {
            List list = (List) getCollectionValue(null, null, v, false, long.class);
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
        return sendAsync(Command.MGET, keys).thenApply(v -> {
            List list = (List) getCollectionValue(keys[0], cryptor, v, false, String.class);
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
        return sendAsync(Command.MGET, keys).thenApply(v -> {
            List list = (List) getCollectionValue(keys[0], cryptor, v, false, String.class);
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
        return sendAsync(Command.MGET, keys).thenApply(v -> {
            List list = (List) getCollectionValue(keys[0], cryptor, v, false, componentType);
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
        final Map<String, Collection<T>> map = new LinkedHashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(set ? Command.SMEMBERS : Command.LRANGE, keyArgs(set, key)).thenAccept(v -> {
                Collection c = getCollectionValue(key, cryptor, v, set, componentType);
                if (c != null) {
                    synchronized (map) {
                        map.put(key, (Collection) c);
                    }
                }
            });
        }

        CompletableFuture.allOf(futures)
            .whenComplete((w, e) -> {
                if (e != null) {
                    rsFuture.completeExceptionally(e);
                } else {
                    rsFuture.complete(map);
                }
            }
            );
        return rsFuture;
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
        return sendAsync(Command.TYPE, key).thenCompose(t -> {
            String type = t.toString();
            if (type == null) return CompletableFuture.completedFuture(null);
            boolean set = !type.contains("list");
            return sendAsync(set ? Command.SMEMBERS : Command.LRANGE, keyArgs(set, key)).thenApply(v -> getCollectionValue(key, cryptor, v, set, String.class));
        });
    }

    @Override
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<String>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<String>> map = new LinkedHashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(set ? Command.SMEMBERS : Command.LRANGE, keyArgs(set, key)).thenAccept(v -> {
                Collection<String> c = getCollectionValue(key, cryptor, v, set, String.class);
                if (c != null) {
                    synchronized (map) {
                        map.put(key, (Collection) c);
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
        return sendAsync(Command.TYPE, key).thenCompose(t -> {
            String type = t.toString();
            if (type == null) return CompletableFuture.completedFuture(null);
            boolean set = !type.contains("list");
            return sendAsync(set ? Command.SMEMBERS : Command.LRANGE, keyArgs(set, key)).thenApply(v -> getCollectionValue(key, cryptor, v, set, long.class));
        });
    }

    @Override
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(final boolean set, String... keys) {
        final CompletableFuture<Map<String, Collection<Long>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<Long>> map = new LinkedHashMap<>();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(set ? Command.SMEMBERS : Command.LRANGE, keyArgs(set, key)).thenAccept(v -> {
                Collection<String> c = getCollectionValue(key, cryptor, v, set, long.class);
                if (c != null) {
                    synchronized (map) {
                        map.put(key, (Collection) c);
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
        return existsSetItemAsync(key, componentType, value).join();
    }

    @Override
    public <T> CompletableFuture<Boolean> existsSetItemAsync(String key, final Type componentType, T value) {
        return sendAsync(Command.SISMEMBER, key, formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> getIntValue(v, 0) > 0);
    }

    @Override
    public boolean existsStringSetItem(String key, String value) {
        return existsStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> existsStringSetItemAsync(String key, String value) {
        return sendAsync(Command.SISMEMBER, key, formatValue(key, cryptor, value)).thenApply(v -> getIntValue(v, 0) > 0);
    }

    @Override
    public boolean existsLongSetItem(String key, long value) {
        return existsLongSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Boolean> existsLongSetItemAsync(String key, long value) {
        return sendAsync(Command.SISMEMBER, key, formatValue(value)).thenApply(v -> getIntValue(v, 0) > 0);
    }

    //--------------------- appendListItem ------------------------------  
    @Override
    public <T> CompletableFuture<Void> appendListItemAsync(String key, final Type componentType, T value) {
        return sendAsync(Command.RPUSH, key, formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> null);
    }

    @Override
    public <T> void appendListItem(String key, final Type componentType, T value) {
        appendListItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Void> appendStringListItemAsync(String key, String value) {
        return sendAsync(Command.RPUSH, key, formatValue(key, cryptor, value)).thenApply(v -> null);
    }

    @Override
    public void appendStringListItem(String key, String value) {
        appendStringListItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> appendLongListItemAsync(String key, long value) {
        return sendAsync(Command.RPUSH, key, formatValue(value)).thenApply(v -> null);
    }

    @Override
    public void appendLongListItem(String key, long value) {
        appendLongListItemAsync(key, value).join();
    }

    //--------------------- removeListItem ------------------------------  
    @Override
    public <T> CompletableFuture<Integer> removeListItemAsync(String key, final Type componentType, T value) {
        return sendAsync(Command.LREM, key, "0", formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> getIntValue(v, 0));
    }

    @Override
    public <T> int removeListItem(String key, final Type componentType, T value) {
        return removeListItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Integer> removeStringListItemAsync(String key, String value) {
        return sendAsync(Command.LREM, key, "0", formatValue(key, cryptor, value)).thenApply(v -> getIntValue(v, 0));
    }

    @Override
    public int removeStringListItem(String key, String value) {
        return removeStringListItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Integer> removeLongListItemAsync(String key, long value) {
        return sendAsync(Command.LREM, key, "0", formatValue(value)).thenApply(v -> getIntValue(v, 0));
    }

    @Override
    public int removeLongListItem(String key, long value) {
        return removeLongListItemAsync(key, value).join();
    }

    //--------------------- appendSetItem ------------------------------  
    @Override
    public <T> CompletableFuture<Void> appendSetItemAsync(String key, Type componentType, T value) {
        return sendAsync(Command.SADD, key, formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<T> spopSetItemAsync(String key, Type componentType) {
        return sendAsync(Command.SPOP, key).thenApply(v -> getObjectValue(key, cryptor, v, componentType));
    }

    @Override
    public <T> CompletableFuture<Set<T>> spopSetItemAsync(String key, int count, Type componentType) {
        return sendAsync(Command.SPOP, key, String.valueOf(count)).thenApply(v -> getObjectValue(key, cryptor, v, componentType));
    }

    @Override
    public CompletableFuture<String> spopStringSetItemAsync(String key) {
        return sendAsync(Command.SPOP, key).thenApply(v -> getStringValue(key, cryptor, v));
    }

    @Override
    public CompletableFuture<Set<String>> spopStringSetItemAsync(String key, int count) {
        return sendAsync(Command.SPOP, key, String.valueOf(count)).thenApply(v -> (Set) getCollectionValue(key, cryptor, v, true, String.class));
    }

    @Override
    public CompletableFuture<Long> spopLongSetItemAsync(String key) {
        return sendAsync(Command.SPOP, key).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Set<Long>> spopLongSetItemAsync(String key, int count) {
        return sendAsync(Command.SPOP, key, String.valueOf(count)).thenApply(v -> (Set) getCollectionValue(key, cryptor, v, true, long.class));
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
    public <T> Set<T> spopSetItem(String key, int count, final Type componentType) {
        return (Set) spopSetItemAsync(key, count, componentType).join();
    }

    @Override
    public String spopStringSetItem(String key) {
        return spopStringSetItemAsync(key).join();
    }

    @Override
    public Set<String> spopStringSetItem(String key, int count) {
        return spopStringSetItemAsync(key, count).join();
    }

    @Override
    public Long spopLongSetItem(String key) {
        return spopLongSetItemAsync(key).join();
    }

    @Override
    public Set<Long> spopLongSetItem(String key, int count) {
        return spopLongSetItemAsync(key, count).join();
    }

    @Override
    public CompletableFuture<Void> appendStringSetItemAsync(String key, String value) {
        return sendAsync(Command.SADD, key, formatValue(key, cryptor, value)).thenApply(v -> null);
    }

    @Override
    public void appendStringSetItem(String key, String value) {
        appendStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> appendLongSetItemAsync(String key, long value) {
        return sendAsync(Command.SADD, key, formatValue(value)).thenApply(v -> null);
    }

    @Override
    public void appendLongSetItem(String key, long value) {
        appendLongSetItemAsync(key, value).join();
    }

    //--------------------- removeSetItem ------------------------------  
    @Override
    public <T> CompletableFuture<Integer> removeSetItemAsync(String key, final Type componentType, T value) {
        return sendAsync(Command.SREM, key, formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> getIntValue(v, 0));
    }

    @Override
    public <T> int removeSetItem(String key, final Type componentType, T value) {
        return removeSetItemAsync(key, componentType, value).join();
    }

    @Override
    public CompletableFuture<Integer> removeStringSetItemAsync(String key, String value) {
        return sendAsync(Command.SREM, key, formatValue(key, cryptor, value)).thenApply(v -> getIntValue(v, 0));
    }

    @Override
    public int removeStringSetItem(String key, String value) {
        return removeStringSetItemAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Integer> removeLongSetItemAsync(String key, long value) {
        return sendAsync(Command.SREM, key, formatValue(value)).thenApply(v -> getIntValue(v, 0));
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
    public byte[] getBytes(final String key) {
        return getBytesAsync(key).join();
    }

    @Override
    public byte[] getSetBytes(final String key, byte[] value) {
        return getSetBytesAsync(key, value).join();
    }

    @Override
    public byte[] getBytesAndRefresh(final String key, final int expireSeconds) {
        return getBytesAndRefreshAsync(key, expireSeconds).join();
    }

    @Override
    public void setBytes(final String key, final byte[] value) {
        setBytesAsync(key, value).join();
    }

    @Override
    public void setBytes(final int expireSeconds, final String key, final byte[] value) {
        setBytesAsync(expireSeconds, key, value).join();
    }

    @Override
    public <T> void setBytes(final String key, final Convert convert, final Type type, final T value) {
        setBytesAsync(key, convert, type, value).join();
    }

    @Override
    public <T> void setBytes(final int expireSeconds, final String key, final Convert convert, final Type type, final T value) {
        setBytesAsync(expireSeconds, key, convert, type, value).join();
    }

    @Override
    public CompletableFuture<byte[]> getBytesAsync(final String key) {
        return sendAsync(Command.GET, key).thenApply(v -> v.toBytes());
    }

    @Override
    public CompletableFuture<byte[]> getSetBytesAsync(final String key, byte[] value) {
        return sendAsync(Command.GETSET, key, new String(value, StandardCharsets.UTF_8)).thenApply(v -> v.toBytes());
    }

    @Override
    public CompletableFuture<byte[]> getBytesAndRefreshAsync(final String key, final int expireSeconds) {
        return refreshAsync(key, expireSeconds).thenCompose(v -> getBytesAsync(key));
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(final String key, final byte[] value) {
        return sendAsync(Command.SET, key, new String(value, StandardCharsets.UTF_8)).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(final int expireSeconds, final String key, final byte[] value) {
        return (CompletableFuture) setBytesAsync(key, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public <T> CompletableFuture<Void> setBytesAsync(final String key, final Convert convert, final Type type, final T value) {
        return sendAsync(Command.SET, key, new String(convert.convertToBytes(type, value), StandardCharsets.UTF_8)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> setBytesAsync(final int expireSeconds, final String key, final Convert convert, final Type type, final T value) {
        return (CompletableFuture) setBytesAsync(key, convert.convertToBytes(type, value)).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public CompletableFuture<List<String>> queryKeysAsync() {
        return sendAsync(Command.KEYS, "*").thenApply(v -> (List) getCollectionValue(null, null, v, false, String.class));
    }

    @Override
    public CompletableFuture<List<String>> queryKeysStartsWithAsync(String startsWith) {
        if (startsWith == null) return queryKeysAsync();
        String key = startsWith + "*";
        return sendAsync(Command.KEYS, key).thenApply(v -> (List) getCollectionValue(null, null, v, false, String.class));
    }

    @Override
    public CompletableFuture<List<String>> queryKeysEndsWithAsync(String endsWith) {
        if (endsWith == null) return queryKeysAsync();
        String key = "*" + endsWith;
        return sendAsync(Command.KEYS, key).thenApply(v -> (List) getCollectionValue(null, null, v, false, String.class
        ));
    }

    //--------------------- getKeySize ------------------------------  
    @Override
    public int getKeySize() {
        return getKeySizeAsync().join();
    }

    @Override
    public CompletableFuture<Integer> getKeySizeAsync() {
        return sendAsync(Command.DBSIZE).thenApply(v -> getIntValue(v, 0));
    }

}
