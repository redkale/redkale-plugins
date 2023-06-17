/*
 */
package org.redkalex.cache.redis;

import io.vertx.core.*;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.*;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceListener;
import org.redkale.annotation.ResourceType;
import org.redkale.convert.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.Local;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_MAXCONNS;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkale.util.Utility.*;

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
        if (conf == null) {
            conf = AnyValue.create();
        }
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
                if (!db0.isEmpty()) {
                    db = Integer.valueOf(db0);
                }
            }
            URI uri = URI.create(addrstr);
            if (isNotEmpty(uri.getQuery())) {
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
        RedisOptions redisConfig = new RedisOptions();
        if (maxconns > 0) {
            redisConfig.setMaxPoolSize(maxconns);
        }
        if (password != null) {
            redisConfig.setPassword(password.trim());
        }
        redisConfig.setMaxPoolWaiting(-1);
//        if (maxconns > 0) {
//            redisConfig.setMaxPoolWaiting(maxconns != Utility.cpus() ? maxconns : maxconns * 10);
//        }
        redisConfig.setEndpoints(addrs);
        if (this.vertx == null) {
            this.vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(Utility.cpus()).setPreferNativeTransport(true));
        }
        RedisAPI old = this.client;
        this.client = RedisAPI.api(Redis.createClient(this.vertx, redisConfig));
        if (old != null) {
            old.close();
        }
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
            String val = node.getValue(CACHE_SOURCE_URL);
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
    public io.vertx.redis.client.RedisAPI getRedisClient() {
        return this.client;
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (client != null) {
            client.close();
        }
        if (this.vertx != null) {
            this.vertx.close();
        }
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
        if (resp == null) {
            return false;
        }
        Boolean v = resp.toBoolean();
        return v == null ? false : v;
    }

    protected String getStringValue(String key, RedisCryptor cryptor, Response resp) {
        if (resp == null) {
            return null;
        }
        String val = resp.toString(StandardCharsets.UTF_8);
        if (cryptor == null) {
            return val;
        }
        return cryptor.decrypt(key, val);
    }

    protected Long getLongValue(Response resp, Long defvalue) {
        if (resp == null) {
            return defvalue;
        }
        Long v = resp.toLong();
        return v == null ? defvalue : v;
    }

    protected Double getDoubleValue(Response resp, Double defvalue) {
        if (resp == null) {
            return defvalue;
        }
        Double v = resp.toDouble();
        return v == null ? defvalue : v;
    }

    protected Integer getIntValue(Response resp, Integer defvalue) {
        if (resp == null) {
            return defvalue;
        }
        Integer v = resp.toInteger();
        return v == null ? defvalue : v;
    }

    protected Boolean getBoolValue(Response resp) {
        if (resp == null) {
            return false;
        }
        Integer v = resp.toInteger();
        return v == null ? false : v > 0;
    }

    protected <T> T getObjectValue(String key, RedisCryptor cryptor, String bs, Type type) {
        if (bs == null) {
            return null;
        }
        if (type == byte[].class) {
            return (T) bs.getBytes(StandardCharsets.UTF_8);
        }
        if (type == String.class) {
            return (T) decryptValue(key, cryptor, bs);
        }
        if (type == long.class || type == Long.class) {
            return (T) (Long) Long.parseLong(bs);
        }
        return (T) JsonConvert.root().convertFrom(type, decryptValue(key, cryptor, bs));
    }

    protected <T> T getObjectValue(String key, RedisCryptor cryptor, byte[] bs, Type type) {
        if (bs == null) {
            return null;
        }
        if (type == byte[].class) {
            return (T) bs;
        }
        if (type == String.class) {
            return (T) decryptValue(key, cryptor, new String(bs, StandardCharsets.UTF_8));
        }
        if (type == long.class || type == Long.class) {
            return (T) (Long) Long.parseLong(new String(bs, StandardCharsets.UTF_8));
        }
        if (type == double.class || type == Double.class) {
            return (T) (Double) Double.parseDouble(new String(bs, StandardCharsets.UTF_8));
        }
        return (T) JsonConvert.root().convertFrom(type, decryptValue(key, cryptor, new String(bs, StandardCharsets.UTF_8)));
    }

    protected <T> T getObjectValue(String key, RedisCryptor cryptor, Response resp, Type type) {
        if (resp == null) {
            return null;
        }
        if (type == boolean.class || type == Boolean.class) {
            return (T) resp.toBoolean();
        }
        if (resp.type() == ResponseType.NUMBER) {
            if (type == short.class || type == Short.class) {
                return (T) resp.toShort();
            } else if (type == int.class || type == Integer.class) {
                return (T) resp.toInteger();
            } else if (type == long.class || type == Long.class) {
                return (T) resp.toLong();
            } else if (type == float.class || type == Float.class) {
                return (T) resp.toFloat();
            } else if (type == double.class || type == Double.class) {
                return (T) resp.toDouble();
            } else if (type == BigInteger.class) {
                return (T) resp.toBigInteger();
            } else {
                return (T) resp.toNumber();
            }
        }
        return getObjectValue(key, cryptor, resp.toString(StandardCharsets.UTF_8), type);
    }

    protected <T> Collection<T> getCollectionValue(String key, RedisCryptor cryptor, Response resp, boolean set, Type type) {
        int size = resp == null ? 0 : resp.size();
        if (size == 0) {
            return set ? new LinkedHashSet<>() : new ArrayList<>();
        }
        Collection<T> list = set ? new LinkedHashSet<>() : new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(getObjectValue(key, cryptor, resp.get(i), type));
        }
        return list;
    }

    protected <T> Collection<T> getCollectionValue(String key, RedisCryptor cryptor, Response gresp, AtomicLong cursor, boolean set, Type type) {
        Collection<T> list = set ? new LinkedHashSet<>() : new ArrayList<>();
        int gsize = gresp.size();
        if (gsize == 0) {
            return list;
        }
        //resp.tostring = [0, [key1, 10, key2, 30]]
        for (int j = 0; j < gsize; j++) {
            Response resp = gresp.get(j);
            if (resp.type() != ResponseType.MULTI) {
                cursor.set(Long.parseLong(new String(resp.toBytes())));
                continue;
            }
            int size = resp.size();
            for (int i = 0; i < size; i++) {
                list.add(getObjectValue(key, cryptor, resp.get(i), type));
            }
        }
        return list;
    }

    protected Collection<CacheScoredValue.NumberScoredValue> getSortedCollectionValue(String key, RedisCryptor cryptor, Response gresp, AtomicLong cursor, boolean set, Type scoreType) {
        Collection<CacheScoredValue.NumberScoredValue> list = set ? new LinkedHashSet<>() : new ArrayList<>();
        int gsize = gresp.size();
        if (gsize == 0) {
            return list;
        }
        //resp.tostring = [0, [key1, 10, key2, 30]]
        for (int j = 0; j < gsize; j++) {
            Response resp = gresp.get(j);
            if (resp.type() != ResponseType.MULTI) {
                cursor.set(Long.parseLong(new String(resp.toBytes())));
                continue;
            }
            int size = resp.size();
            for (int i = 0; i < size; i += 2) {
                String member = resp.get(i).toString(StandardCharsets.UTF_8);
                list.add(new CacheScoredValue.NumberScoredValue(getObjectValue(key, cryptor, resp.get(i + 1), scoreType), member));
            }
        }
        return list;
    }

    protected List<String> getKeysValue(Response gresp, AtomicLong cursor) {
        int gsize = gresp.size();
        if (gsize == 0) {
            return new ArrayList<>();
        }
        List<String> list = new ArrayList<>();
        for (int j = 0; j < gsize; j++) {
            Response resp = gresp.get(j);
            if (resp.type() != ResponseType.MULTI) {
                cursor.set(Long.parseLong(new String(resp.toBytes())));
                continue;
            }
            int size = resp.size();
            for (int i = 0; i < size; i++) {
                list.add(resp.get(i).toString(StandardCharsets.UTF_8));
            }
        }

        return list;
    }

    protected <T> Map<String, T> getMapValue(String key, RedisCryptor cryptor, Response gresp, AtomicLong cursor, Type type) {
        boolean asMap = cursor == null;
        int gsize = gresp.size();
        if (gsize == 0) {
            return new LinkedHashMap<>();
        }
        Map<String, T> map = new LinkedHashMap<>();
        if (asMap) {
            for (String field : gresp.getKeys()) {
                Response resp = gresp.get(field);
                T val = getObjectValue(key, cryptor, resp.toBytes(), type);
                if (val != null) {
                    map.put(field, val);
                }
            }
        } else {
            //resp.tostring = [0, [key1, 10, key2, 30]]
            for (int j = 0; j < gsize; j++) {
                Response resp = gresp.get(j);
                if (resp.type() != ResponseType.MULTI) {
                    cursor.set(Long.parseLong(new String(resp.toBytes())));
                    continue;
                }
                int size = resp.size();
                for (int i = 0; i < size; i += 2) {
                    String bs1 = resp.get(i).toString(StandardCharsets.UTF_8);
                    String bs2 = resp.get(i + 1).toString(StandardCharsets.UTF_8);
                    T val = getObjectValue(key, cryptor, bs2, type);
                    if (val != null) {
                        map.put(getObjectValue(key, cryptor, bs1, String.class).toString(), val);
                    }
                }
            }
        }
        return map;
    }

    protected String[] keyArgs(String key, int start, int stop) {
        return new String[]{key, String.valueOf(start), String.valueOf(stop)};
    }

    protected <T> String[] keyArgs(String key, Type componentType, T... values) {
        String[] strs = new String[values.length + 1];
        strs[0] = key;
        for (int i = 0; i < values.length; i++) {
            strs[i + 1] = formatValue(key, componentType, values[i]);
        }
        return strs;
    }

    protected <T> String[] keyArgs(String key, CacheScoredValue... values) {
        String[] strs = new String[values.length * 2 + 1];
        strs[0] = key;
        for (int i = 0; i < values.length; i++) {
            strs[i * 2 + 1] = values[i].getScore().toString();
            strs[i * 2 + 2] = values[i].getValue();
        }
        return strs;
    }

    protected <T> String[] keyArgs(String key, String... members) {
        String[] strs = new String[members.length + 1];
        strs[0] = key;
        for (int i = 0; i < members.length; i++) {
            strs[i + 1] = members[i];
        }
        return strs;
    }

    protected String[] keyArgs(final String key, AtomicLong cursor, int limit, String pattern) {
        int c = isNotEmpty(key) ? 2 : 1;
        if (isNotEmpty(pattern)) {
            c += 2;
        }
        if (limit > 0) {
            c += 2;
        }
        String[] bss = new String[c];
        int index = -1;
        if (isNotEmpty(key)) {
            bss[++index] = key;
        }
        bss[++index] = cursor.toString();
        if (isNotEmpty(pattern)) {
            bss[++index] = "MATCH";
            bss[++index] = pattern;
        }
        if (limit > 0) {
            bss[++index] = "COUNT";
            bss[++index] = String.valueOf(limit);
        }
        return bss;
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

    protected String formatValue(String key, Type type, Object value) {
        return formatValue(key, cryptor, null, type, value);
    }

    protected String formatValue(String key, RedisCryptor cryptor, Convert convert0, Type type, Object value) {
        if (value == null) {
            throw new NullPointerException();
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value, StandardCharsets.UTF_8);
        }
        if (convert0 == null) {
            if (convert == null) {
                convert = JsonConvert.root(); //compile模式下convert可能为null
            }
            convert0 = convert;
        }
        if (type == null) {
            type = value.getClass();
        }
        Class clz = value.getClass();
        if (clz == String.class || clz == Long.class
            || Number.class.isAssignableFrom(clz) || CharSequence.class.isAssignableFrom(clz)) {
            return String.valueOf(value);
        }
        String val = (convert0 instanceof TextConvert) ? ((TextConvert) convert0).convertTo(type, value) : new String(convert0.convertToBytes(type, value), StandardCharsets.UTF_8);
        if (val != null && val.length() > 1 && type instanceof Class && !CharSequence.class.isAssignableFrom((Class) type)) {
            if (val.charAt(0) == '"' && val.charAt(val.length() - 1) == '"') {
                val = val.substring(1, val.length() - 1);
            }
        }
        return encryptValue(key, cryptor, val);
    }

    @Override
    public CompletableFuture<Boolean> isOpenAsync() {
        return CompletableFuture.completedFuture(client != null);
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        return sendAsync(Command.EXISTS, key).thenApply(v -> getBooleanValue(v));
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        return sendAsync(Command.GET, key).thenApply(v -> getObjectValue(key, cryptor, v, type));
    }

    //--------------------- getex ------------------------------
    @Override
    public <T> CompletableFuture<T> getexAsync(String key, int expireSeconds, final Type type) {
        return sendAsync(Command.GETEX, key, "EX", String.valueOf(expireSeconds)).thenApply(v -> getObjectValue(key, cryptor, v, type));
    }

    //--------------------- set ------------------------------
    @Override
    public CompletableFuture<Void> msetAsync(final Serializable... keyVals) {
        if (keyVals.length % 2 != 0) {
            throw new RedkaleException("key value must be paired");
        }
        String[] args = new String[keyVals.length];
        for (int i = 0; i < keyVals.length; i += 2) {
            String key = keyVals[i].toString();
            Object val = keyVals[i + 1];
            args[i] = key;
            args[i + 1] = formatValue(key, cryptor, convert, val.getClass(), val);
        }
        return sendAsync(Command.MSET, args).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> msetAsync(final Map map) {
        if (isEmpty(map)) {
            return CompletableFuture.completedFuture(null);
        }
        List<String> bs = new ArrayList<>();
        map.forEach((key, val) -> {
            bs.add(key.toString());
            bs.add(formatValue(key.toString(), cryptor, convert, val.getClass(), val));
        });
        return sendAsync(Command.MSET, bs.toArray(new String[bs.size()])).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert, final Type type, T value) {
        return sendAsync(Command.SET, key, formatValue(key, cryptor, convert, type, value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxAsync(String key, Convert convert, final Type type, T value) {
        return sendAsync(Command.SETNX, key, formatValue(key, cryptor, convert, type, value)).thenApply(v -> getBoolValue(v));
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, Convert convert, final Type type, T value) {
        return sendAsync(Command.GETSET, key, formatValue(key, cryptor, convert, type, value)).thenApply(v -> getObjectValue(key, cryptor, v, type));
    }

    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, Convert convert, final Type type, T value) {
        return sendAsync(Command.SETEX, key, String.valueOf(expireSeconds), formatValue(key, cryptor, convert, type, value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxexAsync(String key, int expireSeconds, Convert convert, final Type type, T value) {
        return sendAsync(Command.SET, key, formatValue(key, cryptor, convert, type, value), "NX", "EX", String.valueOf(expireSeconds)).thenApply(v -> v != null && ("OK".equals(v.toString()) || v.toInteger() > 0));
    }

    //--------------------- expire ------------------------------    
    @Override
    public CompletableFuture<Void> expireAsync(String key, int expireSeconds) {
        return sendAsync(Command.EXPIRE, key, String.valueOf(expireSeconds)).thenApply(v -> null);
    }

    //--------------------- persist ------------------------------    
    @Override
    public CompletableFuture<Boolean> persistAsync(String key) {
        return sendAsync(Command.PERSIST, key).thenApply(v -> v != null && ("OK".equals(v.toString()) || v.toInteger() > 0));
    }

    //--------------------- rename ------------------------------    
    @Override
    public CompletableFuture<Boolean> renameAsync(String oldKey, String newKey) {
        return sendAsync(Command.RENAME, oldKey, newKey).thenApply(v -> v != null && ("OK".equals(v.toString()) || v.toInteger() > 0));
    }

    @Override
    public CompletableFuture<Boolean> renamenxAsync(String oldKey, String newKey) {
        return sendAsync(Command.RENAMENX, oldKey, newKey).thenApply(v -> v != null && ("OK".equals(v.toString()) || v.toInteger() > 0));
    }

    //--------------------- del ------------------------------    
    @Override
    public CompletableFuture<Long> delAsync(String... keys) {
        return sendAsync(Command.DEL, keys).thenApply(v -> getLongValue(v, 0L));
    }

    //--------------------- incrby ------------------------------    
    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return sendAsync(Command.INCR, key).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Long> incrbyAsync(final String key, long num) {
        return sendAsync(Command.INCRBY, key, String.valueOf(num)).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Double> incrbyFloatAsync(final String key, double num) {
        return sendAsync(Command.INCRBYFLOAT, key, String.valueOf(num)).thenApply(v -> getDoubleValue(v, 0.d));
    }

    //--------------------- decrby ------------------------------    
    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return sendAsync(Command.DECR, key).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Long> decrbyAsync(final String key, long num) {
        return sendAsync(Command.DECRBY, key, String.valueOf(num)).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Long> hdelAsync(final String key, String... fields) {
        String[] args = new String[fields.length + 1];
        args[0] = key;
        System.arraycopy(fields, 0, args, 1, fields.length);
        return sendAsync(Command.HDEL, args).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Long> hlenAsync(final String key) {
        return sendAsync(Command.HLEN, key).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<List<String>> hkeysAsync(final String key) {
        return sendAsync(Command.HKEYS, key).thenApply(v -> (List) getCollectionValue(key, cryptor, v, false, String.class));
    }

    @Override
    public CompletableFuture<Long> hincrAsync(final String key, String field) {
        return hincrbyAsync(key, field, 1);
    }

    @Override
    public CompletableFuture<Long> hincrbyAsync(final String key, String field, long num) {
        return sendAsync(Command.HINCRBY, key, field, String.valueOf(num)).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Double> hincrbyFloatAsync(final String key, String field, double num) {
        return sendAsync(Command.HINCRBYFLOAT, key, field, String.valueOf(num)).thenApply(v -> getDoubleValue(v, 0d));
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
        return sendAsync(Command.HEXISTS, key, field).thenApply(v -> getIntValue(v, 0) > 0);
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(final String key, final String field, final Convert convert, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendAsync(Command.HSET, key, field, formatValue(key, cryptor, convert, type, value)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Boolean> hsetnxAsync(final String key, final String field, final Convert convert, final Type type, final T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return sendAsync(Command.HSETNX, key, field, formatValue(key, cryptor, convert, type, value)).thenApply(v -> getBoolValue(v));
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
    public CompletableFuture<Void> hmsetAsync(final String key, final Map map) {
        if (isEmpty(map)) {
            return CompletableFuture.completedFuture(null);
        }
        List<String> bs = new ArrayList<>();
        bs.add(key);
        map.forEach((k, v) -> {
            bs.add(k.toString());
            bs.add(formatValue(k.toString(), cryptor, convert, v.getClass(), v));
        });
        return sendAsync(Command.HMSET, bs.toArray(new String[bs.size()])).thenApply(v -> null);
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
    public <T> CompletableFuture<Map<String, T>> hscanAsync(final String key, final Type type, AtomicLong cursor, int limit, String pattern) {
        return sendAsync(Command.HSCAN, keyArgs(key, cursor, limit, pattern)).thenApply(v -> getMapValue(key, cryptor, v, cursor, type));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sscanAsync(final String key, final Type componentType, AtomicLong cursor, int limit, String pattern) {
        return sendAsync(Command.SSCAN, keyArgs(key, cursor, limit, pattern)).thenApply(v -> (Set) getCollectionValue(key, cryptor, v, cursor, true, componentType));
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(final String key, final String field, final Type type) {
        return sendAsync(Command.HGET, key, field).thenApply(v -> getObjectValue(key, cryptor, v, type));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hgetallAsync(final String key, final Type type) {
        return sendAsync(Command.HGETALL, key).thenApply(v -> getMapValue(key, cryptor, v, null, type));
    }

    @Override
    public <T> CompletableFuture<List<T>> hvalsAsync(final String key, final Type type) {
        return sendAsync(Command.HVALS, key).thenApply(v -> (List) getCollectionValue(key, cryptor, v, false, type));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Long> llenAsync(String key) {
        return sendAsync(Command.TYPE, key).thenCompose(t -> sendAsync(Command.LLEN, key).thenApply(v -> getLongValue(v, 0L)));
    }

    @Override
    public CompletableFuture<Long> scardAsync(String key) {
        return sendAsync(Command.TYPE, key).thenCompose(t -> sendAsync(Command.SCARD, key).thenApply(v -> getLongValue(v, 0L)));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sdiffAsync(final String key, final Type componentType, final String... key2s) {
        return sendAsync(Command.SDIFF, Utility.append(key, key2s)).thenApply(v -> (Set) getCollectionValue(key, cryptor, v, true, componentType));
    }

    @Override
    public CompletableFuture<Long> sdiffstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return sendAsync(Command.SDIFFSTORE, Utility.append(key, srcKey, srcKey2s)).thenCompose(t -> sendAsync(Command.SCARD, key).thenApply(v -> getLongValue(v, 0L)));
    }

    @Override
    public <T> CompletableFuture<Set<T>> sinterAsync(final String key, final Type componentType, final String... key2s) {
        return sendAsync(Command.SINTER, Utility.append(key, key2s)).thenApply(v -> (Set) getCollectionValue(key, cryptor, v, true, componentType));
    }

    @Override
    public CompletableFuture<Long> sinterstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return sendAsync(Command.SINTERSTORE, Utility.append(key, srcKey, srcKey2s)).thenCompose(t -> sendAsync(Command.SCARD, key).thenApply(v -> getLongValue(v, 0L)));
    }

    @Override
    public <T> CompletableFuture<Set<T>> smembersAsync(String key, final Type componentType) {
        return sendAsync(Command.SMEMBERS, key).thenApply(v -> (Set) getCollectionValue(key, cryptor, v, true, componentType));
    }

    @Override
    public CompletableFuture<List<Boolean>> smismembersAsync(final String key, final String... members) {
        return sendAsync(Command.SMISMEMBER, Utility.append(key, members)).thenApply(v -> (List) getCollectionValue(key, cryptor, v, false, Boolean.class));
    }

    @Override
    public <T> CompletableFuture<List<T>> lrangeAsync(String key, final Type componentType, int start, int stop) {
        return sendAsync(Command.LRANGE, keyArgs(key, start, stop)).thenApply(v -> (List) getCollectionValue(key, cryptor, v, false, componentType));
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> mgetAsync(final Type componentType, String... keys) {
        return sendAsync(Command.MGET, keys).thenApply(v -> {
            List list = (List) getCollectionValue(keys[0], cryptor, v, false, componentType);
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
    public <T> CompletableFuture<Map<String, List<T>>> lrangeAsync(final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, List<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, List<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(Command.LRANGE, key, "0", "-1").thenAccept(v -> {
                List c = (List) getCollectionValue(key, cryptor, v, false, componentType);
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
    public <T> CompletableFuture<Map<String, Set<T>>> smembersAsync(final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Set<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Set<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(Command.SMEMBERS, key).thenAccept(v -> {
                Set c = (Set) getCollectionValue(key, cryptor, v, true, componentType);
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

    //--------------------- existsItem ------------------------------  
    @Override
    public <T> CompletableFuture<Boolean> sismemberAsync(String key, final Type componentType, T value) {
        return sendAsync(Command.SISMEMBER, key, formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> getIntValue(v, 0) > 0);
    }

    //--------------------- rpush ------------------------------  
    @Override
    public <T> CompletableFuture<Void> rpushAsync(String key, final Type componentType, T... values) {
        return sendAsync(Command.RPUSH, keyArgs(key, componentType, values)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> lpushAsync(String key, final Type componentType, T... values) {
        return sendAsync(Command.LPUSH, keyArgs(key, componentType, values)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> rpushxAsync(final String key, final Type componentType, T... values) {
        return sendAsync(Command.RPUSHX, keyArgs(key, componentType, values)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<Void> lpushxAsync(final String key, final Type componentType, T... values) {
        return sendAsync(Command.LPUSHX, keyArgs(key, componentType, values)).thenApply(v -> null);
    }

    //--------------------- lrem ------------------------------  
    @Override
    public <T> CompletableFuture<Long> lremAsync(String key, final Type componentType, T value) {
        return sendAsync(Command.LREM, key, "0", formatValue(key, cryptor, (Convert) null, componentType, value)).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Void> ltrimAsync(final String key, int start, int stop) {
        return sendAsync(Command.LTRIM, key, String.valueOf(start), String.valueOf(stop)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<T> rpopAsync(String key, Type componentType) {
        return sendAsync(Command.RPOP, key).thenApply(v -> getObjectValue(key, cryptor, v, componentType));
    }

    @Override
    public <T> CompletableFuture<T> lpopAsync(String key, Type componentType) {
        return sendAsync(Command.LPOP, key).thenApply(v -> getObjectValue(key, cryptor, v, componentType));
    }

    @Override
    public <T> CompletableFuture<T> rpoplpushAsync(final String key, final String key2, final Type componentType) {
        return sendAsync(Command.RPOPLPUSH, key, key2).thenApply(v -> getObjectValue(key, cryptor, v, componentType));
    }

    //--------------------- sadd ------------------------------  
    @Override
    public <T> CompletableFuture<Void> saddAsync(String key, Type componentType, T... values) {
        return sendAsync(Command.SADD, keyArgs(key, componentType, values)).thenApply(v -> null);
    }

    @Override
    public <T> CompletableFuture<T> spopAsync(String key, Type componentType) {
        return sendAsync(Command.SPOP, key).thenApply(v -> getObjectValue(key, cryptor, v, componentType));
    }

    @Override
    public <T> CompletableFuture<Set<T>> spopAsync(String key, int count, Type componentType) {
        return sendAsync(Command.SPOP, key, String.valueOf(count)).thenApply(v -> (Set) getCollectionValue(key, cryptor, v, true, componentType));
    }

    @Override
    public <T> CompletableFuture<Long> sremAsync(String key, final Type componentType, T... values) {
        return sendAsync(Command.SREM, keyArgs(key, componentType, values)).thenApply(v -> getLongValue(v, 0L));
    }

    //--------------------- sorted set ------------------------------ 
    @Override
    public CompletableFuture<Void> zaddAsync(String key, CacheScoredValue... values) {
        return sendAsync(Command.ZADD, keyArgs(key, values)).thenApply(v -> null);
    }

    @Override
    public <T extends Number> CompletableFuture<T> zincrbyAsync(String key, CacheScoredValue value) {
        return sendAsync(Command.ZINCRBY, keyArgs(key, value)).thenApply(v -> getObjectValue(key, null, v, value.getScore().getClass()));
    }

    @Override
    public CompletableFuture<Long> zremAsync(String key, String... members) {
        return sendAsync(Command.ZREM, keyArgs(key, members)).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public <T extends Number> CompletableFuture<List<T>> zmscoreAsync(String key, Class<T> scoreType, String... members) {
        return sendAsync(Command.ZMSCORE, keyArgs(key, members)).thenApply(v -> (List) getCollectionValue(key, null, v, false, scoreType));
    }

    @Override
    public <T extends Number> CompletableFuture<T> zscoreAsync(String key, Class<T> scoreType, String member) {
        return sendAsync(Command.ZSCORE, keyArgs(key, member)).thenApply(v -> getObjectValue(key, null, v, scoreType));
    }

    @Override
    public CompletableFuture<Long> zcardAsync(String key) {
        return sendAsync(Command.ZCARD, key).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Long> zrankAsync(String key, String member) {
        return sendAsync(Command.ZRANK, keyArgs(key, member)).thenApply(v -> getLongValue(v, null));
    }

    @Override
    public CompletableFuture<Long> zrevrankAsync(String key, String member) {
        return sendAsync(Command.ZREVRANK, keyArgs(key, member)).thenApply(v -> getLongValue(v, null));
    }

    @Override
    public CompletableFuture<List<String>> zrangeAsync(String key, int start, int stop) {
        return sendAsync(Command.ZRANGE, keyArgs(key, start, stop)).thenApply(v -> (List) getCollectionValue(key, (RedisCryptor) null, v, false, String.class));
    }

    @Override
    public CompletableFuture<List<CacheScoredValue.NumberScoredValue>> zscanAsync(String key, Type scoreType, AtomicLong cursor, int limit, String pattern) {
        return sendAsync(Command.ZSCAN, keyArgs(key, cursor, limit, pattern)).thenApply(v -> (List) getSortedCollectionValue(key, cryptor, v, cursor, false, scoreType));
    }

    //--------------------- keys ------------------------------  
    @Override
    public CompletableFuture<List<String>> keysAsync(String pattern) {
        return sendAsync(Command.KEYS, isEmpty(pattern) ? "*" : pattern).thenApply(v -> (List) getCollectionValue(null, null, v, false, String.class));
    }

    @Override
    public CompletableFuture<List<String>> scanAsync(AtomicLong cursor, int limit, String pattern) {
        return sendAsync(Command.SCAN, keyArgs(null, cursor, limit, pattern)).thenApply(v -> getKeysValue(v, cursor));
    }

    //--------------------- dbsize ------------------------------  
    @Override
    public CompletableFuture<Long> dbsizeAsync() {
        return sendAsync(Command.DBSIZE).thenApply(v -> getLongValue(v, 0L));
    }

    @Override
    public CompletableFuture<Void> flushdbAsync() {
        return sendAsync(Command.FLUSHDB).thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> flushallAsync() {
        return sendAsync(Command.FLUSHALL).thenApply(v -> null);
    }

    //-------------------------- 过期方法 ----------------------------------
    protected String[] keyArgs22(boolean set, String key) {
        if (set) {
            return new String[]{key};
        }
        return new String[]{key, "0", "-1"};
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return sendAsync(Command.TYPE, key).thenCompose(t -> {
            String type = t.toString();
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendAsync(set ? Command.SMEMBERS : Command.LRANGE, set ? new String[]{key} : new String[]{key, "0", "-1"})
                .thenApply(v -> getCollectionValue(key, cryptor, v, set, String.class));
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
            futures[i] = sendAsync(set ? Command.SMEMBERS : Command.LRANGE, set ? new String[]{key} : new String[]{key, "0", "-1"}).thenAccept(v -> {
                Collection<String> c = getCollectionValue(key, cryptor, v, set, String.class);
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
    public CompletableFuture<Integer> getCollectionSizeAsync(String key) {
        return sendAsync(Command.TYPE, key).thenCompose(t -> {
            String type = t.toString();
            if (type == null) {
                return CompletableFuture.completedFuture(0);
            }
            return sendAsync(type.contains("list") ? Command.LLEN : Command.SCARD, key).thenApply(v -> getIntValue(v, 0));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return sendAsync(Command.TYPE, key).thenCompose(t -> {
            String type = t.toString();
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendAsync(set ? Command.SMEMBERS : Command.LRANGE, set ? new String[]{key} : new String[]{key, "0", "-1"})
                .thenApply(v -> getCollectionValue(key, cryptor, v, set, componentType));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
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
    @Deprecated(since = "2.8.0")
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
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(final boolean set, final Type componentType, final String... keys) {
        final CompletableFuture<Map<String, Collection<T>>> rsFuture = new CompletableFuture<>();
        final Map<String, Collection<T>> map = new LinkedHashMap<>();
        final ReentrantLock mapLock = new ReentrantLock();
        final CompletableFuture[] futures = new CompletableFuture[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final String key = keys[i];
            futures[i] = sendAsync(set ? Command.SMEMBERS : Command.LRANGE, set ? new String[]{key} : new String[]{key, "0", "-1"}).thenAccept(v -> {
                Collection c = getCollectionValue(key, cryptor, v, set, componentType);
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
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<Long>> getLongCollectionAsync(String key) {
        return sendAsync(Command.TYPE, key).thenCompose(t -> {
            String type = t.toString();
            if (type == null) {
                return CompletableFuture.completedFuture(null);
            }
            boolean set = !type.contains("list");
            return sendAsync(set ? Command.SMEMBERS : Command.LRANGE, set ? new String[]{key} : new String[]{key, "0", "-1"})
                .thenApply(v -> getCollectionValue(key, cryptor, v, set, long.class));
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
            futures[i] = sendAsync(set ? Command.SMEMBERS : Command.LRANGE, set ? new String[]{key} : new String[]{key, "0", "-1"})
                .thenAccept(v -> {
                    Collection<String> c = getCollectionValue(key, cryptor, v, set, long.class);
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
