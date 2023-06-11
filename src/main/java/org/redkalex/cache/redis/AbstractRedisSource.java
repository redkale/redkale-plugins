/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.redkalex.cache.redis;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.redkale.annotation.Resource;
import org.redkale.convert.Convert;
import org.redkale.convert.json.JsonConvert;
import org.redkale.source.AbstractCacheSource;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 *
 * @since 2.8.0
 */
public abstract class AbstractRedisSource extends AbstractCacheSource {

    public static final String CACHE_SOURCE_CRYPTOR = "cryptor";

    protected String name;

    @Resource(required = false)
    protected ResourceFactory resourceFactory;

    @Resource(required = false)
    protected JsonConvert defaultConvert;

    @Resource(name = "$_convert", required = false)
    protected JsonConvert convert;

    protected int db;

    protected RedisCryptor cryptor;

    protected AnyValue config;

    @Override
    public void init(AnyValue conf) {
        this.config = conf;
        super.init(conf);
        this.name = conf.getValue("name", "");
        if (this.convert == null) {
            this.convert = this.defaultConvert;
        }
        if (conf != null) {
            String cryptStr = conf.getValue(CACHE_SOURCE_CRYPTOR, "").trim();
            if (!cryptStr.isEmpty()) {
                try {
                    Class<RedisCryptor> cryptClass = (Class) getClass().getClassLoader().loadClass(cryptStr);
                    RedkaleClassLoader.putReflectionPublicConstructors(cryptClass, cryptClass.getName());
                    this.cryptor = cryptClass.getConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new RedkaleException(e);
                }
            }
        }
        if (cryptor != null) {
            if (resourceFactory != null) {
                resourceFactory.inject(cryptor);
            }
            cryptor.init(conf);
        }
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (cryptor != null) {
            cryptor.destroy(conf);
        }
    }

    @Override
    public void close() throws Exception {  //在 Application 关闭时调用
        destroy(null);
    }

    @Override
    public String resourceName() {
        return name;
    }

    protected String decryptValue(String key, RedisCryptor cryptor, String value) {
        return cryptor != null ? cryptor.decrypt(key, value) : value;
    }

    protected <T> T decryptValue(String key, RedisCryptor cryptor, Type type, byte[] bs) {
        return decryptValue(key, cryptor, convert, type, bs);
    }

    protected <T> T decryptValue(String key, RedisCryptor cryptor, Convert c, Type type, byte[] bs) {
        if (bs == null) {
            return null;
        }
        if (type == byte[].class) {
            return (T) bs;
        }
        if (cryptor == null && type == String.class) {
            return (T) new String(bs, StandardCharsets.UTF_8);
        }
        if (cryptor == null || (type instanceof Class && (((Class) type).isPrimitive() || Number.class.isAssignableFrom((Class) type)))) {
            return (T) (c == null ? this.convert : c).convertFrom(type, bs);
        }
        String deval = cryptor.decrypt(key, new String(bs, StandardCharsets.UTF_8));
        if (type == String.class) {
            return (T) deval;
        }
        return deval == null ? null : (T) (c == null ? this.convert : c).convertFrom(type, deval.getBytes(StandardCharsets.UTF_8));
    }

    protected String encryptValue(String key, RedisCryptor cryptor, String value) {
        return cryptor != null ? cryptor.encrypt(key, value) : value;
    }

    protected <T> byte[] encryptValue(String key, RedisCryptor cryptor, Convert c, T value) {
        return encryptValue(key, cryptor, null, c, value);
    }

    protected <T> byte[] encryptValue(String key, RedisCryptor cryptor, Type type, Convert c, T value) {
        if (value == null) {
            return null;
        }
        Type t = type == null ? value.getClass() : type;
        if (cryptor == null && t == String.class) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
        return encryptValue(key, cryptor, t, (c == null ? this.convert : c).convertToBytes(t, value));
    }

    protected byte[] encryptValue(String key, RedisCryptor cryptor, Type type, byte[] bs) {
        if (bs == null) {
            return null;
        }
        if (cryptor == null || (type instanceof Class && (((Class) type).isPrimitive() || Number.class.isAssignableFrom((Class) type)))) {
            return bs;
        }
        String enval = cryptor.encrypt(key, new String(bs, StandardCharsets.UTF_8));
        return enval == null ? null : enval.getBytes(StandardCharsets.UTF_8);
    }

    //------------------------ 默认同步接口 ------------------------    
    @Override
    public boolean exists(String key) {
        return existsAsync(key).join();
    }

    @Override
    public <T> T get(String key, final Type type) {
        return (T) getAsync(key, type).join();
    }

    @Override
    public <T> T getex(String key, final int expireSeconds, final Type type) {
        return (T) getexAsync(key, expireSeconds, type).join();
    }

    @Override
    public void mset(final Serializable... keyVals) {
        msetAsync(keyVals).join();
    }

    @Override
    public void mset(final Map map) {
        msetAsync(map).join();
    }

    @Override
    public <T> void set(String key, final Convert convert, final Type type, T value) {
        setAsync(key, convert, type, value).join();
    }

    @Override
    public <T> boolean setnx(String key, final Convert convert, final Type type, T value) {
        return setnxAsync(key, convert, type, value).join();
    }

    @Override
    public <T> T getSet(String key, Convert convert, final Type type, T value) {
        return getSetAsync(key, convert, type, value).join();
    }

    @Override
    public <T> void setex(String key, int expireSeconds, Convert convert, final Type type, T value) {
        setexAsync(key, expireSeconds, convert, type, value).join();
    }

    @Override
    public <T> boolean setnxex(final String key, final int expireSeconds, final Convert convert, final Type type, final T value) {
        return setnxexAsync(key, expireSeconds, convert, type, value).join();
    }

    @Override
    public void expire(String key, int expireSeconds) {
        expireAsync(key, expireSeconds).join();
    }

    @Override
    public boolean persist(String key) {
        return persistAsync(key).join();
    }

    @Override
    public boolean rename(String oldKey, String newKey) {
        return renameAsync(oldKey, newKey).join();
    }

    @Override
    public boolean renamenx(String oldKey, String newKey) {
        return renamenxAsync(oldKey, newKey).join();
    }

    @Override
    public long del(String... keys) {
        return delAsync(keys).join();
    }

    @Override
    public long incr(final String key) {
        return incrAsync(key).join();
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
    public long decr(final String key) {
        return decrAsync(key).join();
    }

    @Override
    public long decrby(final String key, long num) {
        return decrbyAsync(key, num).join();
    }

    @Override
    public long hdel(final String key, String... fields) {
        return hdelAsync(key, fields).join();
    }

    @Override
    public long hlen(final String key) {
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
    public <T> void hset(final String key, final String field, final Convert convert, final Type type, final T value) {
        hsetAsync(key, field, convert, type, value).join();
    }

    @Override
    public <T> boolean hsetnx(final String key, final String field, final Convert convert, final Type type, final T value) {
        return hsetnxAsync(key, field, convert, type, value).join();
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
        return (List) hmgetAsync(key, type, fields).join();
    }

    @Override
    public <T> Map<String, T> hscan(final String key, final Type type, AtomicLong cursor, int limit, String pattern) {
        return (Map) hscanAsync(key, type, cursor, limit, pattern).join();
    }

    @Override
    public <T> T hget(final String key, final String field, final Type type) {
        return (T) hgetAsync(key, field, type).join();
    }

    @Override
    public <T> Map<String, T> hgetall(final String key, final Type type) {
        return (Map) hgetallAsync(key, type).join();
    }

    @Override
    public <T> List<T> hvals(final String key, final Type type) {
        return (List) hvalsAsync(key, type).join();
    }

    @Override
    public long llen(String key) {
        return llenAsync(key).join();
    }

    @Override
    public long scard(String key) {
        return scardAsync(key).join();
    }

    @Override
    public <T> Set<T> smembers(String key, final Type componentType) {
        return (Set) smembersAsync(key, componentType).join();
    }

    @Override
    public <T> List<T> lrange(String key, final Type componentType, int start, int stop) {
        return (List) lrangeAsync(key, componentType, start, stop).join();
    }

    @Override
    public void ltrim(String key, int start, int stop) {
        ltrimAsync(key, start, stop).join();
    }

    @Override
    public <T> T lpop(String key, final Type componentType) {
        return (T) lpopAsync(key, componentType).join();
    }

    @Override
    public <T> void lpush(final String key, final Type componentType, T... values) {
        lpushAsync(key, componentType, values).join();
    }

    @Override
    public <T> void lpushx(final String key, final Type componentType, T... values) {
        lpushxAsync(key, componentType, values).join();
    }

    @Override
    public <T> T rpop(String key, final Type componentType) {
        return (T) rpopAsync(key, componentType).join();
    }

    @Override
    public <T> T rpoplpush(final String list1, final String list2, final Type componentType) {
        return (T) rpoplpushAsync(list1, list2, componentType).join();
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

    @Override
    public <T> boolean sismember(String key, final Type componentType, T value) {
        return sismemberAsync(key, componentType, value).join();
    }

    @Override
    public <T> void rpush(String key, final Type componentType, T... values) {
        rpushAsync(key, componentType, values).join();
    }

    @Override
    public <T> void rpushx(String key, final Type componentType, T... values) {
        rpushxAsync(key, componentType, values).join();
    }

    @Override
    public <T> int lrem(String key, final Type componentType, T value) {
        return lremAsync(key, componentType, value).join();
    }

    @Override
    public <T> void sadd(String key, final Type componentType, T... values) {
        saddAsync(key, componentType, values).join();
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
    public <T> Set< T> sscan(final String key, final Type componentType, AtomicLong cursor, int limit, String pattern) {
        return (Set) sscanAsync(key, componentType, cursor, limit, pattern).join();
    }

    @Override
    public <T> long srem(String key, final Type componentType, T... values) {
        return sremAsync(key, componentType, values).join();
    }

    @Override
    public List<String> keys(String pattern) {
        return keysAsync(pattern).join();
    }

    @Override
    public List<String> scan(AtomicLong cursor, int limit, String pattern) {
        return scanAsync(cursor, limit, pattern).join();
    }

    @Override
    public long dbsize() {
        return dbsizeAsync().join();
    }

    @Override
    public void flushdb() {
        flushdbAsync().join();
    }

    @Override
    public void flushall() {
        flushallAsync().join();
    }

}
