/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.redkalex.cache.redis;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import javax.annotation.Resource;
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

    @Resource
    protected ResourceFactory resourceFactory;

    @Resource
    protected JsonConvert defaultConvert;

    @Resource(name = "$_convert")
    protected JsonConvert convert;

    protected int db;

    protected RedisCryptor cryptor;

    protected AnyValue config;

    @Override
    public void init(AnyValue conf) {
        this.config = conf;
        super.init(conf);
        this.name = conf.getValue("name", "");
        if (this.convert == null) this.convert = this.defaultConvert;
        if (conf != null) {
            String cryptStr = conf.getValue(CACHE_SOURCE_CRYPTOR, "").trim();
            if (!cryptStr.isEmpty()) {
                try {
                    Class<RedisCryptor> cryptClass = (Class) getClass().getClassLoader().loadClass(cryptStr);
                    RedkaleClassLoader.putReflectionPublicConstructors(cryptClass, cryptClass.getName());
                    this.cryptor = cryptClass.getConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
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
        if (bs == null) return null;
        if (cryptor == null || (type instanceof Class && (((Class) type).isPrimitive() || Number.class.isAssignableFrom((Class) type)))) {
            return (T) (c == null ? this.convert : c).convertFrom(type, bs);
        }
        String deval = cryptor.decrypt(key, new String(bs, StandardCharsets.UTF_8));
        return deval == null ? null : (T) (c == null ? this.convert : c).convertFrom(type, deval.getBytes(StandardCharsets.UTF_8));
    }

    protected String encryptValue(String key, RedisCryptor cryptor, String value) {
        return cryptor != null ? cryptor.encrypt(key, value) : value;
    }

    protected <T> byte[] encryptValue(String key, RedisCryptor cryptor, Convert c, T value) {
        return encryptValue(key, cryptor, null, c, value);
    }

    protected <T> byte[] encryptValue(String key, RedisCryptor cryptor, Type type, Convert c, T value) {
        if (value == null) return null;
        Type t = type == null ? value.getClass() : type;
        if (cryptor == null && type == String.class) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }
        return encryptValue(key, cryptor, t, (c == null ? this.convert : c).convertToBytes(t, value));
    }

    protected byte[] encryptValue(String key, RedisCryptor cryptor, Type type, byte[] bs) {
        if (bs == null) return null;
        if (cryptor == null || (type instanceof Class && (((Class) type).isPrimitive() || Number.class.isAssignableFrom((Class) type)))) {
            return bs;
        }
        String enval = cryptor.encrypt(key, new String(bs, StandardCharsets.UTF_8));
        return enval == null ? null : enval.getBytes(StandardCharsets.UTF_8);
    }
}
