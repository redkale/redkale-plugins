/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.redkalex.cache.redis;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.redkale.annotation.Resource;
import static org.redkale.boot.Application.RESNAME_APP_EXECUTOR;
import static org.redkale.boot.Application.RESNAME_APP_NAME;
import org.redkale.convert.Convert;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceFactory;
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

    @Resource(name = RESNAME_APP_NAME, required = false)
    protected String appName = "";

    @Resource(required = false)
    protected ResourceFactory resourceFactory;

    @Resource(required = false)
    protected JsonConvert defaultConvert;

    @Resource(name = Resource.PARENT_NAME + "_convert", required = false)
    protected JsonConvert convert;

    protected int db;

    protected RedisCryptor cryptor;

    protected AnyValue conf;

    private ExecutorService subExecutor;

    private final ReentrantLock subExecutorLock = new ReentrantLock();

    @Resource(name = RESNAME_APP_EXECUTOR, required = false)
    protected ExecutorService workExecutor;

    @Override
    public void init(AnyValue conf) {
        this.conf = conf;
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

    public boolean acceptsConf(AnyValue config) {
        if (config == null) {
            return false;
        }
        return "redis".equalsIgnoreCase(config.getValue(CACHE_SOURCE_TYPE))
            || getClass().getName().equalsIgnoreCase(config.getValue(CACHE_SOURCE_TYPE))
            || config.getValue(CACHE_SOURCE_NODES, config.getValue("url", "")).startsWith("redis://")
            || config.getValue(CACHE_SOURCE_NODES, config.getValue("url", "")).startsWith("rediss://");
    }

    protected ExecutorService subExecutor() {
        ExecutorService executor = subExecutor;
        if (executor != null) {
            return executor;
        }
        subExecutorLock.lock();
        try {
            if (subExecutor == null) {
                String threadNameFormat = "CacheSource-" + resourceName() + "-SubThread-%s";
                Function<String, ExecutorService> func = Utility.virtualExecutorFunction();
                final AtomicInteger counter = new AtomicInteger();
                subExecutor = func == null ? Executors.newFixedThreadPool(Utility.cpus(), r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    int c = counter.incrementAndGet();
                    t.setName(String.format(threadNameFormat, "Virtual-" + (c < 10 ? ("00" + c) : (c < 100 ? ("0" + c) : c))));
                    return t;
                }) : func.apply(threadNameFormat);
            }
            executor = subExecutor;
        } finally {
            subExecutorLock.unlock();
        }
        return executor;
    }

    protected String getNodes(AnyValue config) {
        return config.getValue(CACHE_SOURCE_NODES, config.getValue("url", ""));
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
        byte[] bs = (c == null ? this.convert : c).convertToBytes(t, value);
        if (bs.length > 1 && t instanceof Class && !CharSequence.class.isAssignableFrom((Class) t)) {
            if (bs[0] == '"' && bs[bs.length - 1] == '"') {
                bs = Arrays.copyOfRange(bs, 1, bs.length - 1);
            }
        }
        return encryptValue(key, cryptor, t, bs);
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

    protected <T extends Number> T decryptScore(Class<T> scoreType, Double score) {
        if (score == null) {
            return null;
        }
        if (scoreType == int.class || scoreType == Integer.class) {
            return (T) (Number) score.intValue();
        } else if (scoreType == long.class || scoreType == Long.class) {
            return (T) (Number) score.longValue();
        } else if (scoreType == float.class || scoreType == Float.class) {
            return (T) (Number) score.floatValue();
        } else if (scoreType == double.class || scoreType == Double.class) {
            return (T) (Number) score;
        } else {
            return JsonConvert.root().convertFrom(scoreType, score.toString());
        }
    }

    protected CompletableFuture<Integer> returnFutureSize(List<CompletableFuture<Void>> futures) {
        return futures == null || futures.isEmpty() ? CompletableFuture.completedFuture(0) : Utility.allOfFutures(futures).thenApply(v -> futures.size());
    }
}
