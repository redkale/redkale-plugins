/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.*;
import io.lettuce.core.cluster.api.sync.*;
import io.lettuce.core.codec.*;
import io.lettuce.core.support.*;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;
import java.util.stream.Collectors;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceListener;
import org.redkale.annotation.ResourceType;
import org.redkale.convert.Convert;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkale.util.Utility.*;

/**
 * 注意: 目前Lettuce连接数过小时会出现连接池频频报连接耗尽的错误，推荐使用Redission实现方式。
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@ResourceType(CacheSource.class)
public class RedisLettuceCacheSource extends AbstractRedisSource {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected Type objValueType = String.class;

    protected List<String> nodeAddrs;

    protected io.lettuce.core.AbstractRedisClient client;

    protected BoundedAsyncPool<StatefulRedisConnection<String, byte[]>> singleBytesConnPool;

    protected BoundedAsyncPool<StatefulRedisConnection<String, String>> singleStringConnPool;

    protected BoundedAsyncPool<StatefulRedisClusterConnection<String, byte[]>> clusterBytesConnPool;

    protected BoundedAsyncPool<StatefulRedisClusterConnection<String, String>> clusterStringConnPool;

    protected RedisCodec<String, byte[]> stringByteArrayCodec;

    protected RedisCodec<String, String> stringStringCodec;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (conf == null) {
            conf = AnyValue.create();
        }
        initClient(conf);
    }

    private void initClient(AnyValue conf) {
        this.stringByteArrayCodec = (RedisCodec) RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);
        this.stringStringCodec = StringCodec.UTF8;

        final RedisConfig config = RedisConfig.create(conf);
        List<RedisURI> uris = new ArrayList();
        for (String addr : config.getAddresses()) {
            RedisURI ruri = RedisURI.create(addr);
            ruri.setDatabase(config.getDb());
            if (config.getUsername() != null || config.getPassword() != null) {
                RedisCredentials authCredentials = RedisCredentials.just(
                    config.getUsername() != null ? config.getUsername() : "",
                    config.getPassword() != null ? config.getPassword() : "");
                ruri.setCredentialsProvider(RedisCredentialsProvider.from(() -> authCredentials));
            }

            uris.add(ruri);
        }
        final RedisURI singleRedisURI = uris.get(0);
        io.lettuce.core.AbstractRedisClient old = this.client;

        RedisClient singleClient = null;
        RedisClusterClient clusterClient = null;
        if (uris.size() < 2) {
            singleClient = io.lettuce.core.RedisClient.create(singleRedisURI);
            this.client = singleClient;
        } else {
            clusterClient = RedisClusterClient.create(uris);
            this.client = clusterClient;
        }
        this.nodeAddrs = config.getAddresses();
        BoundedPoolConfig bpc = BoundedPoolConfig.builder().maxTotal(-1).maxIdle(config.getMaxconns()).minIdle(0).build();
        if (clusterClient == null) {
            RedisClient sClient = singleClient;
            this.singleBytesConnPool = AsyncConnectionPoolSupport.createBoundedObjectPool(() -> sClient.connectAsync(stringByteArrayCodec, singleRedisURI), bpc);
            this.singleStringConnPool = AsyncConnectionPoolSupport.createBoundedObjectPool(() -> sClient.connectAsync(stringStringCodec, singleRedisURI), bpc);
        } else {
            RedisClusterClient sClient = clusterClient;
            this.clusterBytesConnPool = AsyncConnectionPoolSupport.createBoundedObjectPool(() -> sClient.connectAsync(stringByteArrayCodec), bpc);
            this.clusterStringConnPool = AsyncConnectionPoolSupport.createBoundedObjectPool(() -> sClient.connectAsync(stringStringCodec), bpc);
        }
        if (old != null) {
            old.close();
        }
        //if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedisLettuceCacheSource.class.getSimpleName() + ": addrs=" + addresses);
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
        initClient(this.conf);
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
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
    public io.lettuce.core.AbstractRedisClient getRedisClient() {
        return this.client;
    }

    protected <T> List<T> formatCollection(String key, RedisCryptor cryptor, List<byte[]> collection, Convert convert0, final Type componentType) {
        List<T> rs = new ArrayList<>();
        if (collection == null) {
            return rs;
        }
        for (byte[] bs : collection) {
            if (bs == null) {
                continue;
            }
            rs.add((T) decryptValue(key, cryptor, convert0, componentType, bs));
        }
        return rs;
    }

    protected <T> Set<T> formatCollection(String key, RedisCryptor cryptor, Set<byte[]> collection, Convert convert0, final Type componentType) {
        Set<T> rs = new LinkedHashSet<>();
        if (collection == null) {
            return rs;
        }
        for (byte[] bs : collection) {
            if (bs == null) {
                continue;
            }
            rs.add((T) decryptValue(key, cryptor, convert0, componentType, bs));
        }
        return rs;
    }

    protected Collection<Long> formatLongCollection(boolean set, Collection<String> list) {
        if (set) {
            Set<Long> rs = new LinkedHashSet<>();
            for (String str : list) {
                rs.add(Long.parseLong(str));
            }
            return rs;
        } else {
            List<Long> rs = new ArrayList<>();
            for (String str : list) {
                rs.add(Long.parseLong(str));
            }
            return rs;
        }
    }

    protected Collection<String> formatStringCollection(String key, RedisCryptor cryptor, boolean set, Collection<String> list) {
        if (cryptor == null) {
            return list;
        }
        if (set) {
            Set<String> rs = new LinkedHashSet<>();
            for (String str : list) {
                rs.add(cryptor.decrypt(key, str));
            }
            return rs;
        } else {
            List<String> rs = new ArrayList<>();
            for (String str : list) {
                rs.add(cryptor.decrypt(key, str));
            }
            return rs;
        }
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (client != null) {
            client.shutdown();
        }
    }

    protected <T, U> CompletableFuture<U> completableBoolFuture(RedisClusterAsyncCommands<String, byte[]> command, CompletionStage<Boolean> rf) {
        return (CompletableFuture) rf.toCompletableFuture().whenComplete((v, e) -> releaseBytesCommand(command));
    }

    protected <T, U> CompletableFuture<U> completableBoolFuture(String key, RedisCryptor cryptor, RedisClusterAsyncCommands<String, String> command, CompletionStage<Boolean> rf) {
        return (CompletableFuture) rf.toCompletableFuture().whenComplete((v, e) -> releaseStringCommand(command));
    }

    protected <T, U> CompletableFuture<U> completableBytesFuture(RedisClusterAsyncCommands<String, byte[]> command, CompletionStage<T> rf) {
        return (CompletableFuture) rf.toCompletableFuture().whenComplete((v, e) -> releaseBytesCommand(command));
    }

    protected <T, U> CompletableFuture<U> completableStringFuture(String key, RedisCryptor cryptor, RedisClusterAsyncCommands<String, String> command, CompletionStage<T> rf) {
        if (cryptor != null) {
            return (CompletableFuture) rf.toCompletableFuture()
                .thenApply(v -> v == null ? v : cryptor.decrypt(key, v.toString()))
                .whenComplete((v, e) -> releaseStringCommand(command));
        }
        return (CompletableFuture) rf.toCompletableFuture()
            .whenComplete((v, e) -> releaseStringCommand(command));
    }

    protected <T> CompletableFuture<Long> completableLongFuture(RedisClusterAsyncCommands<String, String> command, CompletionStage<T> rf) {
        return (CompletableFuture) rf.toCompletableFuture()
            .whenComplete((v, e) -> releaseStringCommand(command));
    }

    protected CompletableFuture<RedisClusterAsyncCommands<String, byte[]>> connectBytesAsync() {
        if (clusterBytesConnPool == null) {
            return singleBytesConnPool.acquire().thenApply(c -> c.async());
        } else {
            return clusterBytesConnPool.acquire().thenApply(c -> c.async());
        }
    }

    protected CompletableFuture<RedisClusterAsyncCommands<String, String>> connectStringAsync() {
        if (clusterBytesConnPool == null) {
            return singleStringConnPool.acquire().thenApply(c -> c.async());
        } else {
            return clusterStringConnPool.acquire().thenApply(c -> c.async());
        }
    }

    protected RedisClusterCommands<String, byte[]> connectBytes() {
        if (clusterBytesConnPool == null) {
            return singleBytesConnPool.acquire().join().sync();
        } else {
            return clusterBytesConnPool.acquire().join().sync();
        }
    }

    protected RedisClusterCommands<String, String> connectString() {
        if (clusterStringConnPool == null) {
            return singleStringConnPool.acquire().join().sync();
        } else {
            return clusterStringConnPool.acquire().join().sync();
        }
    }

    protected void releaseBytesCommand(RedisClusterCommands<String, byte[]> command) {
        if (command instanceof RedisCommands) {
            singleBytesConnPool.release(((RedisCommands) command).getStatefulConnection()).join();
        } else {
            clusterBytesConnPool.release(((RedisAdvancedClusterCommands) command).getStatefulConnection()).join();
        }
    }

    protected void releaseStringCommand(RedisClusterCommands<String, String> command) {
        if (command instanceof RedisCommands) {
            singleStringConnPool.release(((RedisCommands) command).getStatefulConnection()).join();
        } else {
            clusterStringConnPool.release(((RedisAdvancedClusterCommands) command).getStatefulConnection()).join();
        }
    }

    protected void releaseBytesCommand(RedisClusterAsyncCommands<String, byte[]> command) {
        if (command instanceof RedisAsyncCommands) {
            singleBytesConnPool.release(((RedisAsyncCommands) command).getStatefulConnection());
        } else {
            clusterBytesConnPool.release(((RedisAdvancedClusterAsyncCommands) command).getStatefulConnection());
        }
    }

    protected void releaseStringCommand(RedisClusterAsyncCommands<String, String> command) {
        if (command instanceof RedisAsyncCommands) {
            singleStringConnPool.release(((RedisAsyncCommands) command).getStatefulConnection());
        } else {
            clusterStringConnPool.release(((RedisAdvancedClusterAsyncCommands) command).getStatefulConnection());
        }
    }

    protected <T> byte[][] keyArgs(String key, Type componentType, T... values) {
        byte[][] bss = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bss[i] = encryptValue(key, cryptor, componentType, (Convert) null, values[i]);
        }
        return bss;
    }

    protected <T> byte[][] keyArgs(String... members) {
        byte[][] bss = new byte[members.length][];
        for (int i = 0; i < members.length; i++) {
            bss[i] = members[i].getBytes(StandardCharsets.UTF_8);
        }
        return bss;
    }

    @Override
    public CompletableFuture<Boolean> isOpenAsync() {
        return CompletableFuture.completedFuture(client != null);
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.exists(key).thenApply(v -> v > 0));
        });
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.get(key).thenApply(bs -> decryptValue(key, cryptor, type, bs)));
        });
    }

    //--------------------- getex ------------------------------
    @Override
    public <T> CompletableFuture<T> getexAsync(String key, int expireSeconds, final Type type) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.getex(key, GetExArgs.Builder.ex(expireSeconds)).thenApply(v -> decryptValue(key, cryptor, type, v)));
        });
    }

//    //--------------------- setex ------------------------------
    @Override
    public CompletableFuture<Void> msetAsync(final Serializable... keyVals) {
        if (keyVals.length % 2 != 0) {
            throw new RedkaleException("key value must be paired");
        }
        Map<String, byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < keyVals.length; i += 2) {
            String key = keyVals[i].toString();
            Object val = keyVals[i + 1];
            map.put(key, encryptValue(key, cryptor, convert, val));
        }
        return connectBytesAsync().thenCompose(command -> completableBytesFuture(command, command.mset(map)));
    }

    @Override
    public CompletableFuture<Void> msetAsync(final Map map) {
        if (isEmpty(map)) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, byte[]> rs = new LinkedHashMap<>();
        map.forEach((key, val) -> {
            rs.put(key.toString(), encryptValue(key.toString(), cryptor, convert, val));
        });
        return connectBytesAsync().thenCompose(command -> completableBytesFuture(command, command.mset(rs)));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert0, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.set(key, encryptValue(key, cryptor, type, convert0, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxAsync(String key, Convert convert0, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBoolFuture(command, command.setnx(key, encryptValue(key, cryptor, type, convert0, value)));
        });
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, Convert convert0, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            Convert c = convert0 == null ? this.convert : convert0;
            return completableBytesFuture(command, command.setGet(key, encryptValue(key, cryptor, type, c, value))
                .thenApply(old -> decryptValue(key, cryptor, c, type, old)));
        });
    }

    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setex(key, expireSeconds, encryptValue(key, cryptor, type, convert0, value)).thenApply(r -> null));
        });
    }

    @Override
    public <T> CompletableFuture<Boolean> setnxexAsync(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.set(key, encryptValue(key, cryptor, type, convert0, value), SetArgs.Builder.nx().ex(expireSeconds))
                .thenApply(r -> r != null && ("OK".equals(r) || Integer.parseInt(r) > 0)));
        });
    }

    //--------------------- expire ------------------------------    
    @Override
    public CompletableFuture<Void> expireAsync(String key, int expireSeconds) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.expire(key, Duration.ofSeconds(expireSeconds)).thenApply(r -> null));
        });
    }

//    //--------------------- persist ------------------------------    
    @Override
    public CompletableFuture<Boolean> persistAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.persist(key));
        });
    }

//    //--------------------- rename ------------------------------    
    @Override
    public CompletableFuture<Boolean> renameAsync(String oldKey, String newKey) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.rename(oldKey, newKey).thenApply(r -> r != null && ("OK".equals(r) || Integer.parseInt(r) > 0)));
        });
    }

    @Override
    public CompletableFuture<Boolean> renamenxAsync(String oldKey, String newKey) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.renamenx(oldKey, newKey));
        });
    }

//    //--------------------- del ------------------------------    
    @Override
    public CompletableFuture<Long> delAsync(String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.del(keys));
        });
    }

//    //--------------------- incrby ------------------------------    
    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.incr(key));
        });
    }

    @Override
    public CompletableFuture<Long> incrbyAsync(final String key, long num) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.incrby(key, num));
        });
    }

    @Override
    public CompletableFuture<Double> incrbyFloatAsync(final String key, double num) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.incrbyfloat(key, num));
        });
    }

    //--------------------- decrby ------------------------------    
    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.decr(key));
        });
    }

    @Override
    public CompletableFuture<Long> decrbyAsync(final String key, long num) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.decrby(key, num));
        });
    }

    //--------------------- dbsize ------------------------------  
    @Override
    public CompletableFuture<Long> dbsizeAsync() {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.dbsize());
        });
    }

    @Override
    public CompletableFuture<Void> flushdbAsync() {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.flushdb());
        });
    }

    @Override
    public CompletableFuture<Void> flushallAsync() {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.flushall());
        });
    }

    @Override
    public <T> CompletableFuture<T> rpopAsync(final String key, final Type componentType) {
        return connectBytesAsync().thenCompose(command
            -> completableBytesFuture(command, command.rpop(key).thenApply(bs -> decryptValue(key, cryptor, componentType, bs)))
        );
    }

    @Override
    public <T> CompletableFuture<T> rpoplpushAsync(final String key, final String key2, final Type componentType) {
        return connectBytesAsync().thenCompose(command
            -> completableBytesFuture(command, command.rpoplpush(key, key2).thenApply(bs -> decryptValue(key, cryptor, componentType, bs)))
        );
    }

    @Override
    public CompletableFuture<Long> hdelAsync(String key, String... fields) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hdel(key, fields));
        });
    }

    @Override
    public CompletableFuture<List<String>> hkeysAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hkeys(key));
        });
    }

    @Override
    public CompletableFuture<Long> hlenAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hlen(key));
        });
    }

    @Override
    public CompletableFuture<Long> hincrAsync(String key, String field) {
        return hincrbyAsync(key, field, 1);
    }

    @Override
    public CompletableFuture<Long> hincrbyAsync(String key, String field, long num) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.hincrby(key, field, num));
        });
    }

    @Override
    public CompletableFuture<Double> hincrbyFloatAsync(String key, String field, double num) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.hincrbyfloat(key, field, num));
        });
    }

    @Override
    public CompletableFuture<Long> hdecrAsync(String key, String field) {
        return hincrbyAsync(key, field, -1);
    }

    @Override
    public CompletableFuture<Long> hdecrbyAsync(String key, String field, long num) {
        return hincrbyAsync(key, field, -num);
    }

    @Override
    public CompletableFuture<Boolean> hexistsAsync(String key, String field) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hexists(key, field));
        });
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(String key, String field, Convert convert0, Type type, T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hset(key, field, encryptValue(key, cryptor, type, convert0, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Boolean> hsetnxAsync(String key, String field, Convert convert0, Type type, T value) {
        if (value == null) {
            return CompletableFuture.completedFuture(null);
        }
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hsetnx(key, field, (convert0 == null ? convert : convert0).convertToBytes(type, value)));
        });
    }

    @Override
    public CompletableFuture<Boolean> hsetnxStringAsync(String key, String field, String value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.hsetnx(key, field, value));
        });
    }

    @Override
    public CompletableFuture<Boolean> hsetnxLongAsync(String key, String field, long value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.hsetnx(key, field, String.valueOf(value)));
        });
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(String key, Serializable... values) {
        Map<String, byte[]> vals = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            byte[] bs;
            if (cryptor != null && values[i + 1] != null) {
                bs = values[i + 1] instanceof String ? cryptor.encrypt(key, values[i + 1].toString()).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, values[i + 1].getClass(), this.convert, values[i + 1]);
            } else {
                bs = values[i + 1] instanceof String ? values[i + 1].toString().getBytes(StandardCharsets.UTF_8) : this.convert.convertToBytes(values[i + 1]);
            }
            vals.put(String.valueOf(values[i]), bs);
        }
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hmset(key, vals));
        });
    }

    @Override
    public CompletableFuture<Void> hmsetAsync(String key, Map map) {
        Map<String, byte[]> vals = new LinkedHashMap<>();
        map.forEach((k, v) -> {
            byte[] bs;
            if (cryptor != null && v != null) {
                bs = v instanceof String ? cryptor.encrypt(key, v.toString()).getBytes(StandardCharsets.UTF_8) : encryptValue(key, cryptor, v.getClass(), this.convert, v);
            } else {
                bs = v instanceof String ? v.toString().getBytes(StandardCharsets.UTF_8) : this.convert.convertToBytes(v);
            }
            vals.put(k.toString(), bs);
        });
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hmset(key, vals));
        });
    }

    @Override
    public <T> CompletableFuture<List<T>> hmgetAsync(String key, Type type, String... fields) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hmget(key, fields).thenApply((List<KeyValue<String, byte[]>> rs) -> {
                List<Serializable> list = new ArrayList<>(fields.length);
                for (String field : fields) {
                    byte[] bs = null;
                    for (KeyValue<String, byte[]> kv : rs) {
                        if (kv.getKey().equals(field)) {
                            bs = kv.hasValue() ? kv.getValue() : null;
                            break;
                        }
                    }
                    if (bs == null) {
                        list.add(null);
                    } else {
                        list.add(decryptValue(key, cryptor, type, bs));
                    }
                }
                return list;
            }));
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hscanAsync(String key, Type type, AtomicLong cursor, int limit, String pattern) {
        ScanArgs args = new ScanArgs();
        if (isNotEmpty(pattern)) {
            args = args.match(pattern);
        }
        if (limit > 0) {
            args = args.limit(limit);
        }
        final ScanArgs scanArgs = args;
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hscan(key, new ScanCursor(cursor.toString(), false), scanArgs)
                .thenApply((MapScanCursor<String, byte[]> rs) -> {
                    final Map<String, T> map = new LinkedHashMap<>();
                    rs.getMap().forEach((k, v) -> map.put(k, v == null ? null : decryptValue(key, cryptor, type, v)));
                    cursor.set(Long.parseLong(rs.getCursor()));
                    return map;
                }));
        });
    }

    @Override
    public <T> CompletableFuture<Set< T>> sscanAsync(String key, Type componentType, AtomicLong cursor, int limit, String pattern) {
        ScanArgs args = new ScanArgs();
        if (isNotEmpty(pattern)) {
            args = args.match(pattern);
        }
        if (limit > 0) {
            args = args.limit(limit);
        }
        final ScanArgs scanArgs = args;
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sscan(key, new ScanCursor(cursor.toString(), false), scanArgs)
                .thenApply((ValueScanCursor<byte[]> rs) -> {
                    final Set< T> set = new LinkedHashSet<>();
                    rs.getValues().forEach(v -> set.add(v == null ? null : decryptValue(key, cryptor, componentType, v)));
                    cursor.set(Long.parseLong(rs.getCursor()));
                    return set;
                }));
        });
    }

    @Override
    public CompletableFuture<List<String>> scanAsync(AtomicLong cursor, int limit, String pattern) {
        ScanArgs args = new ScanArgs();
        if (isNotEmpty(pattern)) {
            args = args.match(pattern);
        }
        if (limit > 0) {
            args = args.limit(limit);
        }
        final ScanArgs scanArgs = args;
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.scan(new ScanCursor(cursor.toString(), false), scanArgs)
                .thenApply((KeyScanCursor<String> rs) -> {
                    final List<String> list = rs.getKeys();
                    cursor.set(Long.parseLong(rs.getCursor()));
                    return list;
                }));
        });
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(String key, String field, Type type) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hget(key, field)
                .thenApply(bs -> bs == null ? null : decryptValue(key, cryptor, type, bs)));
        });
    }

    @Override
    public CompletableFuture<Long> hstrlenAsync(String key, String field) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hstrlen(key, field));
        });
    }

    @Override
    public <T> CompletableFuture<List<T>> mgetAsync(Type componentType, String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.mget(keys)
                .thenApply((List<KeyValue<String, byte[]>> rs) -> {
                    List<T> list = new ArrayList<>(rs.size());
                    rs.forEach(kv -> {
                        if (kv.hasValue()) {
                            list.add(decryptValue(kv.getKey(), cryptor, componentType, kv.getValue()));
                        } else {
                            list.add(null);
                        }
                    });
                    return list;
                }));
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hgetallAsync(String key, final Type type) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hgetall(key)
                .thenApply((Map<String, byte[]> rs) -> {
                    Map<String, T> map = new LinkedHashMap(rs.size());
                    rs.forEach((k, v) -> {
                        map.put(k, decryptValue(k, cryptor, type, v));
                    });
                    return map;
                }));
        });
    }

    @Override
    public <T> CompletableFuture<List<T>> hvalsAsync(String key, final Type type) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hvals(key)
                .thenApply((List<byte[]> rs) -> {
                    List< T> list = new ArrayList<>(rs.size());
                    rs.forEach(v -> {
                        list.add(decryptValue(key, cryptor, type, v));
                    });
                    return list;
                }));
        });
    }

    @Override
    public <T> CompletableFuture<Set<T>> smembersAsync(String key, final Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.smembers(key).thenApply(set -> formatCollection(key, cryptor, set, convert, componentType)));
        });
    }

    @Override
    public <T> CompletableFuture<List<T>> lrangeAsync(String key, final Type componentType, int start, int stop) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.lrange(key, start, stop).thenApply(list -> formatCollection(key, cryptor, list, convert, componentType)));
        });
    }

    @Override
    public <T> CompletableFuture<T> lindexAsync(String key, Type componentType, int index) {
        return connectBytesAsync().thenCompose(command
            -> completableBytesFuture(command, command.lindex(key, index).thenApply(bs -> decryptValue(key, cryptor, componentType, bs)))
        );
    }

    @Override
    public <T> CompletableFuture<Long> linsertBeforeAsync(String key, Type componentType, T pivot, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.linsert(key, true,
                encryptValue(key, cryptor, componentType, (Convert) null, pivot),
                encryptValue(key, cryptor, componentType, (Convert) null, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Long> linsertAfterAsync(String key, Type componentType, T pivot, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.linsert(key, false,
                encryptValue(key, cryptor, componentType, (Convert) null, pivot),
                encryptValue(key, cryptor, componentType, (Convert) null, value)));
        });
    }

    @Override
    public CompletableFuture<Void> ltrimAsync(final String key, int start, int stop) {
        return connectBytesAsync().thenCompose(command -> completableBytesFuture(command, command.ltrim(key, start, stop)));
    }

    @Override
    public <T> CompletableFuture<T> lpopAsync(final String key, final Type componentType) {
        return connectBytesAsync().thenCompose(command
            -> completableBytesFuture(command, command.lpop(key).thenApply(bs -> decryptValue(key, cryptor, componentType, bs)))
        );
    }

    @Override
    public <T> CompletableFuture<Void> lpushAsync(final String key, final Type componentType, T... values) {
        return connectBytesAsync().thenCompose(command
            -> completableBytesFuture(command, command.lpush(key, keyArgs(key, componentType, values)))
        );
    }

    @Override
    public <T> CompletableFuture<Void> lpushxAsync(final String key, final Type componentType, T... values) {
        return connectBytesAsync().thenCompose(command
            -> completableBytesFuture(command, command.lpushx(key, keyArgs(key, componentType, values)))
        );
    }

    @Override
    public <T> CompletableFuture<Void> rpushxAsync(final String key, final Type componentType, T... values) {
        return connectBytesAsync().thenCompose(command
            -> completableBytesFuture(command, command.rpushx(key, keyArgs(key, componentType, values)))
        );
    }

    @Override
    public <T> CompletableFuture<Map<String, List<T>>> lrangesAsync(Type componentType, String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            final Map<String, List<T>> map = new LinkedHashMap<>();
            final ReentrantLock mapLock = new ReentrantLock();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = command.lrange(key, 0, -1).thenAccept(rs -> {
                    if (rs != null) {
                        mapLock.lock();
                        try {
                            map.put(key, formatCollection(key, cryptor, rs, convert, componentType));
                        } finally {
                            mapLock.unlock();
                        }
                    }
                }).toCompletableFuture();
            }
            return completableBytesFuture(command, CompletableFuture.allOf(futures).thenApply(v -> map));
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, Set<T>>> smembersAsync(Type componentType, String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            final Map<String, Set<T>> map = new LinkedHashMap<>();
            final ReentrantLock mapLock = new ReentrantLock();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = command.smembers(key).thenAccept(rs -> {
                    if (rs != null) {
                        mapLock.lock();
                        try {
                            map.put(key, formatCollection(key, cryptor, rs, convert, componentType));
                        } finally {
                            mapLock.unlock();
                        }
                    }
                }).toCompletableFuture();
            }
            return completableBytesFuture(command, CompletableFuture.allOf(futures).thenApply(v -> map));
        });
    }

    @Override
    public CompletableFuture<Long> llenAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.type(key).thenCompose(type -> {
                return command.llen(key);
            }));
        });
    }

    @Override
    public CompletableFuture<Long> scardAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.type(key).thenCompose(type -> {
                return command.scard(key);
            }));
        });
    }

    @Override
    public <T> CompletableFuture<Boolean> smoveAsync(String key, String key2, Type componentType, T member) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.smove(key, key2, encryptValue(key, cryptor, componentType, (Convert) null, member)));
        });
    }

    @Override
    public <T> CompletableFuture<List<T>> srandmemberAsync(String key, Type componentType, int count) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.srandmember(key, count)
                .thenApply(v -> formatCollection(key, cryptor, v, convert, componentType)));
        });
    }

    @Override
    public <T> CompletableFuture<Set<T>> sdiffAsync(final String key, final Type componentType, final String... key2s) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sdiff(Utility.append(key, key2s))
                .thenApply(v -> formatCollection(key, cryptor, v, convert, componentType)));
        });
    }

    @Override
    public CompletableFuture<Long> sdiffstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sdiffstore(key, Utility.append(srcKey, srcKey2s)));
        });
    }

    @Override
    public <T> CompletableFuture<Set<T>> sinterAsync(final String key, final Type componentType, final String... key2s) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sinter(Utility.append(key, key2s))
                .thenApply(v -> formatCollection(key, cryptor, v, convert, componentType)));
        });
    }

    @Override
    public CompletableFuture<Long> sinterstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sinterstore(key, Utility.append(srcKey, srcKey2s)));
        });
    }

    @Override
    public <T> CompletableFuture<Set<T>> sunionAsync(final String key, final Type componentType, final String... key2s) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sunion(Utility.append(key, key2s))
                .thenApply(v -> formatCollection(key, cryptor, v, convert, componentType)));
        });
    }

    @Override
    public CompletableFuture<Long> sunionstoreAsync(final String key, final String srcKey, final String... srcKey2s) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sunionstore(key, Utility.append(srcKey, srcKey2s)));
        });
    }

    @Override
    public <T> CompletableFuture<T> spopAsync(String key, Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.spop(key)
                .thenApply(bs -> bs == null ? null : decryptValue(key, cryptor, componentType, bs)));
        });
    }

    @Override
    public <T> CompletableFuture<Set<T>> spopAsync(String key, int count, Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.spop(key, count)
                .thenApply(v -> formatCollection(key, cryptor, v, convert, componentType)));
        });
    }

    @Override
    public <T> CompletableFuture<Void> rpushAsync(String key, Type componentType, T... values) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.rpush(key, keyArgs(key, componentType, values)));
        });
    }

    @Override
    public <T> CompletableFuture<Long> lremAsync(String key, Type componentType, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.lrem(key, 0, encryptValue(key, cryptor, componentType, convert, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Boolean> sismemberAsync(String key, Type componentType, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sismember(key, encryptValue(key, cryptor, componentType, convert, value)));
        });
    }

    @Override
    public <T> CompletableFuture<List<Boolean>> smismembersAsync(final String key, final String... members) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, cryptor, command, command.smismember(key, members));
        });
    }

    @Override
    public <T> CompletableFuture<Void> saddAsync(String key, Type componentType, T... values) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sadd(key, keyArgs(key, componentType, values)));
        });
    }

    @Override
    public <T> CompletableFuture<Long> sremAsync(String key, Type componentType, T... values) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.srem(key, keyArgs(key, componentType, values)));
        });
    }

    @Override
    public CompletableFuture<List<String>> keysAsync(String pattern) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取key值，无需传cryptor进行解密
            return completableStringFuture(null, (RedisCryptor) null, command, command.keys(isEmpty(pattern) ? "*" : pattern));
        });
    }

    //--------------------- sorted set ------------------------------ 
    @Override
    public CompletableFuture<Void> zaddAsync(String key, CacheScoredValue... values) {
        ScoredValue<byte[]>[] vals = new ScoredValue[values.length];
        for (int i = 0; i < values.length; i++) {
            vals[i] = ScoredValue.just(values[i].getScore().doubleValue(), values[i].getValue().getBytes(StandardCharsets.UTF_8));
        }
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.zadd(key, (ScoredValue[]) vals).thenApply(v -> null));
        });
    }

    @Override
    public <T extends Number> CompletableFuture<T> zincrbyAsync(String key, CacheScoredValue value) {
        return connectBytesAsync().thenCompose(command -> {
            //此处获取score值，无需传cryptor进行解密 
            return completableBytesFuture(command, command.zincrby(key, value.getScore().doubleValue(), value.getValue().getBytes(StandardCharsets.UTF_8))
                .thenApply(v -> decryptScore((Class) value.getScore().getClass(), v)));
        });
    }

    @Override
    public CompletableFuture<Long> zremAsync(String key, String... members) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.zrem(key, keyArgs(members)));
        });
    }

    @Override
    public <T extends Number> CompletableFuture<List<T>> zmscoreAsync(String key, Class<T> scoreType, String... members) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取score值，无需传cryptor进行解密
            return completableStringFuture(null, (RedisCryptor) null, command, command.zmscore(key, members)
                .thenApply(list -> list.stream().map(v -> decryptScore(scoreType, v)).collect(Collectors.toList())));
        });
    }

    @Override
    public <T extends Number> CompletableFuture<T> zscoreAsync(String key, Class<T> scoreType, String member) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取score值，无需传cryptor进行解密 
            return completableStringFuture(null, (RedisCryptor) null, command, command.zscore(key, member)
                .thenApply(v -> decryptScore(scoreType, v)));
        });
    }

    @Override
    public CompletableFuture<Long> zcardAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.zcard(key));
        });
    }

    @Override
    public CompletableFuture<Long> zrankAsync(String key, String member) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.zrank(key, member.getBytes(StandardCharsets.UTF_8)));
        });
    }

    @Override
    public CompletableFuture<Long> zrevrankAsync(String key, String member) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.zrevrank(key, member.getBytes(StandardCharsets.UTF_8)));
        });
    }

    @Override
    public CompletableFuture<List<String>> zrangeAsync(String key, int start, int stop) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取score值，无需传cryptor进行解密
            return completableStringFuture(null, (RedisCryptor) null, command, command.zrange(key, start, stop));
        });
    }

    @Override
    public CompletableFuture<List<CacheScoredValue.NumberScoredValue>> zscanAsync(String key, Type scoreType, AtomicLong cursor, int limit, String pattern) {
        ScanArgs args = new ScanArgs();
        if (isNotEmpty(pattern)) {
            args = args.match(pattern);
        }
        if (limit > 0) {
            args = args.limit(limit);
        }
        final ScanArgs scanArgs = args;
        return connectStringAsync().thenCompose(command -> {
            //此处获取score值，无需传cryptor进行解密 
            return completableStringFuture(null, (RedisCryptor) null, command, command.zscan(key, scanArgs)
                .thenApply(sv -> {
                    return sv.getValues().stream()
                        .map(v -> {
                            Number num = v.getScore();
                            if (scoreType == int.class || scoreType == Integer.class) {
                                num = ((Number) v.getScore()).intValue();
                            } else if (scoreType == long.class || scoreType == Long.class) {
                                num = ((Number) v.getScore()).longValue();
                            } else if (scoreType == float.class || scoreType == Float.class) {
                                num = ((Number) v.getScore()).floatValue();
                            } else if (scoreType == double.class || scoreType == Double.class) {
                                num = ((Number) v.getScore()).doubleValue();
                            } else {
                                num = (Number) JsonConvert.root().convertFrom(scoreType, String.valueOf(v.getScore()));
                            }
                            return new CacheScoredValue.NumberScoredValue(num, v.getValue());
                        })
                        .collect(Collectors.toList());
                }));
        });
    }

    //-------------------------- 过期方法 ----------------------------------
    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Long[]> getLongArrayAsync(String... keys) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(keys[0], null, command, command.mget(keys).thenApply((List<KeyValue<String, String>> rs) -> {
                Long[] array = new Long[keys.length];
                for (int i = 0; i < array.length; i++) {
                    Long bs = null;
                    for (KeyValue<String, String> kv : rs) {
                        if (kv.getKey().equals(keys[i])) {
                            bs = kv.hasValue() ? Long.parseLong(kv.getValue()) : null;
                            break;
                        }
                    }
                    array[i] = bs;
                }
                return array;
            }));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<Long>> getLongCollectionAsync(String key) {
        return getLongCollectionAsync(connectStringAsync(), key);
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(boolean set, String... keys) {
        return connectStringAsync().thenCompose(command -> {
            final Map<String, Collection<Long>> map = new LinkedHashMap<>();
            final ReentrantLock mapLock = new ReentrantLock();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (set) {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.smembers(key).thenAccept(rs -> {
                        if (rs != null) {
                            mapLock.lock();
                            try {
                                map.put(key, formatLongCollection(set, rs));
                            } finally {
                                mapLock.unlock();
                            }
                        }
                    }).toCompletableFuture();
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.lrange(key, 0, -1).thenAccept(rs -> {
                        if (rs != null) {
                            mapLock.lock();
                            try {
                                map.put(key, formatLongCollection(set, rs));
                            } finally {
                                mapLock.unlock();
                            }
                        }
                    }).toCompletableFuture();
                }
            }
            return CompletableFuture.allOf(futures).thenApply(v -> map);
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<Long>> getexLongCollectionAsync(String key, int expireSeconds) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.expire(key, expireSeconds).thenCompose(v -> getLongCollectionAsync(command, key)));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(boolean set, String... keys) {
        return connectStringAsync().thenCompose(command -> {
            final Map<String, Collection<String>> map = new LinkedHashMap<>();
            final ReentrantLock mapLock = new ReentrantLock();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (set) {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.smembers(key).thenAccept(rs -> {
                        if (rs != null) {
                            mapLock.lock();
                            try {
                                map.put(key, formatStringCollection(key, cryptor, set, rs));
                            } finally {
                                mapLock.unlock();
                            }
                        }
                    }).toCompletableFuture();
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.lrange(key, 0, -1).thenAccept(rs -> {
                        if (rs != null) {
                            mapLock.lock();
                            try {
                                map.put(key, formatStringCollection(key, cryptor, set, rs));
                            } finally {
                                mapLock.unlock();
                            }
                        }
                    }).toCompletableFuture();
                }
            }
            return CompletableFuture.allOf(futures).thenApply(v -> map);
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<String>> getexStringCollectionAsync(String key, int expireSeconds) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, null, command, command.expire(key, expireSeconds).thenCompose(v -> getStringCollectionAsync(command, key)));
        });
    }

    @Deprecated(since = "2.8.0")
    protected CompletableFuture<Collection<Long>> getLongCollectionAsync(final CompletableFuture<RedisClusterAsyncCommands<String, String>> commandFuture, String key) {
        return commandFuture.thenCompose(command -> getLongCollectionAsync(command, key));
    }

    @Deprecated(since = "2.8.0")
    protected CompletableFuture<Collection<Long>> getLongCollectionAsync(final RedisClusterAsyncCommands<String, String> command, String key) {
        //此处获取的long值，无需传cryptor进行解密
        return completableStringFuture(key, null, command, command.type(key).thenApply(type -> {
            if (type.contains("list")) {
                return command.lrange(key, 0, -1).thenApply(rs -> formatLongCollection(false, rs));
            } else { //set
                return command.smembers(key).thenApply(rs -> formatLongCollection(true, rs));
            }
        }));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<String[]> getStringArrayAsync(String... keys) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(null, null, command, command.mget(keys).thenApply((List<KeyValue<String, String>> rs) -> {
                String[] array = new String[keys.length];
                for (int i = 0; i < array.length; i++) {
                    String bs = null;
                    for (KeyValue<String, String> kv : rs) {
                        if (kv.getKey().equals(keys[i])) {
                            bs = kv.hasValue() ? (cryptor != null ? cryptor.decrypt(kv.getKey(), kv.getValue()) : kv.getValue()) : null;
                            break;
                        }
                    }
                    array[i] = bs;
                }
                return array;
            }));
        });
    }

    @Deprecated(since = "2.8.0")
    protected <T> CompletableFuture<Collection<T>> getCollectionAsync(
        final RedisClusterAsyncCommands<String, byte[]> command, String key, final Type componentType) {
        return completableBytesFuture(command, command.type(key).thenCompose(type -> {
            if (type.contains("list")) {
                return command.lrange(key, 0, -1).thenApply(list -> formatCollection(key, cryptor, list, convert, componentType));
            } else { //set
                return command.smembers(key).thenApply(set -> formatCollection(key, cryptor, set, convert, componentType));
            }
        }));
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Collection<T>> getexCollectionAsync(String key, int expireSeconds, final Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.expire(key, expireSeconds).thenCompose(v -> getCollectionAsync(command, key, componentType)));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return getCollectionAsync(command, key, componentType);
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(boolean set, Type componentType, String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            final Map<String, Collection<T>> map = new LinkedHashMap<>();
            final ReentrantLock mapLock = new ReentrantLock();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (set) {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.smembers(key).thenAccept(rs -> {
                        if (rs != null) {
                            mapLock.lock();
                            try {
                                map.put(key, formatCollection(key, cryptor, rs, convert, componentType));
                            } finally {
                                mapLock.unlock();
                            }
                        }
                    }).toCompletableFuture();
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.lrange(key, 0, -1).thenAccept(rs -> {
                        if (rs != null) {
                            mapLock.lock();
                            try {
                                map.put(key, formatCollection(key, cryptor, rs, convert, componentType));
                            } finally {
                                mapLock.unlock();
                            }
                        }
                    }).toCompletableFuture();
                }
            }
            return CompletableFuture.allOf(futures).thenApply(v -> map);
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Integer> getCollectionSizeAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.type(key).thenCompose(type -> {
                if (type.contains("list")) {
                    return command.llen(key).thenApply(v -> v.intValue());
                } else { //set 
                    return command.scard(key).thenApply(v -> v.intValue());
                }
            }));
        });
    }

    @Override
    @Deprecated(since = "2.8.0")
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return getStringCollectionAsync(connectStringAsync(), key);
    }

    @Deprecated(since = "2.8.0")
    protected <T> Collection<T> getCollection(final RedisClusterCommands<String, byte[]> command, String key, final Type componentType) {
        final String type = command.type(key);
        if (type.contains("list")) {
            return formatCollection(key, cryptor, command.lrange(key, 0, -1), convert, componentType);
        } else { //set
            return formatCollection(key, cryptor, command.smembers(key), convert, componentType);
        }
    }

    @Deprecated(since = "2.8.0")
    protected CompletableFuture<Collection<String>> getStringCollectionAsync(RedisClusterAsyncCommands<String, String> command, String key) {
        return completableStringFuture(key, null, command, command.type(key).thenApply(type -> {
            if (type.contains("list")) {
                return command.lrange(key, 0, -1).thenApply(list -> formatStringCollection(key, cryptor, false, list));
            } else { //set
                return command.smembers(key).thenApply(list -> formatStringCollection(key, cryptor, true, list));
            }
        }));
    }

    @Deprecated(since = "2.8.0")
    protected CompletableFuture<Collection<String>> getStringCollectionAsync(CompletableFuture<RedisClusterAsyncCommands<String, String>> commandFuture, String key) {
        return commandFuture.thenCompose(command -> getStringCollectionAsync(command, key));
    }

    @Deprecated(since = "2.8.0")
    private Collection<String> decryptStringCollection(String key, final boolean set, Collection<String> collection) {
        if (isEmpty(collection) || cryptor == null) {
            return collection;
        }
        if (set) {
            Set<String> newset = new LinkedHashSet<>();
            for (String value : collection) {
                newset.add(cryptor.decrypt(key, value));
            }
            return newset;
        } else {
            List<String> newlist = new ArrayList<>();
            for (String value : collection) {
                newlist.add(cryptor.decrypt(key, value));
            }
            return newlist;
        }
    }

}
