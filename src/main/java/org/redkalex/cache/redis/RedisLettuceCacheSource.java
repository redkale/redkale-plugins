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
import io.lettuce.core.codec.*;
import io.lettuce.core.support.*;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceListener;
import org.redkale.annotation.ResourceType;
import org.redkale.convert.Convert;
import org.redkale.service.Local;
import org.redkale.source.CacheSource;
import org.redkale.util.*;

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

    protected io.lettuce.core.RedisClient client;

    protected BoundedAsyncPool<StatefulRedisConnection<String, byte[]>> bytesConnPool;

    protected BoundedAsyncPool<StatefulRedisConnection<String, String>> stringConnPool;

    protected RedisCodec<String, byte[]> stringByteArrayCodec;

    protected RedisCodec<String, String> stringStringCodec;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (conf == null) conf = AnyValue.create();
        initClient(conf);
    }

    private void initClient(AnyValue conf) {
        this.stringByteArrayCodec = (RedisCodec) RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE);
        this.stringStringCodec = StringCodec.UTF8;

        final List<String> addresses = new ArrayList<>();
        AnyValue[] nodes = getNodes(conf);
        List<RedisURI> uris = new ArrayList(nodes.length);
        int urlmaxconns = Utility.cpus();
        String gdb = conf.getValue(CACHE_SOURCE_DB, "").trim();
        String gusername = conf.getValue(CACHE_SOURCE_USER, "").trim();
        String gpassword = conf.getValue(CACHE_SOURCE_PASSWORD, "").trim();
        for (AnyValue node : nodes) {
            String addr = node.getValue(CACHE_SOURCE_URL, node.getValue("addr"));  //兼容addr
            addresses.add(addr);
            String dbstr = node.getValue(CACHE_SOURCE_DB, "").trim();
            String username = node.getValue(CACHE_SOURCE_USER, "").trim();
            String password = node.getValue(CACHE_SOURCE_PASSWORD, "").trim();
            URI uri = URI.create(addr);
            if (uri.getQuery() != null && !uri.getQuery().isEmpty()) {
                String[] qrys = uri.getQuery().split("&|=");
                for (int i = 0; i < qrys.length; i += 2) {
                    if (CACHE_SOURCE_MAXCONNS.equals(qrys[i])) {
                        urlmaxconns = i == qrys.length - 1 ? Utility.cpus() : Integer.parseInt(qrys[i + 1]);
                    }
                }
            }
            RedisURI ruri = RedisURI.create(addr);
            if (!dbstr.isEmpty()) {
                db = Integer.parseInt(dbstr);
                ruri.setDatabase(db);
            } else if (!gdb.isEmpty()) {
                ruri.setDatabase(Integer.parseInt(gdb));
            }
//            if (!username.isEmpty()) {
//                ruri.setUsername(username);
//            } else if (!gusername.isEmpty()) {
//                ruri.setUsername(gusername);
//            }
//            if (!password.isEmpty()) {
//                ruri.setPassword(password.toCharArray());
//            } else if (!gpassword.isEmpty()) {
//                ruri.setPassword(gpassword.toCharArray());
//            }
            if (!username.isEmpty() || !gusername.isEmpty()) {
                RedisCredentials authCredentials = RedisCredentials.just(!username.isEmpty() ? username : gusername, !password.isEmpty() ? password : (!gpassword.isEmpty() ? gpassword : ""));
                ruri.setCredentialsProvider(RedisCredentialsProvider.from(() -> authCredentials));
            }

            uris.add(ruri);
        }
        final int maxconns = conf.getIntValue(CACHE_SOURCE_MAXCONNS, urlmaxconns);
        final RedisURI redisURI = uris.get(0);
        io.lettuce.core.RedisClient old = this.client;
        this.client = io.lettuce.core.RedisClient.create(redisURI);
        this.nodeAddrs = addresses;
        BoundedPoolConfig bpc = BoundedPoolConfig.builder().maxTotal(maxconns).maxIdle(maxconns).minIdle(0).build();
        this.bytesConnPool = AsyncConnectionPoolSupport.createBoundedObjectPool(() -> client.connectAsync(stringByteArrayCodec, redisURI), bpc);
        this.stringConnPool = AsyncConnectionPoolSupport.createBoundedObjectPool(() -> client.connectAsync(stringStringCodec, redisURI), bpc);
        if (old != null) old.close();
        //if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, RedisLettuceCacheSource.class.getSimpleName() + ": addrs=" + addresses);
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
            String val = node.getValue(CACHE_SOURCE_URL, node.getValue("addr"));  //兼容addr
            if (val != null && val.startsWith("redis://")) return true;
            if (val != null && val.startsWith("rediss://")) return true;
            if (val != null && val.startsWith("redis-socket://")) return true;
            if (val != null && val.startsWith("redis-sentinel://")) return true;
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

    @Local
    public io.lettuce.core.RedisClient getRedisClient() {
        return this.client;
    }

    protected <T> List<T> formatCollection(String key, RedisCryptor cryptor, List<byte[]> collection, Convert convert0, final Type componentType) {
        List<T> rs = new ArrayList<>();
        if (collection == null) return rs;
        for (byte[] bs : collection) {
            if (bs == null) continue;
            rs.add((T) decryptValue(key, cryptor, convert0, componentType, bs));
        }
        return rs;
    }

    protected <T> Set<T> formatCollection(String key, RedisCryptor cryptor, Set<byte[]> collection, Convert convert0, final Type componentType) {
        Set<T> rs = new LinkedHashSet<>();
        if (collection == null) return rs;
        for (byte[] bs : collection) {
            if (bs == null) continue;
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
        if (cryptor == null) return list;
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
        if (client != null) client.shutdown();
    }

    protected CompletableFuture<RedisAsyncCommands<String, byte[]>> connectBytesAsync() {
        //return client.connect(stringByteArrayCodec).async();
        return bytesConnPool.acquire().thenApply(c -> c.async());
    }

    protected CompletableFuture<RedisAsyncCommands<String, String>> connectStringAsync() {
        //return client.connect(stringStringCodec).async();
        return stringConnPool.acquire().thenApply(c -> c.async());
    }

    protected <T, U> CompletableFuture<U> completableBytesFuture(RedisAsyncCommands<String, byte[]> command, CompletionStage<T> rf) {
        return (CompletableFuture) rf.toCompletableFuture().whenComplete((v, e) -> bytesConnPool.release(command.getStatefulConnection()));
    }

    protected <T, U> CompletableFuture<U> completableStringFuture(String key, RedisCryptor cryptor, RedisAsyncCommands<String, String> command, CompletionStage<T> rf) {
        if (cryptor != null) {
            return (CompletableFuture) rf.toCompletableFuture().thenApply(v -> v == null ? v : cryptor.decrypt(key, v.toString()))
                .whenComplete((v, e) -> stringConnPool.release(command.getStatefulConnection()));
        }
        return (CompletableFuture) rf.toCompletableFuture().whenComplete((v, e) -> stringConnPool.release(command.getStatefulConnection()));
    }

    protected <T> CompletableFuture<Long> completableLongFuture(RedisAsyncCommands<String, String> command, CompletionStage<T> rf) {
        return (CompletableFuture) rf.toCompletableFuture().whenComplete((v, e) -> stringConnPool.release(command.getStatefulConnection()));
    }

    protected RedisCommands<String, byte[]> connectBytes() {
        //return client.connect(stringByteArrayCodec).sync();
        return bytesConnPool.acquire().join().sync();
    }

    protected RedisCommands<String, String> connectString() {
        //return client.connect(stringStringCodec).sync();
        return stringConnPool.acquire().join().sync();
    }

    protected void releaseBytesCommand(RedisCommands<String, byte[]> command) {
        bytesConnPool.release(command.getStatefulConnection()).join();
    }

    protected void releaseStringCommand(RedisCommands<String, String> command) {
        stringConnPool.release(command.getStatefulConnection()).join();
    }

    //--------------------- exists ------------------------------
    @Override
    public CompletableFuture<Boolean> existsAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.exists(key).thenApply(v -> v > 0));
        });
    }

    @Override
    public boolean exists(String key) {
        RedisCommands<String, byte[]> command = connectBytes();
        boolean rs = command.exists(key) > 0;
        releaseBytesCommand(command);
        return rs;
    }

    //--------------------- get ------------------------------
    @Override
    public <T> CompletableFuture<T> getAsync(String key, Type type) {
        return connectBytesAsync().thenCompose(command -> {
            CompletableFuture<byte[]> rf = completableBytesFuture(command, command.get(key));
            return rf.thenApply(bs -> decryptValue(key, cryptor, type, bs));
        });
    }

    @Override
    public CompletableFuture<String> getStringAsync(String key) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, cryptor, command, command.get(key));
        });
    }

    @Override
    public CompletableFuture<String> getSetStringAsync(String key, String value) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, cryptor, command, command.setGet(key, encryptValue(key, cryptor, value)));
        });
    }

    @Override
    public CompletableFuture<Long> getLongAsync(String key, long defValue) {
        return connectStringAsync().thenCompose(command -> {
            return completableLongFuture(command, command.get(key).thenApply(v -> v == null ? defValue : Long.parseLong(v)));
        });
    }

    @Override
    public CompletableFuture<Long> getSetLongAsync(String key, long value, long defValue) {
        return connectStringAsync().thenCompose(command -> {
            return completableLongFuture(command, command.setGet(key, String.valueOf(value)).thenApply(v -> v == null ? defValue : Long.parseLong(v)));
        });
    }

    @Override
    public <T> T get(String key, final Type type) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs = command.get(key);
        releaseBytesCommand(command);
        return decryptValue(key, cryptor, type, bs);
    }

    @Override
    public <T> T getSet(String key, final Type type, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs = command.setGet(key, encryptValue(key, cryptor, type, convert, value));
        releaseBytesCommand(command);
        return decryptValue(key, cryptor, type, bs);
    }

    @Override
    public <T> T getSet(String key, Convert convert0, final Type type, T value) {
        Convert c = convert0 == null ? this.convert : convert0;
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs = command.setGet(key, encryptValue(key, cryptor, type, c, value));
        releaseBytesCommand(command);
        return decryptValue(key, cryptor, c, type, bs);
    }

    @Override
    public String getString(String key) {
        final RedisCommands<String, String> command = connectString();
        String rs = command.get(key);
        releaseStringCommand(command);
        return cryptor != null ? cryptor.decrypt(key, rs) : rs;
    }

    @Override
    public String getSetString(String key, String value) {
        final RedisCommands<String, String> command = connectString();
        String rs = command.setGet(key, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
        return decryptValue(key, cryptor, rs);
    }

    @Override
    public long getLong(String key, long defValue) {
        final RedisCommands<String, String> command = connectString();
        final String v = command.get(key);
        releaseStringCommand(command);
        return v == null ? defValue : Long.parseLong(v);
    }

    @Override
    public long getSetLong(String key, long value, long defValue) {
        final RedisCommands<String, String> command = connectString();
        final String v = command.setGet(key, String.valueOf(value));
        releaseStringCommand(command);
        return v == null ? defValue : Long.parseLong(v);
    }

    //--------------------- getex ------------------------------
    @Override
    public <T> CompletableFuture<T> getexAsync(String key, int expireSeconds, final Type type) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.getex(key, GetExArgs.Builder.ex(expireSeconds)).thenApply(v -> decryptValue(key, cryptor, type, v)));
        });
    }

    @Override
    public <T> T getex(String key, final int expireSeconds, final Type type) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs = command.getex(key, GetExArgs.Builder.ex(expireSeconds));
        releaseBytesCommand(command);
        if (bs == null) return null;
        return decryptValue(key, cryptor, type, bs);
    }

    @Override
    public CompletableFuture<String> getexStringAsync(String key, int expireSeconds) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, null, command, command.getex(key, GetExArgs.Builder.ex(expireSeconds)).thenApply(v -> cryptor != null ? cryptor.decrypt(key, v) : v));
        });
    }

    @Override
    public String getexString(String key, final int expireSeconds) {
        final RedisCommands<String, String> command = connectString();
        String v = command.getex(key, GetExArgs.Builder.ex(expireSeconds));
        releaseStringCommand(command);
        if (v == null) return null;
        return cryptor != null ? cryptor.decrypt(key, v) : v;
    }

    @Override
    public CompletableFuture<Long> getexLongAsync(String key, int expireSeconds, long defValue) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableLongFuture(command, command.getex(key, GetExArgs.Builder.ex(expireSeconds)).thenApply(v -> v == null ? defValue : Long.parseLong(v)));
        });
    }

    @Override
    public long getexLong(String key, final int expireSeconds, long defValue) {
        final RedisCommands<String, String> command = connectString();
        String v = command.getex(key, GetExArgs.Builder.ex(expireSeconds));
        releaseStringCommand(command);
        if (v == null) return defValue;
        return Long.parseLong(v);
    }

//    //--------------------- setex ------------------------------
    @Override
    public CompletableFuture<Void> msetAsync(final Object... keyVals) {
        if (keyVals.length % 2 != 0) {
            throw new RuntimeException("key value must be paired");
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
        if (map == null || map.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, byte[]> rs = new LinkedHashMap<>();
        map.forEach((key, val) -> {
            rs.put(key.toString(), encryptValue(key.toString(), cryptor, convert, val));
        });
        return connectBytesAsync().thenCompose(command -> completableBytesFuture(command, command.mset(rs)));
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.set(key, encryptValue(key, cryptor, type, this.convert, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Void> setAsync(String key, Convert convert0, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.set(key, encryptValue(key, cryptor, type, convert0, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Void> setnxAsync(String key, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setnx(key, encryptValue(key, cryptor, type, this.convert, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Void> setnxAsync(String key, Convert convert0, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setnx(key, encryptValue(key, cryptor, type, convert0, value)));
        });
    }

    @Override
    public <T> CompletableFuture<T> getSetAsync(String key, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setGet(key, encryptValue(key, cryptor, type, this.convert, value))
                .thenApply(old -> decryptValue(key, cryptor, type, old)));
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

    @Override
    public void mset(final Object... keyVals) {
        if (keyVals.length % 2 != 0) {
            throw new RuntimeException("key value must be paired");
        }
        Map<String, byte[]> map = new LinkedHashMap<>();
        for (int i = 0; i < keyVals.length; i += 2) {
            String key = keyVals[i].toString();
            Object val = keyVals[i + 1];
            map.put(key, encryptValue(key, cryptor, convert, val));
        }
        final RedisCommands<String, byte[]> command = connectBytes();
        command.mset(map);
        releaseBytesCommand(command);
    }

    @Override
    public void mset(final Map map) {
        if (map == null || map.isEmpty()) {
            return;
        }
        Map<String, byte[]> rs = new LinkedHashMap<>();
        map.forEach((key, val) -> {
            rs.put(key.toString(), encryptValue(key.toString(), cryptor, convert, val));
        });
        final RedisCommands<String, byte[]> command = connectBytes();
        command.mset(rs);
        releaseBytesCommand(command);
    }

    @Override
    public <T> void set(final String key, final Type type, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.set(key, encryptValue(key, cryptor, type, this.convert, value));
        releaseBytesCommand(command);
    }

    @Override
    public <T> void set(String key, final Convert convert0, final Type type, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.set(key, encryptValue(key, cryptor, type, convert0, value));
        releaseBytesCommand(command);
    }

    @Override
    public <T> void setnx(final String key, final Type type, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.setnx(key, encryptValue(key, cryptor, type, this.convert, value));
        releaseBytesCommand(command);
    }

    @Override
    public <T> void setnx(String key, final Convert convert0, final Type type, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.setnx(key, encryptValue(key, cryptor, type, convert0, value));
        releaseBytesCommand(command);
    }

    @Override
    public CompletableFuture<Void> setStringAsync(String key, String value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.set(key, encryptValue(key, cryptor, value)));
        });
    }

    @Override
    public CompletableFuture<Void> setnxStringAsync(String key, String value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.setnx(key, encryptValue(key, cryptor, value)));
        });
    }

    @Override
    public void setString(String key, String value) {
        final RedisCommands<String, String> command = connectString();
        command.set(key, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
    }

    @Override
    public void setnxString(String key, String value) {
        final RedisCommands<String, String> command = connectString();
        command.setnx(key, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
    }

    @Override
    public CompletableFuture<Void> setLongAsync(String key, long value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.set(key, String.valueOf(value)));
        });
    }

    @Override
    public CompletableFuture<Void> setnxLongAsync(String key, long value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.setnx(key, String.valueOf(value)));
        });
    }

    @Override
    public void setLong(String key, long value) {
        final RedisCommands<String, String> command = connectString();
        command.set(key, String.valueOf(value));
        releaseStringCommand(command);
    }

    @Override
    public void setnxLong(String key, long value) {
        final RedisCommands<String, String> command = connectString();
        command.setnx(key, String.valueOf(value));
        releaseStringCommand(command);
    }

    @Override
    public void setnxBytes(final String key, final byte[] value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.setnx(key, value);
        releaseBytesCommand(command);
    }

    @Override
    public CompletableFuture<Void> setnxBytesAsync(String key, byte[] value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setnx(key, value));
        });
    }

//    //--------------------- setex ------------------------------    
    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setex(key, expireSeconds, encryptValue(key, cryptor, type, convert, value)).thenApply(r -> null));

        });
    }

    @Override
    public <T> CompletableFuture<Void> setexAsync(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setex(key, expireSeconds, encryptValue(key, cryptor, type, convert0, value)).thenApply(r -> null));
        });
    }

    @Override
    public <T> void setex(String key, int expireSeconds, final Type type, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.setex(key, expireSeconds, encryptValue(key, cryptor, type, convert, value));
        releaseBytesCommand(command);
    }

    @Override
    public <T> void setex(String key, int expireSeconds, Convert convert0, final Type type, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.setex(key, expireSeconds, encryptValue(key, cryptor, type, convert0, value));
        releaseBytesCommand(command);
    }

    @Override
    public CompletableFuture<Void> setexStringAsync(String key, int expireSeconds, String value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.setex(key, expireSeconds, encryptValue(key, cryptor, value)).thenApply(r -> null));
        });
    }

    @Override
    public void setexString(String key, int expireSeconds, String value) {
        final RedisCommands<String, String> command = connectString();
        command.setex(key, expireSeconds, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
    }

    @Override
    public CompletableFuture<Void> setexLongAsync(String key, int expireSeconds, long value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.setex(key, expireSeconds, String.valueOf(value)).thenApply(r -> null));
        });
    }

    @Override
    public void setexLong(String key, int expireSeconds, long value) {
        final RedisCommands<String, String> command = connectString();
        command.setex(key, expireSeconds, String.valueOf(value));
        releaseStringCommand(command);
    }

//    //--------------------- expire ------------------------------    
    @Override
    public CompletableFuture<Void> expireAsync(String key, int expireSeconds) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.expire(key, Duration.ofSeconds(expireSeconds)).thenApply(r -> null));
        });
    }

    @Override
    public void expire(String key, int expireSeconds) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.expire(key, Duration.ofSeconds(expireSeconds));
        releaseBytesCommand(command);
    }

//    //--------------------- del ------------------------------    
    @Override
    public CompletableFuture<Integer> delAsync(String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.del(keys).thenApply(rs -> rs > 0 ? 1 : 0));
        });
    }

    @Override
    public int del(String... keys) {
        final RedisCommands<String, byte[]> command = connectBytes();
        int rs = command.del(keys).intValue();
        releaseBytesCommand(command);
        return rs;
    }

//    //--------------------- incrby ------------------------------    
    @Override
    public long incr(final String key) {
        final RedisCommands<String, byte[]> command = connectBytes();
        long rs = command.incr(key);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.incr(key));
        });
    }

    @Override
    public long incrby(final String key, long num) {
        final RedisCommands<String, byte[]> command = connectBytes();
        long rs = command.incrby(key, num);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public double incrbyFloat(final String key, double num) {
        final RedisCommands<String, byte[]> command = connectBytes();
        Double rs = command.incrbyfloat(key, num);
        releaseBytesCommand(command);
        return rs;
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
//    //--------------------- decrby ------------------------------    

    @Override
    public long decr(final String key) {
        final RedisCommands<String, byte[]> command = connectBytes();
        long rs = command.decr(key);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.decr(key));
        });
    }

    @Override
    public long decrby(final String key, long num) {
        final RedisCommands<String, byte[]> command = connectBytes();
        long rs = command.decrby(key, num);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public CompletableFuture<Long> decrbyAsync(final String key, long num) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.decrby(key, num));
        });
    }

    @Override
    public int hdel(final String key, String... fields) {
        final RedisCommands<String, byte[]> command = connectBytes();
        int rs = command.hdel(key, fields).intValue();
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public int hlen(final String key) {
        final RedisCommands<String, byte[]> command = connectBytes();
        int rs = command.hlen(key).intValue();
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public List<String> hkeys(final String key) {
        final RedisCommands<String, String> command = connectString();
        List<String> rs = command.hkeys(key);
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public long hincr(final String key, String field) {
        final RedisCommands<String, String> command = connectString();
        long rs = command.hincrby(key, field, 1L);
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public long hincrby(final String key, String field, long num) {
        final RedisCommands<String, String> command = connectString();
        long rs = command.hincrby(key, field, num);
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public double hincrbyFloat(final String key, String field, double num) {
        final RedisCommands<String, String> command = connectString();
        double rs = command.hincrbyfloat(key, field, num);
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public long hdecr(final String key, String field) {
        final RedisCommands<String, String> command = connectString();
        long rs = command.hincrby(key, field, -1L);
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public long hdecrby(final String key, String field, long num) {
        final RedisCommands<String, String> command = connectString();
        long rs = command.hincrby(key, field, -num);
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public boolean hexists(final String key, String field) {
        final RedisCommands<String, byte[]> command = connectBytes();
        boolean rs = command.hget(key, field) != null;
        releaseBytesCommand(command);
        return rs;
    }

//    //--------------------- dbsize ------------------------------  
    @Override
    public int dbsize() {
        final RedisCommands<String, byte[]> command = connectBytes();
        List<String> keys = command.keys("*");
        releaseBytesCommand(command);
        return keys == null ? 0 : keys.size();
    }

    @Override
    public CompletableFuture<Integer> dbsizeAsync() {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.keys("*").thenApply(v -> v == null ? 0 : v.size()));
        });
    }

    @Override
    public <T> void hset(final String key, final String field, final Type type, final T value) {
        if (value == null) return;
        final RedisCommands<String, byte[]> command = connectBytes();
        command.hset(key, field, encryptValue(key, cryptor, type, this.convert, value));
        releaseBytesCommand(command);
    }

    @Override
    public <T> void hset(final String key, final String field, final Convert convert0, final Type type, final T value) {
        if (value == null) return;
        final RedisCommands<String, byte[]> command = connectBytes();
        command.hset(key, field, encryptValue(key, cryptor, type, convert0, value));
        releaseBytesCommand(command);
    }

    @Override
    public void hsetString(final String key, final String field, final String value) {
        if (value == null) return;
        final RedisCommands<String, String> command = connectString();
        command.hset(key, field, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
    }

    @Override
    public void hsetLong(final String key, final String field, final long value) {
        final RedisCommands<String, String> command = connectString();
        command.hset(key, field, String.valueOf(value));
        releaseStringCommand(command);
    }

    @Override
    public <T> void hsetnx(final String key, final String field, final Type type, final T value) {
        if (value == null) return;
        final RedisCommands<String, byte[]> command = connectBytes();
        command.hsetnx(key, field, encryptValue(key, cryptor, type, this.convert, value));
        releaseBytesCommand(command);
    }

    @Override
    public <T> void hsetnx(final String key, final String field, final Convert convert0, final Type type, final T value) {
        if (value == null) return;
        final RedisCommands<String, byte[]> command = connectBytes();
        command.hsetnx(key, field, encryptValue(key, cryptor, type, convert0, value));
        releaseBytesCommand(command);
    }

    @Override
    public void hsetnxString(final String key, final String field, final String value) {
        if (value == null) return;
        final RedisCommands<String, String> command = connectString();
        command.hsetnx(key, field, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
    }

    @Override
    public void hsetnxLong(final String key, final String field, final long value) {
        final RedisCommands<String, String> command = connectString();
        command.hsetnx(key, field, String.valueOf(value));
        releaseStringCommand(command);
    }

    @Override
    public void hmset(final String key, final Serializable... values) {
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
        final RedisCommands<String, byte[]> command = connectBytes();
        command.hmset(key, vals);
        releaseBytesCommand(command);
    }

    @Override
    public void hmset(final String key, final Map map) {
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
        final RedisCommands<String, byte[]> command = connectBytes();
        command.hmset(key, vals);
        releaseBytesCommand(command);
    }

    @Override
    public List<Serializable> hmget(final String key, final Type type, final String... fields) {
        final RedisCommands<String, byte[]> command = connectBytes();
        List<KeyValue<String, byte[]>> rs = command.hmget(key, fields);
        releaseBytesCommand(command);
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
    }

    @Override
    public <T> Map<String, T> hmap(final String key, final Type type, int offset, int limit, String pattern) {
        final RedisCommands<String, byte[]> command = connectBytes();
        ScanArgs args = ScanArgs.Builder.limit(limit);
        if (pattern != null) args = args.match(pattern);
        MapScanCursor<String, byte[]> rs = command.hscan(key, new ScanCursor(String.valueOf(offset), false), args);
        releaseBytesCommand(command);
        final Map<String, T> map = new LinkedHashMap<>();
        rs.getMap().forEach((k, v) -> map.put(k, decryptValue(key, cryptor, type, v)));
        return map;
    }

    @Override
    public <T> Map<String, T> hmap(final String key, final Type type, int offset, int limit) {
        return hmap(key, type, offset, limit, null);
    }

    @Override
    public <T> T hget(final String key, final String field, final Type type) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs = command.hget(key, field);
        releaseBytesCommand(command);
        return decryptValue(key, cryptor, type, bs);
    }

    @Override
    public String hgetString(final String key, final String field) {
        final RedisCommands<String, String> command = connectString();
        String rs = command.hget(key, field);
        releaseStringCommand(command);
        return cryptor != null ? cryptor.decrypt(key, rs) : rs;
    }

    @Override
    public long hgetLong(final String key, final String field, long defValue) {
        final RedisCommands<String, String> command = connectString();
        String rs = command.hget(key, field);
        releaseStringCommand(command);
        if (rs == null) return defValue;
        try {
            return Long.parseLong(rs);
        } catch (NumberFormatException e) {
            return defValue;
        }
    }

    @Override
    public <T> Map<String, T> mget(final Type componentType, final String... keys) {
        final RedisCommands<String, byte[]> command = connectBytes();
        List<KeyValue<String, byte[]>> rs = command.mget(keys);
        releaseBytesCommand(command);
        Map<String, T> map = new LinkedHashMap(rs.size());
        rs.forEach(kv -> {
            if (kv.hasValue()) map.put(kv.getKey(), decryptValue(kv.getKey(), cryptor, componentType, kv.getValue()));
        });
        return map;
    }

    @Override
    public Map<String, byte[]> mgetBytes(final String... keys) {
        final RedisCommands<String, byte[]> command = connectBytes();
        List<KeyValue<String, byte[]>> rs = command.mget(keys);
        releaseBytesCommand(command);
        Map<String, byte[]> map = new LinkedHashMap(rs.size());
        rs.forEach(kv -> {
            if (kv.hasValue()) map.put(kv.getKey(), decryptValue(kv.getKey(), cryptor, byte[].class, kv.getValue()));
        });
        return map;
    }

    @Override
    public <T> Set<T> smembers(String key, final Type componentType) {
        final RedisCommands<String, byte[]> command = connectBytes();
        Set<T> rs = formatCollection(key, cryptor, command.smembers(key), convert, componentType);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public <T> List<T> lrange(String key, final Type componentType) {
        final RedisCommands<String, byte[]> command = connectBytes();
        List<T> rs = formatCollection(key, cryptor, command.lrange(key, 0, -1), convert, componentType);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public <T> Collection<T> getCollection(String key, final Type componentType) {
        final RedisCommands<String, byte[]> command = connectBytes();
        Collection<T> rs = getCollection(command, key, componentType);
        releaseBytesCommand(command);
        return rs;
    }

    protected <T> Collection<T> getCollection(final RedisCommands<String, byte[]> command, String key, final Type componentType) {
        final String type = command.type(key);
        if (type.contains("list")) {
            return formatCollection(key, cryptor, command.lrange(key, 0, -1), convert, componentType);
        } else { //set
            return formatCollection(key, cryptor, command.smembers(key), convert, componentType);
        }
    }

    @Override
    public <T> Map<String, Collection<T>> getCollectionMap(final boolean set, final Type componentType, String... keys) {
        final RedisCommands<String, byte[]> command = connectBytes();
        final Map<String, Collection<T>> map = new LinkedHashMap<>();
        if (set) {  //set
            for (String key : keys) {
                map.put(key, formatCollection(key, cryptor, command.smembers(key), convert, componentType));
            }
        } else { //list
            for (String key : keys) {
                map.put(key, formatCollection(key, cryptor, command.lrange(key, 0, -1), convert, componentType));
            }
        }
        releaseBytesCommand(command);
        return map;
    }

    @Override
    public <T> Map<String, Set<T>> smembers(final Type componentType, String... keys) {
        final RedisCommands<String, byte[]> command = connectBytes();
        final Map<String, Set<T>> map = new LinkedHashMap<>();
        for (String key : keys) {
            map.put(key, formatCollection(key, cryptor, command.smembers(key), convert, componentType));
        }
        releaseBytesCommand(command);
        return map;
    }

    @Override
    public <T> Map<String, List<T>> lrange(final Type componentType, String... keys) {
        final RedisCommands<String, byte[]> command = connectBytes();
        final Map<String, List<T>> map = new LinkedHashMap<>();
        for (String key : keys) {
            map.put(key, formatCollection(key, cryptor, command.lrange(key, 0, -1), convert, componentType));
        }
        releaseBytesCommand(command);
        return map;
    }

    @Override
    public int getCollectionSize(String key) {
        final RedisCommands<String, byte[]> command = connectBytes();
        final String type = command.type(key);
        int rs;
        if (type.contains("list")) {
            rs = command.llen(key).intValue();
        } else { //set
            rs = command.scard(key).intValue();
        }
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public int llen(String key) {
        final RedisCommands<String, byte[]> command = connectBytes();
        int rs = command.llen(key).intValue();
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public int scard(String key) {
        final RedisCommands<String, byte[]> command = connectBytes();
        int rs = command.scard(key).intValue();
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public <T> Collection<T> getexCollection(String key, final int expireSeconds, final Type componentType) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.expire(key, Duration.ofSeconds(expireSeconds));
        Collection<T> rs = getCollection(command, key, componentType);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    @SuppressWarnings({"ConfusingArrayVararg", "PrimitiveArrayArgumentToVariableArgMethod"})
    public <T> void rpush(String key, final Type componentType, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs;
        if (cryptor != null && componentType == String.class) {
            bs = cryptor.encrypt(key, String.valueOf(value)).getBytes(StandardCharsets.UTF_8);
        } else {
            bs = encryptValue(key, cryptor, componentType, convert, value);
        }
        command.rpush(key, bs);
        releaseBytesCommand(command);
    }

    @Override
    public <T> int lrem(String key, final Type componentType, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs;
        if (cryptor != null && componentType == String.class) {
            bs = cryptor.encrypt(key, String.valueOf(value)).getBytes(StandardCharsets.UTF_8);
        } else {
            bs = encryptValue(key, cryptor, componentType, convert, value);
        }
        int rs = command.lrem(key, 1L, bs).intValue();
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public <T> boolean sismember(String key, final Type componentType, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs;
        if (cryptor != null && componentType == String.class) {
            bs = cryptor.encrypt(key, String.valueOf(value)).getBytes(StandardCharsets.UTF_8);
        } else {
            bs = encryptValue(key, cryptor, componentType, convert, value);
        }
        boolean rs = command.sismember(key, bs);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    @SuppressWarnings({"ConfusingArrayVararg", "PrimitiveArrayArgumentToVariableArgMethod"})
    public <T> void sadd(String key, final Type componentType, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs;
        if (cryptor != null && componentType == String.class) {
            bs = cryptor.encrypt(key, String.valueOf(value)).getBytes(StandardCharsets.UTF_8);
        } else {
            bs = encryptValue(key, cryptor, componentType, convert, value);
        }
        command.sadd(key, bs);
        releaseBytesCommand(command);
    }

    @Override
    @SuppressWarnings({"ConfusingArrayVararg", "PrimitiveArrayArgumentToVariableArgMethod"})
    public <T> int srem(String key, final Type componentType, T value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs;
        if (cryptor != null && componentType == String.class) {
            bs = cryptor.encrypt(key, String.valueOf(value)).getBytes(StandardCharsets.UTF_8);
        } else {
            bs = encryptValue(key, cryptor, componentType, convert, value);
        }
        int rs = command.srem(key, bs).intValue();
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public <T> T spop(String key, final Type componentType) {
        final RedisCommands<String, byte[]> command = connectBytes();
        T rs = decryptValue(key, cryptor, componentType, command.spop(key));
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public <T> Set<T> spop(String key, int count, final Type componentType) {
        final RedisCommands<String, byte[]> command = connectBytes();
        Set<T> rs = formatCollection(key, cryptor, command.spop(key, count), convert, componentType);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public byte[] getBytes(final String key) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs = command.get(key);
        releaseBytesCommand(command);
        return bs;
    }

    @Override
    public byte[] getSetBytes(final String key, byte[] value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs = command.setGet(key, value);
        releaseBytesCommand(command);
        return bs;
    }

    @Override
    public byte[] getexBytes(final String key, final int expireSeconds) {
        final RedisCommands<String, byte[]> command = connectBytes();
        byte[] bs = command.getex(key, GetExArgs.Builder.ex(expireSeconds));
        releaseBytesCommand(command);
        return bs;
    }

    @Override
    public void setBytes(final String key, final byte[] value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.set(key, value);
        releaseBytesCommand(command);
    }

    @Override
    public void setexBytes(final String key, final int expireSeconds, final byte[] value) {
        final RedisCommands<String, byte[]> command = connectBytes();
        command.setex(key, expireSeconds, value);
        releaseBytesCommand(command);
    }

    @Override
    public List<String> keys(String pattern) {
        final RedisCommands<String, byte[]> command = connectBytes();
        List<String> rs = command.keys(pattern == null || pattern.isEmpty() ? "*" : pattern);
        releaseBytesCommand(command);
        return rs;
    }

    @Override
    public Map<String, String> mgetString(final String... keys) {
        final RedisCommands<String, String> command = connectString();
        List<KeyValue<String, String>> rs = command.mget(keys);
        releaseStringCommand(command);
        Map<String, String> map = new LinkedHashMap<>();
        rs.forEach(kv -> {
            if (kv.hasValue()) map.put(kv.getKey(), cryptor != null ? cryptor.decrypt(kv.getKey(), kv.getValue()) : kv.getValue());
        });
        return map;
    }

    @Override
    public String[] getStringArray(final String... keys) {
        final RedisCommands<String, String> command = connectString();
        List<KeyValue<String, String>> rs = command.mget(keys);
        releaseStringCommand(command);
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
    }

    @Override
    public Collection<String> getStringCollection(String key) {
        final RedisCommands<String, String> command = connectString();
        Collection<String> rs = getStringCollection(command, key);
        releaseStringCommand(command);
        return rs;
    }

    protected Collection<String> decryptStringCollection(String key, final boolean set, Collection<String> collection) {
        if (collection == null || collection.isEmpty() || cryptor == null) return collection;
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

    protected Collection<String> getStringCollection(final RedisCommands<String, String> command, String key) {
        final String type = command.type(key);
        if (type.contains("list")) { //list
            return decryptStringCollection(key, false, command.lrange(key, 0, -1));
        } else { //set
            return decryptStringCollection(key, true, command.smembers(key));
        }
    }

    @Override
    public Map<String, Collection<String>> getStringCollectionMap(final boolean set, String... keys) {
        final RedisCommands<String, String> command = connectString();
        final Map<String, Collection<String>> map = new LinkedHashMap<>();
        if (set) {//set    
            for (String key : keys) {
                map.put(key, decryptStringCollection(key, true, command.smembers(key)));
            }
        } else {  //list           
            for (String key : keys) {
                map.put(key, decryptStringCollection(key, false, command.lrange(key, 0, -1)));
            }
        }
        releaseStringCommand(command);
        return map;
    }

    @Override
    public Collection<String> getexStringCollection(String key, final int expireSeconds) {
        final RedisCommands<String, String> command = connectString();
        command.expire(key, expireSeconds);
        Collection<String> rs = getStringCollection(command, key);
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public void rpushString(String key, String value) {
        final RedisCommands<String, String> command = connectString();
        command.rpush(key, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
    }

    @Override
    public String spopString(String key) {
        final RedisCommands<String, String> command = connectString();
        String rs = command.spop(key);
        releaseStringCommand(command);
        return cryptor != null ? cryptor.encrypt(key, rs) : rs;
    }

    @Override
    public Set<String> spopString(String key, int count) {
        final RedisCommands<String, String> command = connectString();
        Set<String> rs = command.spop(key, count);
        releaseStringCommand(command);
        return (Set) decryptStringCollection(key, true, rs);
    }

    @Override
    public int lremString(String key, String value) {
        final RedisCommands<String, String> command = connectString();
        int rs = command.lrem(key, 1, encryptValue(key, cryptor, value)).intValue();
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public boolean sismemberString(String key, String value) {
        final RedisCommands<String, String> command = connectString();
        boolean rs = command.sismember(key, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public void saddString(String key, String value) {
        final RedisCommands<String, String> command = connectString();
        command.sadd(key, encryptValue(key, cryptor, value));
        releaseStringCommand(command);
    }

    @Override
    public int sremString(String key, String value) {
        final RedisCommands<String, String> command = connectString();
        int rs = command.srem(key, encryptValue(key, cryptor, value)).intValue();
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public Map<String, Long> mgetLong(String... keys) {
        final RedisCommands<String, String> command = connectString();
        List<KeyValue<String, String>> rs = command.mget(keys);
        releaseStringCommand(command);
        Map<String, Long> map = new LinkedHashMap<>();
        rs.forEach(kv -> {
            if (kv.hasValue()) map.put(kv.getKey(), Long.parseLong(kv.getValue()));
        });
        return map;
    }

    @Override
    public Long[] getLongArray(String... keys) {
        final RedisCommands<String, String> command = connectString();
        List<KeyValue<String, String>> rs = command.mget(keys);
        releaseStringCommand(command);
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
    }

    @Override
    public Collection<Long> getLongCollection(String key) {
        final RedisCommands<String, String> command = connectString();
        Collection<Long> rs = getLongCollection(command, key);
        releaseStringCommand(command);
        return rs;
    }

    protected Collection<Long> getLongCollection(final RedisCommands<String, String> command, String key) {
        final String type = command.type(key);
        Collection<Long> rs;
        if (type.contains("list")) { //list
            rs = formatLongCollection(false, command.lrange(key, 0, -1));
        } else { //set
            rs = formatLongCollection(true, command.smembers(key));
        }
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public Map<String, Collection<Long>> getLongCollectionMap(boolean set, String... keys) {
        final RedisCommands<String, String> command = connectString();
        final Map<String, Collection<Long>> map = new LinkedHashMap<>();
        if (set) { //set
            for (String key : keys) {
                map.put(key, formatLongCollection(true, command.smembers(key)));
            }
        } else { //list                
            for (String key : keys) {
                map.put(key, formatLongCollection(false, command.lrange(key, 0, -1)));
            }
        }
        releaseStringCommand(command);
        return map;
    }

    @Override
    public Collection<Long> getexLongCollection(String key, int expireSeconds) {
        final RedisCommands<String, String> command = connectString();
        command.expire(key, expireSeconds);
        releaseStringCommand(command);
        return getLongCollection(key);
    }

    @Override
    public void rpushLong(String key, long value) {
        final RedisCommands<String, String> command = connectString();
        command.rpush(key, String.valueOf(value));
        releaseStringCommand(command);
    }

    @Override
    public Long spopLong(String key) {
        final RedisCommands<String, String> command = connectString();
        String value = command.spop(key);
        releaseStringCommand(command);
        return value == null ? null : Long.parseLong(value);
    }

    @Override
    public Set<Long> spopLong(String key, int count) {
        final RedisCommands<String, String> command = connectString();
        Set<Long> rs = (Set) formatLongCollection(true, command.spop(key, count));
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public int lremLong(String key, long value) {
        final RedisCommands<String, String> command = connectString();
        int rs = command.lrem(key, 1, String.valueOf(value)).intValue();
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public boolean sismemberLong(String key, long value) {
        final RedisCommands<String, String> command = connectString();
        boolean rs = command.sismember(key, String.valueOf(value));
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public void saddLong(String key, long value) {
        final RedisCommands<String, String> command = connectString();
        command.sadd(key, String.valueOf(value));
        releaseStringCommand(command);
    }

    @Override
    public int sremLong(String key, long value) {
        final RedisCommands<String, String> command = connectString();
        int rs = command.srem(key, String.valueOf(value)).intValue();
        releaseStringCommand(command);
        return rs;
    }

    @Override
    public CompletableFuture<Integer> hdelAsync(String key, String... fields) {
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
    public CompletableFuture<Integer> hlenAsync(String key) {
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
    public <T> CompletableFuture<Void> hsetAsync(String key, String field, Type type, T value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hset(key, field, convert.convertToBytes(type, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Void> hsetAsync(String key, String field, Convert convert0, Type type, T value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hset(key, field, (convert0 == null ? convert : convert0).convertToBytes(type, value)));
        });
    }

    @Override
    public CompletableFuture<Void> hsetStringAsync(String key, String field, String value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.hset(key, field, value));
        });
    }

    @Override
    public CompletableFuture<Void> hsetLongAsync(String key, String field, long value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.hset(key, field, String.valueOf(value)));
        });
    }

    @Override
    public <T> CompletableFuture<Void> hsetnxAsync(String key, String field, Type type, T value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hsetnx(key, field, convert.convertToBytes(type, value)));
        });
    }

    @Override
    public <T> CompletableFuture<Void> hsetnxAsync(String key, String field, Convert convert0, Type type, T value) {
        if (value == null) return CompletableFuture.completedFuture(null);
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hsetnx(key, field, (convert0 == null ? convert : convert0).convertToBytes(type, value)));
        });
    }

    @Override
    public CompletableFuture<Void> hsetnxStringAsync(String key, String field, String value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.hsetnx(key, field, value));
        });
    }

    @Override
    public CompletableFuture<Void> hsetnxLongAsync(String key, String field, long value) {
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
    public <T> CompletableFuture<Map<String, T>> hmapAsync(String key, Type type, int offset, int limit) {
        return hmapAsync(key, type, offset, limit, null);
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> hmapAsync(String key, Type type, int offset, int limit, String pattern) {
        return connectBytesAsync().thenCompose(command -> {
            ScanArgs args = ScanArgs.Builder.limit(limit);
            if (pattern != null) args = args.match(pattern);
            return completableBytesFuture(command, command.hscan(key, new ScanCursor(String.valueOf(offset), false), args).thenApply((MapScanCursor<String, byte[]> rs) -> {
                final Map<String, T> map = new LinkedHashMap<>();
                rs.getMap().forEach((k, v) -> map.put(k, v == null ? null : decryptValue(key, cryptor, type, v)));
                return map;
            }));
        });
    }

    @Override
    public <T> CompletableFuture<T> hgetAsync(String key, String field, Type type) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.hget(key, field).thenApply(bs -> bs == null ? null : decryptValue(key, cryptor, type, bs)));
        });
    }

    @Override
    public CompletableFuture<String> hgetStringAsync(String key, String field) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, cryptor, command, command.hget(key, field));
        });
    }

    @Override
    public CompletableFuture<Long> hgetLongAsync(String key, String field, long defValue) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.hget(key, field).thenApply(bs -> bs == null ? defValue : Long.parseLong(bs)));
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, T>> mgetAsync(Type componentType, String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.mget(keys).thenApply((List<KeyValue<String, byte[]>> rs) -> {
                Map<String, T> map = new LinkedHashMap(rs.size());
                rs.forEach(kv -> {
                    if (kv.hasValue()) map.put(kv.getKey(), decryptValue(kv.getKey(), cryptor, componentType, kv.getValue()));
                });
                return map;
            }));
        });
    }

    @Override
    public CompletableFuture<Map<String, byte[]>> mgetBytesAsync(String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.mget(keys).thenApply((List<KeyValue<String, byte[]>> rs) -> {
                Map<String, byte[]> map = new LinkedHashMap(rs.size());
                rs.forEach(kv -> {
                    if (kv.hasValue()) map.put(kv.getKey(), decryptValue(kv.getKey(), cryptor, byte[].class, kv.getValue()));
                });
                return map;
            }));
        });
    }

    @Override
    public <T> CompletableFuture<Set<T>> smembersAsync(String key, final Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return command.smembers(key).thenApply(set -> formatCollection(key, cryptor, set, convert, componentType));
        });
    }

    @Override
    public <T> CompletableFuture<List<T>> lrangeAsync(String key, final Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return command.lrange(key, 0, -1).thenApply(list -> formatCollection(key, cryptor, list, convert, componentType));
        });
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getCollectionAsync(String key, final Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return getCollectionAsync(command, key, componentType);
        });
    }

    protected <T> CompletableFuture<Collection<T>> getCollectionAsync(
        final RedisAsyncCommands<String, byte[]> command, String key, final Type componentType) {
        return completableBytesFuture(command, command.type(key).thenCompose(type -> {
            if (type.contains("list")) {
                return command.lrange(key, 0, -1).thenApply(list -> formatCollection(key, cryptor, list, convert, componentType));
            } else { //set
                return command.smembers(key).thenApply(set -> formatCollection(key, cryptor, set, convert, componentType));
            }
        }));
    }

    @Override
    public <T> CompletableFuture<Map<String, Collection<T>>> getCollectionMapAsync(boolean set, Type componentType, String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            final Map<String, Collection<T>> map = new LinkedHashMap<>();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (set) {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.smembers(key).thenAccept(rs -> {
                        if (rs != null) {
                            synchronized (map) {
                                map.put(key, formatCollection(key, cryptor, rs, convert, componentType));
                            }
                        }
                    }).toCompletableFuture();
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.lrange(key, 0, -1).thenAccept(rs -> {
                        if (rs != null) {
                            synchronized (map) {
                                map.put(key, formatCollection(key, cryptor, rs, convert, componentType));
                            }
                        }
                    }).toCompletableFuture();
                }
            }
            return CompletableFuture.allOf(futures).thenApply(v -> map);
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, List<T>>> lrangeAsync(Type componentType, String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            final Map<String, List<T>> map = new LinkedHashMap<>();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = command.lrange(key, 0, -1).thenAccept(rs -> {
                    if (rs != null) {
                        synchronized (map) {
                            map.put(key, formatCollection(key, cryptor, rs, convert, componentType));
                        }
                    }
                }).toCompletableFuture();
            }
            return CompletableFuture.allOf(futures).thenApply(v -> map);
        });
    }

    @Override
    public <T> CompletableFuture<Map<String, Set<T>>> smembersAsync(Type componentType, String... keys) {
        return connectBytesAsync().thenCompose(command -> {
            final Map<String, Set<T>> map = new LinkedHashMap<>();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            for (int i = 0; i < keys.length; i++) {
                final String key = keys[i];
                futures[i] = command.smembers(key).thenAccept(rs -> {
                    if (rs != null) {
                        synchronized (map) {
                            map.put(key, formatCollection(key, cryptor, rs, convert, componentType));
                        }
                    }
                }).toCompletableFuture();
            }
            return CompletableFuture.allOf(futures).thenApply(v -> map);
        });
    }

    @Override
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
    public CompletableFuture<Integer> llenAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.type(key).thenCompose(type -> {
                return command.llen(key).thenApply(v -> v.intValue());
            }));
        });
    }

    @Override
    public CompletableFuture<Integer> scardAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.type(key).thenCompose(type -> {
                return command.scard(key).thenApply(v -> v.intValue());
            }));
        });
    }

    @Override
    public <T> CompletableFuture<Collection<T>> getexCollectionAsync(String key, int expireSeconds, final Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.expire(key, expireSeconds).thenCompose(v -> getCollectionAsync(command, key, componentType)));
        });
    }

    @Override
    public <T> CompletableFuture<T> spopAsync(String key, Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.spop(key).thenApply(bs -> bs == null ? null : decryptValue(key, cryptor, componentType, bs)));
        });
    }

    @Override
    public <T> CompletableFuture<Set<T>> spopAsync(String key, int count, Type componentType) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.spop(key, count).thenApply(v -> formatCollection(key, cryptor, v, convert, componentType)));
        });
    }

    @Override
    public <T> CompletableFuture<Void> rpushAsync(String key, Type componentType, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.rpush(key, encryptValue(key, cryptor, componentType, convert.convertToBytes(componentType, value))));
        });
    }

    @Override
    public <T> CompletableFuture<Integer> lremAsync(String key, Type componentType, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.lrem(key, 1, encryptValue(key, cryptor, componentType, convert.convertToBytes(componentType, value))).thenApply(v -> v.intValue()));
        });
    }

    @Override
    public <T> CompletableFuture<Boolean> sismemberAsync(String key, Type componentType, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sismember(key, encryptValue(key, cryptor, componentType, convert.convertToBytes(componentType, value))));
        });
    }

    @Override
    @SuppressWarnings({"ConfusingArrayVararg", "PrimitiveArrayArgumentToVariableArgMethod"})
    public <T> CompletableFuture<Void> saddAsync(String key, Type componentType, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.sadd(key, encryptValue(key, cryptor, componentType, convert.convertToBytes(componentType, value))));
        });
    }

    @Override
    public <T> CompletableFuture<Integer> sremAsync(String key, Type componentType, T value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.srem(key, encryptValue(key, cryptor, componentType, convert.convertToBytes(componentType, value))).thenApply(v -> v.intValue()));
        });
    }

    @Override
    public CompletableFuture<byte[]> getBytesAsync(String key) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.get(key));
        });
    }

    @Override
    public CompletableFuture<byte[]> getSetBytesAsync(String key, byte[] value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setGet(key, value));
        });
    }

    @Override
    public CompletableFuture<byte[]> getexBytesAsync(String key, int expireSeconds) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.expire(key, expireSeconds).thenCompose(v -> command.get(key)));
        });
    }

    @Override
    public CompletableFuture<Void> setBytesAsync(String key, byte[] value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.set(key, value));
        });
    }

    @Override
    public CompletableFuture<Void> setexBytesAsync(String key, int expireSeconds, byte[] value) {
        return connectBytesAsync().thenCompose(command -> {
            return completableBytesFuture(command, command.setex(key, expireSeconds, value));
        });
    }

    @Override
    public CompletableFuture<List<String>> keysAsync(String pattern) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取key值，无需传cryptor进行解密
            return completableStringFuture(null, null, command, command.keys(pattern == null || pattern.isEmpty() ? "*" : pattern));
        });
    }

    @Override
    public CompletableFuture<Map<String, String>> mgetStringAsync(String... keys) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(null, null, command, command.mget(keys).thenApply((List<KeyValue<String, String>> rs) -> {
                Map<String, String> map = new LinkedHashMap<>();
                rs.forEach(kv -> {
                    if (kv.hasValue()) map.put(kv.getKey(), cryptor != null ? cryptor.decrypt(kv.getKey(), kv.getValue()) : kv.getValue());
                });
                return map;
            }));
        });
    }

    @Override
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

    @Override
    public CompletableFuture<Collection<String>> getStringCollectionAsync(String key) {
        return getStringCollectionAsync(connectStringAsync(), key);
    }

    protected CompletableFuture<Collection<String>> getStringCollectionAsync(CompletableFuture<RedisAsyncCommands<String, String>> commandFuture, String key) {
        return commandFuture.thenCompose(command -> getStringCollectionAsync(command, key));
    }

    protected CompletableFuture<Collection<String>> getStringCollectionAsync(RedisAsyncCommands<String, String> command, String key) {
        return completableStringFuture(key, null, command, command.type(key).thenApply(type -> {
            if (type.contains("list")) {
                return command.lrange(key, 0, -1).thenApply(list -> formatStringCollection(key, cryptor, false, list));
            } else { //set
                return command.smembers(key).thenApply(list -> formatStringCollection(key, cryptor, true, list));
            }
        }));
    }

    @Override
    public CompletableFuture<Map<String, Collection<String>>> getStringCollectionMapAsync(boolean set, String... keys) {
        return connectStringAsync().thenCompose(command -> {
            final Map<String, Collection<String>> map = new LinkedHashMap<>();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (set) {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.smembers(key).thenAccept(rs -> {
                        if (rs != null) {
                            synchronized (map) {
                                map.put(key, formatStringCollection(key, cryptor, set, rs));
                            }
                        }
                    }).toCompletableFuture();
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.lrange(key, 0, -1).thenAccept(rs -> {
                        if (rs != null) {
                            synchronized (map) {
                                map.put(key, formatStringCollection(key, cryptor, set, rs));
                            }
                        }
                    }).toCompletableFuture();
                }
            }
            return CompletableFuture.allOf(futures).thenApply(v -> map);
        });
    }

    @Override
    public CompletableFuture<Collection<String>> getexStringCollectionAsync(String key, int expireSeconds) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, null, command, command.expire(key, expireSeconds).thenCompose(v -> getStringCollectionAsync(command, key)));
        });
    }

    @Override
    public CompletableFuture<Void> rpushStringAsync(String key, String value) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, null, command, command.rpush(key, encryptValue(key, cryptor, value)));
        });
    }

    @Override
    public CompletableFuture<String> spopStringAsync(String key) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, cryptor, command, command.spop(key));
        });
    }

    @Override
    public CompletableFuture<Set<String>> spopStringAsync(String key, int count) {
        return connectStringAsync().thenCompose(command -> {
            return completableStringFuture(key, null, command, command.spop(key, count).thenApply(list -> formatStringCollection(key, cryptor, true, list)));
        });
    }

    @Override
    public CompletableFuture<Integer> lremStringAsync(String key, String value) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的int值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.lrem(key, 1, value).thenApply(v -> v.intValue()));
        });
    }

    @Override
    public CompletableFuture<Boolean> sismemberStringAsync(String key, String value) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的boolean值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.sismember(key, value));
        });
    }

    @Override
    public CompletableFuture<Void> saddStringAsync(String key, String value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.sadd(key, value));
        });
    }

    @Override
    public CompletableFuture<Integer> sremStringAsync(String key, String value) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的int值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.srem(key, value).thenApply(v -> v.intValue()));
        });
    }

    @Override
    public CompletableFuture<Map<String, Long>> mgetLongAsync(String... keys) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(keys[0], null, command, command.mget(keys).thenApply((List<KeyValue<String, String>> rs) -> {
                Map<String, Long> map = new LinkedHashMap<>();
                rs.forEach(kv -> {
                    if (kv.hasValue()) map.put(kv.getKey(), Long.parseLong(kv.getValue()));
                });
                return map;
            }));
        });
    }

    @Override
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
    public CompletableFuture<Collection<Long>> getLongCollectionAsync(String key) {
        return getLongCollectionAsync(connectStringAsync(), key);
    }

    protected CompletableFuture<Collection<Long>> getLongCollectionAsync(final CompletableFuture<RedisAsyncCommands<String, String>> commandFuture, String key) {
        return commandFuture.thenCompose(command -> getLongCollectionAsync(command, key));
    }

    protected CompletableFuture<Collection<Long>> getLongCollectionAsync(final RedisAsyncCommands<String, String> command, String key) {
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
    public CompletableFuture<Map<String, Collection<Long>>> getLongCollectionMapAsync(boolean set, String... keys) {
        return connectStringAsync().thenCompose(command -> {
            final Map<String, Collection<Long>> map = new LinkedHashMap<>();
            final CompletableFuture[] futures = new CompletableFuture[keys.length];
            if (set) {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.smembers(key).thenAccept(rs -> {
                        if (rs != null) {
                            synchronized (map) {
                                map.put(key, formatLongCollection(set, rs));
                            }
                        }
                    }).toCompletableFuture();
                }
            } else {
                for (int i = 0; i < keys.length; i++) {
                    final String key = keys[i];
                    futures[i] = command.lrange(key, 0, -1).thenAccept(rs -> {
                        if (rs != null) {
                            synchronized (map) {
                                map.put(key, formatLongCollection(set, rs));
                            }
                        }
                    }).toCompletableFuture();
                }
            }
            return CompletableFuture.allOf(futures).thenApply(v -> map);
        });
    }

    @Override
    public CompletableFuture<Collection<Long>> getexLongCollectionAsync(String key, int expireSeconds) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.expire(key, expireSeconds).thenCompose(v -> getLongCollectionAsync(command, key)));
        });
    }

    @Override
    public CompletableFuture<Void> rpushLongAsync(String key, long value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.rpush(key, String.valueOf(value)));
        });
    }

    @Override
    public CompletableFuture<Long> spopLongAsync(String key) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.spop(key).thenApply(v -> v == null ? null : Long.parseLong(v)));
        });
    }

    @Override
    public CompletableFuture<Set<Long>> spopLongAsync(String key, int count) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的long值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.spop(key, count).thenApply(v -> v == null ? null : formatLongCollection(true, v)));
        });
    }

    @Override
    public CompletableFuture<Integer> lremLongAsync(String key, long value) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的int值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.lrem(key, 1, String.valueOf(value)).thenApply(v -> v.intValue()));
        });
    }

    @Override
    public CompletableFuture<Boolean> sismemberLongAsync(String key, long value) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的boolean值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.sismember(key, String.valueOf(value)));
        });
    }

    @Override
    public CompletableFuture<Void> saddLongAsync(String key, long value) {
        return connectStringAsync().thenCompose(command -> {
            //不处理返回值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.sadd(key, String.valueOf(value)));
        });
    }

    @Override
    public CompletableFuture<Integer> sremLongAsync(String key, long value) {
        return connectStringAsync().thenCompose(command -> {
            //此处获取的int值，无需传cryptor进行解密
            return completableStringFuture(key, null, command, command.srem(key, String.valueOf(value)).thenApply(v -> v.intValue()));
        });
    }

}
