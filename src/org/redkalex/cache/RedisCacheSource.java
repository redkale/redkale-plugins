/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache;

import java.io.*;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
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

    static final String UTF8_NAME = "UTF-8";

    static final Charset UTF8 = Charset.forName(UTF8_NAME);

    private static final byte DOLLAR_BYTE = '$';

    private static final byte ASTERISK_BYTE = '*';

    private static final byte PLUS_BYTE = '+';

    private static final byte MINUS_BYTE = '-';

    private static final byte COLON_BYTE = ':';

    @Resource
    private JsonConvert defaultConvert;

    @Resource(name = "$_convert")
    private JsonConvert convert;

    private Type objValueType = String.class;

    private Transport transport;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = this.defaultConvert;
        if (conf == null) conf = new AnyValue.DefaultAnyValue();
        final int bufferCapacity = conf.getIntValue("bufferCapacity", 8 * 1024);
        final int bufferPoolSize = conf.getIntValue("bufferPoolSize", Runtime.getRuntime().availableProcessors() * 8);
        final int threads = conf.getIntValue("threads", Runtime.getRuntime().availableProcessors() * 8);
        final List<InetSocketAddress> addresses = new ArrayList<>();
        for (AnyValue node : conf.getAnyValues("node")) {
            addresses.add(new InetSocketAddress(node.getValue("addr"), node.getIntValue("port")));
        }
        TransportFactory transportFactory = TransportFactory.create(threads, bufferPoolSize, bufferCapacity);
        this.transport = transportFactory.createTransportTCP("Redis-Transport", null, addresses);
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

        System.out.println("------------------------------------");
        source.remove("key1");
        source.remove("key2");
        source.remove("300");
        source.set("key1", "value1");
        source.set("300", "4000");
        source.getAndRefresh("key1", 3500);
        System.out.println("[有值] 300 GET : " + source.get("300"));
        System.out.println("[有值] key1 GET : " + source.get("key1"));
        System.out.println("[无值] key2 GET : " + source.get("key2"));
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
        source.appendSetItem("sets3", "setvals1");
        source.appendSetItem("sets3", "setvals2");
        source.appendSetItem("sets3", "setvals1");
        System.out.println("[两值] sets3 VALUES : " + source.getCollection("sets3"));
        System.out.println("[有值] sets3 EXISTS : " + source.exists("sets3"));
        source.removeSetItem("sets3", "setvals1");
        System.out.println("[一值] sets3 VALUES : " + source.getCollection("sets3"));
        System.out.println("sets3 大小 : " + source.getCollectionSize("sets3"));
        System.out.println("all keys: " + source.queryKeys());
        System.out.println("newnum 值 : " + source.incr("newnum"));
        System.out.println("newnum 值 : " + source.decr("newnum"));
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
        return (CompletableFuture) send("EXISTS", key, key.getBytes(UTF8));
    }

    @Override
    public boolean exists(String key) {
        return existsAsync(key).join();
    }

    //--------------------- get ------------------------------
    @Override
    public CompletableFuture<V> getAsync(String key) {
        return (CompletableFuture) send("GET", key, key.getBytes(UTF8));
    }

    @Override
    public V get(String key) {
        return getAsync(key).join();
    }

    //--------------------- getAndRefresh ------------------------------
    @Override
    public CompletableFuture<V> getAndRefreshAsync(String key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getAsync(key));
    }

    @Override
    public V getAndRefresh(String key, final int expireSeconds) {
        return getAndRefreshAsync(key, expireSeconds).join();
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
        return (CompletableFuture) send("SET", key, key.getBytes(UTF8), formatValue(value));
    }

    @Override
    public void set(String key, V value) {
        setAsync(key, value).join();
    }

    //--------------------- set ------------------------------    
    @Override
    public CompletableFuture<Void> setAsync(int expireSeconds, String key, V value) {
        return (CompletableFuture) setAsync(key, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public void set(int expireSeconds, String key, V value) {
        setAsync(expireSeconds, key, value).join();
    }

    //--------------------- setExpireSeconds ------------------------------    
    @Override
    public CompletableFuture<Void> setExpireSecondsAsync(String key, int expireSeconds) {
        return (CompletableFuture) send("EXPIRE", key, key.getBytes(UTF8), String.valueOf(expireSeconds).getBytes(UTF8));
    }

    @Override
    public void setExpireSeconds(String key, int expireSeconds) {
        setExpireSecondsAsync(key, expireSeconds).join();
    }

    //--------------------- remove ------------------------------    
    @Override
    public CompletableFuture<Void> removeAsync(String key) {
        return (CompletableFuture) send("DEL", key, key.getBytes(UTF8));
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
        return (CompletableFuture) send("INCR", key, key.getBytes(UTF8));
    }

    @Override
    public long incr(final String key, long num) {
        return incrAsync(key, num).join();
    }

    @Override
    public CompletableFuture<Long> incrAsync(final String key, long num) {
        return (CompletableFuture) send("INCRBY", key, key.getBytes(UTF8), String.valueOf(num).getBytes(UTF8));
    }

    //--------------------- decr ------------------------------    
    @Override
    public long decr(final String key) {
        return decrAsync(key).join();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key) {
        return (CompletableFuture) send("DECR", key, key.getBytes(UTF8));
    }

    @Override
    public long decr(final String key, long num) {
        return decrAsync(key, num).join();
    }

    @Override
    public CompletableFuture<Long> decrAsync(final String key, long num) {
        return (CompletableFuture) send("DECRBY", key, key.getBytes(UTF8), String.valueOf(num).getBytes(UTF8));
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Integer> getCollectionSizeAsync(String key) {
        return (CompletableFuture) send("OBJECT", key, "ENCODING".getBytes(UTF8), key.getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LLEN", key, key.getBytes(UTF8));
            } else {
                return send("SCARD", key, key.getBytes(UTF8));
            }
        });
    }

    @Override
    public CompletableFuture<Collection<V>> getCollectionAsync(String key) {
        return (CompletableFuture) send("OBJECT", key, "ENCODING".getBytes(UTF8), key.getBytes(UTF8)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LRANGE", false, key, key.getBytes(UTF8), new byte[]{'0'}, new byte[]{'-', '1'});
            } else {
                return send("SMEMBERS", true, key, key.getBytes(UTF8));
            }
        });
    }

    @Override
    public Collection<V> getCollection(String key) {
        return getCollectionAsync(key).join();
    }

    @Override
    public int getCollectionSize(String key) {
        return getCollectionSizeAsync(key).join();
    }

    //--------------------- getCollectionAndRefresh ------------------------------  
    @Override
    public CompletableFuture<Collection<V>> getCollectionAndRefreshAsync(String key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key));
    }

    @Override
    public Collection<V> getCollectionAndRefresh(String key, final int expireSeconds) {
        return getCollectionAndRefreshAsync(key, expireSeconds).join();
    }

    //--------------------- appendListItem ------------------------------  
    @Override
    public CompletableFuture<Void> appendListItemAsync(String key, V value) {
        return (CompletableFuture) send("RPUSH", key, key.getBytes(UTF8), formatValue(value));
    }

    @Override
    public void appendListItem(String key, V value) {
        appendListItemAsync(key, value).join();
    }

    //--------------------- removeListItem ------------------------------  
    @Override
    public CompletableFuture<Void> removeListItemAsync(String key, V value) {
        return (CompletableFuture) send("LREM", key, key.getBytes(UTF8), new byte[]{'0'}, formatValue(value));
    }

    @Override
    public void removeListItem(String key, V value) {
        removeListItemAsync(key, value).join();
    }

    //--------------------- appendSetItem ------------------------------  
    @Override
    public CompletableFuture<Void> appendSetItemAsync(String key, V value) {
        return (CompletableFuture) send("SADD", key, key.getBytes(UTF8), formatValue(value));
    }

    @Override
    public void appendSetItem(String key, V value) {
        appendSetItemAsync(key, value).join();
    }

    //--------------------- removeSetItem ------------------------------  
    @Override
    public CompletableFuture<Void> removeSetItemAsync(String key, V value) {
        return (CompletableFuture) send("SREM", key, key.getBytes(UTF8), formatValue(value));
    }

    @Override
    public void removeSetItem(String key, V value) {
        removeSetItemAsync(key, value).join();
    }

    //--------------------- queryKeys ------------------------------  
    @Override
    public List<String> queryKeys() {
        return queryKeysAsync().join();
    }

    @Override
    public CompletableFuture<List<String>> queryKeysAsync() {
        return (CompletableFuture) send("KEYS", "*", new byte[]{(byte) '*'});
    }

    //--------------------- getKeySize ------------------------------  
    @Override
    public int getKeySize() {
        return getKeySizeAsync().join();
    }

    @Override
    public CompletableFuture<Integer> getKeySizeAsync() {
        return (CompletableFuture) send("DBSIZE", null);
    }

    //--------------------- queryList ------------------------------  
    @Override
    public List<CacheEntry<Object>> queryList() {
        return queryListAsync().join();
    }

    @Override
    public CompletableFuture<List<CacheEntry<Object>>> queryListAsync() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    //--------------------- send ------------------------------  
    private byte[] formatValue(V value) {
        return convert.convertTo(objValueType, value).getBytes(UTF8);
    }

    private CompletableFuture<Serializable> send(final String command, final String key, final byte[]... args) {
        return send(command, false, key, args);
    }

    private CompletableFuture<Serializable> send(final String command, final boolean set, final String key, final byte[]... args) {
        return send(null, command, set, key, args);
    }

    private CompletableFuture<Serializable> send(final CompletionHandler callback, final String command, final String key, final byte[]... args) {
        return send(callback, command, false, key, args);
    }

    private CompletableFuture<Serializable> send(final CompletionHandler callback, final String command, final boolean set, final String key, final byte[]... args) {
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
        AsyncConnection conn0 = null;
        try {
            final CompletableFuture<Serializable> future = callback == null ? new CompletableFuture<>() : null;
            conn0 = this.transport.pollConnection(null);
            final AsyncConnection conn = conn0;
            conn.write(buffers, buffers, new CompletionHandler<Integer, ByteBuffer[]>() {
                @Override
                public void completed(Integer result, ByteBuffer[] attachments) {
                    int index = -1;
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
                    ByteBuffer buffer0 = transport.pollBuffer();
                    conn.read(buffer0, null, new ReplyCompletionHandler< Void>(conn, buffer0) {
                        @Override
                        public void completed(Integer result, Void attachment) {
                            buffer.flip();
                            final byte sign = buffer.get();
                            try {
                                if (sign == PLUS_BYTE) { // +
                                    byte[] bs = readBytes();
                                    if (future == null) {
                                        callback.completed(null, key);
                                    } else {
                                        future.complete(bs);
                                    }
                                } else if (sign == MINUS_BYTE) { // -
                                    String bs = readString();
                                    if (future == null) {
                                        callback.failed(new RuntimeException(bs), key);
                                    } else {
                                        future.completeExceptionally(new RuntimeException("command : " + command + ", error: " + bs));
                                    }
                                } else if (sign == COLON_BYTE) { // :
                                    long rs = readLong();
                                    if (future == null) {
                                        if (command.startsWith("INCR") || command.startsWith("DECR")) {
                                            callback.completed(rs, key);
                                        } else {
                                            callback.completed("EXISTS".equals(command) ? (rs > 0) : (("LLEN".equals(command) || "SCARD".equals(command) || "DBSIZE".equals(command)) ? (int) rs : null), key);
                                        }
                                    } else {
                                        if (command.startsWith("INCR") || command.startsWith("DECR")) {
                                            future.complete(rs);
                                        } else {
                                            future.complete("EXISTS".equals(command) ? (rs > 0) : (("LLEN".equals(command) || "SCARD".equals(command) || "DBSIZE".equals(command)) ? (int) rs : null));
                                        }
                                    }
                                } else if (sign == DOLLAR_BYTE) { // $
                                    long val = readLong();
                                    byte[] rs = val <= 0 ? null : readBytes();
                                    if (future == null) {
                                        callback.completed(("GET".equals(command) || rs == null) ? convert.convertFrom(objValueType, new String(rs, UTF8)) : null, key);
                                    } else {
                                        future.complete("GET".equals(command) ? convert.convertFrom(objValueType, rs == null ? null : new String(rs, UTF8)) : rs);
                                    }
                                } else if (sign == ASTERISK_BYTE) { // *
                                    final int len = readInt();
                                    if (len < 0) {
                                        if (future == null) {
                                            callback.completed(null, key);
                                        } else {
                                            future.complete((byte[]) null);
                                        }
                                    } else {
                                        Collection rs = set ? new HashSet() : new ArrayList();
                                        boolean keys = "KEYS".equals(command);
                                        for (int i = 0; i < len; i++) {
                                            if (readInt() > 0) rs.add(keys ? new String(readBytes(), UTF8) : convert.convertFrom(objValueType, new String(readBytes(), UTF8)));
                                        }
                                        if (future == null) {
                                            callback.completed(rs, key);
                                        } else {
                                            future.complete((Serializable) rs);
                                        }
                                    }
                                } else {
                                    String exstr = "Unknown reply: " + (char) sign;
                                    if (future == null) {
                                        callback.failed(new RuntimeException(exstr), key);
                                    } else {
                                        future.completeExceptionally(new RuntimeException(exstr));
                                    }
                                }
                                transport.offerConnection(false, conn);
                            } catch (IOException e) {
                                transport.offerConnection(true, conn);
                                throw new RuntimeException(e);
                            }
                        }

                        @Override
                        public void failed(Throwable exc, Void attachment) {
                            if (future == null) {
                                callback.failed(exc, attachments);
                            } else {
                                future.completeExceptionally(exc);
                            }
                        }

                    });
                }

                @Override
                public void failed(Throwable exc, ByteBuffer[] attachments) {
                    transport.offerConnection(false, conn);
                    if (future == null) {
                        callback.failed(exc, attachments);
                    } else {
                        future.completeExceptionally(exc);
                    }
                }
            });
            return future;
        } catch (Exception e) {
            transport.offerBuffer(buffers);
            if (conn0 != null) this.transport.offerConnection(true, conn0);
            throw new RuntimeException(e);
        }
    }

}

abstract class ReplyCompletionHandler<T> implements CompletionHandler<Integer, T> {

    protected final ByteArray out = new ByteArray();

    protected final ByteBuffer buffer;

    protected final AsyncConnection conn;

    public ReplyCompletionHandler(AsyncConnection conn, ByteBuffer buffer) {
        this.conn = conn;
        this.buffer = buffer;
    }

    protected byte[] readBytes() throws IOException {
        readLine();
        return out.getBytesAndClear();
    }

    protected String readString() throws IOException {
        readLine();
        return out.toStringAndClear(null);//传null则表示使用UTF8 
    }

    protected int readInt() throws IOException {
        return (int) readLong();
    }

    protected long readLong() throws IOException {
        readLine();
        int start = 0;
        if (out.get(0) == '$') start = 1;
        boolean negative = out.get(start) == '-';
        long value = negative ? 0 : (out.get(start) - '0');
        for (int i = 1 + start; i < out.size(); i++) {
            value *= 10 + out.get(i) - '0';
        }
        out.clear();
        return negative ? -value : value;
    }

    private void readLine() throws IOException {
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
        try {
            conn.read(buffer).get();
            buffer.flip();
        } catch (Exception e) {
            throw new IOException(e);
        }
        readLine();
    }

}
