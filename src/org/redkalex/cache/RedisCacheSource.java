/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import javax.annotation.Resource;
import org.redkale.convert.bson.*;
import org.redkale.net.*;
import org.redkale.service.*;
import org.redkale.source.CacheSource;
import org.redkale.util.*;
import org.redkale.util.AnyValue.DefaultAnyValue;

/**
 * 详情见: https://redkale.org
 *
 * @param <K> Key
 * @param <V> Value
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@ResourceType(CacheSource.class)
public class RedisCacheSource<K extends Serializable, V extends Object> extends AbstractService implements CacheSource<K, V>, Service, AutoCloseable, Resourcable {

    static final String UTF8_NAME = "UTF-8";

    static final Charset UTF8 = Charset.forName(UTF8_NAME);

    private static final byte DOLLAR_BYTE = '$';

    private static final byte ASTERISK_BYTE = '*';

    private static final byte PLUS_BYTE = '+';

    private static final byte MINUS_BYTE = '-';

    private static final byte COLON_BYTE = ':';

    @Resource
    private BsonConvert convert;

    private Transport transport;

    @Override
    public void init(AnyValue conf) {
        if (conf == null) conf = new AnyValue.DefaultAnyValue();
        final int bufferCapacity = conf.getIntValue("bufferCapacity", 8 * 1024);
        final int bufferPoolSize = conf.getIntValue("bufferPoolSize", Runtime.getRuntime().availableProcessors() * 8);
        final int threads = conf.getIntValue("threads", Runtime.getRuntime().availableProcessors() * 8);
        final ObjectPool<ByteBuffer> transportPool = new ObjectPool<>(new AtomicLong(), new AtomicLong(), bufferPoolSize,
            (Object... params) -> ByteBuffer.allocateDirect(bufferCapacity), null, (e) -> {
                if (e == null || e.isReadOnly() || e.capacity() != bufferCapacity) return false;
                e.clear();
                return true;
            });
        final List<InetSocketAddress> addresses = new ArrayList<>();
        for (AnyValue node : conf.getAnyValues("node")) {
            addresses.add(new InetSocketAddress(node.getValue("addr"), node.getIntValue("port")));
        }
        final AtomicInteger counter = new AtomicInteger();
        ExecutorService transportExec = Executors.newFixedThreadPool(threads, (Runnable r) -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("Transport-Thread-" + counter.incrementAndGet());
            return t;
        });
        AsynchronousChannelGroup transportGroup = null;
        try {
            transportGroup = AsynchronousChannelGroup.withCachedThreadPool(transportExec, 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.transport = new Transport("Redis-Transport", "TCP", "", transportPool, transportGroup, null, addresses, null);
    }

    public static void main(String[] args) throws Exception {
        DefaultAnyValue conf = new DefaultAnyValue();
        conf.addValue("node", new DefaultAnyValue().addValue("addr", "127.0.0.1").addValue("port", "6379"));

        RedisCacheSource source = new RedisCacheSource();
        source.init(conf);
        source.convert = BsonFactory.root().getConvert();

        System.out.println("------------------------------------");
        source.remove("key1");
        source.remove("key2");
        source.remove(300);
        source.set("key1", "value1");
        source.set(300, 4000);
        source.getAndRefresh("key1", 3500);
        System.out.println("[有值] 300 GET : " + source.get(300));
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
        source.getCollectionAndRefresh("keys3", 30);

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
    public CompletableFuture<Boolean> existsAsync(K key) {
        return (CompletableFuture) send("EXISTS", key, convert.convertTo(Serializable.class, key));
    }

    @Override
    public boolean exists(K key) {
        return existsAsync(key).join();
    }

    //--------------------- get ------------------------------
    @Override
    public CompletableFuture<V> getAsync(K key) {
        return (CompletableFuture) send("GET", key, convert.convertTo(Serializable.class, key));
    }

    @Override
    public V get(K key) {
        return getAsync(key).join();
    }

    //--------------------- getAndRefresh ------------------------------
    @Override
    public CompletableFuture<V> getAndRefreshAsync(K key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getAsync(key));
    }

    @Override
    public V getAndRefresh(K key, final int expireSeconds) {
        return getAndRefreshAsync(key, expireSeconds).join();
    }

    //--------------------- refresh ------------------------------
    @Override
    public CompletableFuture<Void> refreshAsync(K key, int expireSeconds) {
        return setExpireSecondsAsync(key, expireSeconds);
    }

    @Override
    public void refresh(K key, final int expireSeconds) {
        setExpireSeconds(key, expireSeconds);
    }

    //--------------------- set ------------------------------
    @Override
    public CompletableFuture<Void> setAsync(K key, V value) {
        return (CompletableFuture) send("SET", key, convert.convertTo(Serializable.class, key), convert.convertTo(Object.class, value));
    }

    @Override
    public void set(K key, V value) {
        setAsync(key, value).join();
    }

    //--------------------- set ------------------------------    
    @Override
    public CompletableFuture<Void> setAsync(int expireSeconds, K key, V value) {
        return (CompletableFuture) setAsync(key, value).thenCompose(v -> setExpireSecondsAsync(key, expireSeconds));
    }

    @Override
    public void set(int expireSeconds, K key, V value) {
        setAsync(expireSeconds, key, value).join();
    }

    //--------------------- setExpireSeconds ------------------------------    
    @Override
    public CompletableFuture<Void> setExpireSecondsAsync(K key, int expireSeconds) {
        return (CompletableFuture) send("EXPIRE", key, convert.convertTo(Serializable.class, key), String.valueOf(expireSeconds).getBytes(UTF8));
    }

    @Override
    public void setExpireSeconds(K key, int expireSeconds) {
        setExpireSecondsAsync(key, expireSeconds).join();
    }

    //--------------------- remove ------------------------------    
    @Override
    public CompletableFuture<Void> removeAsync(K key) {
        return (CompletableFuture) send("DEL", key, convert.convertTo(Serializable.class, key));
    }

    @Override
    public void remove(K key) {
        removeAsync(key).join();
    }

    //--------------------- collection ------------------------------  
    @Override
    public CompletableFuture<Integer> getCollectionSizeAsync(K key) {
        return (CompletableFuture) send("OBJECT", key, "ENCODING".getBytes(UTF8), convert.convertTo(Serializable.class, key)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LLEN", key, convert.convertTo(Serializable.class, key));
            } else {
                return send("SCARD", key, convert.convertTo(Serializable.class, key));
            }
        });
    }

    @Override
    public CompletableFuture<Collection<V>> getCollectionAsync(K key) {
        return (CompletableFuture) send("OBJECT", key, "ENCODING".getBytes(UTF8), convert.convertTo(Serializable.class, key)).thenCompose(t -> {
            if (t == null) return CompletableFuture.completedFuture(null);
            if (new String((byte[]) t).contains("list")) { //list
                return send("LRANGE", false, key, convert.convertTo(Serializable.class, key), new byte[]{'0'}, new byte[]{'-', '1'});
            } else {
                return send("SMEMBERS", true, key, convert.convertTo(Serializable.class, key));
            }
        });
    }

    @Override
    public Collection<V> getCollection(K key) {
        return getCollectionAsync(key).join();
    }

    @Override
    public int getCollectionSize(K key) {
        return getCollectionSizeAsync(key).join();
    }

    //--------------------- getCollectionAndRefresh ------------------------------  
    @Override
    public CompletableFuture<Collection<V>> getCollectionAndRefreshAsync(K key, int expireSeconds) {
        return (CompletableFuture) refreshAsync(key, expireSeconds).thenCompose(v -> getCollectionAsync(key));
    }

    @Override
    public Collection<V> getCollectionAndRefresh(K key, final int expireSeconds) {
        return getCollectionAndRefreshAsync(key, expireSeconds).join();
    }

    //--------------------- appendListItem ------------------------------  
    @Override
    public CompletableFuture<Void> appendListItemAsync(K key, V value) {
        return (CompletableFuture) send("RPUSH", key, convert.convertTo(Serializable.class, key), convert.convertTo(Object.class, value));
    }

    @Override
    public void appendListItem(K key, V value) {
        appendListItemAsync(key, value).join();
    }

    //--------------------- removeListItem ------------------------------  
    @Override
    public CompletableFuture<Void> removeListItemAsync(K key, V value) {
        return (CompletableFuture) send("LREM", key, convert.convertTo(Serializable.class, key), new byte[]{'0'}, convert.convertTo(Object.class, value));
    }

    @Override
    public void removeListItem(K key, V value) {
        removeListItemAsync(key, value).join();
    }

    //--------------------- appendSetItem ------------------------------  
    @Override
    public CompletableFuture<Void> appendSetItemAsync(K key, V value) {
        return (CompletableFuture) send("SADD", key, convert.convertTo(Serializable.class, key), convert.convertTo(Object.class, value));
    }

    @Override
    public void appendSetItem(K key, V value) {
        appendSetItemAsync(key, value).join();
    }

    //--------------------- removeSetItem ------------------------------  
    @Override
    public CompletableFuture<Void> removeSetItemAsync(K key, V value) {
        return (CompletableFuture) send("SREM", key, convert.convertTo(Serializable.class, key), convert.convertTo(Object.class, value));
    }

    @Override
    public void removeSetItem(K key, V value) {
        removeSetItemAsync(key, value).join();
    }

    //--------------------- queryKeys ------------------------------  
    @Override
    public List<K> queryKeys() {
        return queryKeysAsync().join();
    }

    @Override
    public CompletableFuture<List<K>> queryKeysAsync() {
        return (CompletableFuture) send("KEYS", "*", new byte[]{(byte) '*'});
    }

    //--------------------- queryList ------------------------------  
    @Override
    public List<CacheEntry<K, Object>> queryList() {
        return queryListAsync().join();
    }

    @Override
    public CompletableFuture<List<CacheEntry<K, Object>>> queryListAsync() {
        //待实现
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    //--------------------- send ------------------------------  
    private CompletableFuture<Serializable> send(final String command, final Serializable key, final byte[]... args) {
        return send(command, false, key, args);
    }

    private CompletableFuture<Serializable> send(final String command, final boolean set, final Serializable key, final byte[]... args) {
        return send(null, command, set, key, args);
    }

    private CompletableFuture<Serializable> send(final CompletionHandler callback, final String command, final Serializable key, final byte[]... args) {
        return send(callback, command, false, key, args);
    }

    private CompletableFuture<Serializable> send(final CompletionHandler callback, final String command, final boolean set, final Serializable key, final byte[]... args) {
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
                                        callback.completed("EXISTS".equals(command) ? (rs > 0) : (("LLEN".equals(command) || "SCARD".equals(command)) ? (int) rs : null), key);
                                    } else {
                                        future.complete("EXISTS".equals(command) ? (rs > 0) : (("LLEN".equals(command) || "SCARD".equals(command)) ? (int) rs : null));
                                    }
                                } else if (sign == DOLLAR_BYTE) { // $
                                    long val = readLong();
                                    byte[] rs = val <= 0 ? null : readBytes();
                                    if (future == null) {
                                        callback.completed("GET".equals(command) ? convert.convertFrom(Object.class, rs) : null, key);
                                    } else {
                                        future.complete("GET".equals(command) ? convert.convertFrom(Object.class, rs) : rs);
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
                                        for (int i = 0; i < len; i++) {
                                            if (readInt() > 0) rs.add(convert.convertFrom(Serializable.class, readBytes()));
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

    @Override
    public int getKeySize() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public CompletableFuture<Integer> getKeySizeAsync() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
