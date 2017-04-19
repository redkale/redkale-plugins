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
import org.redkale.watch.WatchFactory;

/**
 * 详情见: https://redkale.org
 *
 * @param <K>
 * @param <V>
 *
 * @author zhangjx
 */
@LocalService
@AutoLoad(false)
@ResourceType({CacheSource.class})
public class RedisCacheSource<K extends Serializable, V extends Object> implements CacheSource<K, V>, Service, AutoCloseable, Resourcable {

    static final String UTF8_NAME = "UTF-8";

    static final Charset UTF8 = Charset.forName(UTF8_NAME);

    private static final byte DOLLAR_BYTE = '$';

    private static final byte ASTERISK_BYTE = '*';

    private static final byte PLUS_BYTE = '+';

    private static final byte MINUS_BYTE = '-';

    private static final byte COLON_BYTE = ':';

    @Resource
    private BsonConvert convert;

    private WatchFactory watchFactory;

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
        this.transport = new Transport("Redis-Transport", "TCP", null, "", transportPool, transportGroup, null, addresses);
    }

    /*
     * public static void main(String[] args) throws Exception {
     * DefaultAnyValue conf = new DefaultAnyValue();
     * conf.addValue("node", new DefaultAnyValue().addValue("addr", "10.28.10.11").addValue("port", "5050"));
     *
     * RedisCacheSource<String, String> source = new RedisCacheSource();
     * source.init(conf);
     * source.convert = BsonFactory.root().getConvert();
     * source.setAsync("key1", "value1");
     * source.getAndRefreshAsync("key1", 3500);
     * System.out.println("key1 GET : " + source.getAsync("key1"));
     * System.out.println("key2 GET : " + source.getAsync("key2"));
     * System.out.println("key1 EXISTS : " + source.existsAsync("key1"));
     * System.out.println("key2 EXISTS : " + source.existsAsync("key2"));
     * source.removeAsync("keys3");
     * source.appendListItemAsync("keys3", "vals1");
     * source.appendListItemAsync("keys3", "vals2");
     * System.out.println("keys3 VALUES : " + source.getCollectionAsync("keys3"));
     * System.out.println("keys3 EXISTS : " + source.existsAsync("keys3"));
     * source.removeListItemAsync("keys3", "vals1");
     * System.out.println("keys3 VALUES : " + source.getCollectionAsync("keys3"));
     * source.getCollectionAndRefreshAsync("keys3", 30);
     *
     * source.removeAsync("sets3");
     * source.appendSetItemAsync("sets3", "setvals1");
     * source.appendSetItemAsync("sets3", "setvals2");
     * System.out.println("sets3 VALUES : " + source.getCollectionAsync("sets3"));
     * System.out.println("sets3 EXISTS : " + source.existsAsync("sets3"));
     * source.removeSetItemAsync("sets3", "setvals1");
     * System.out.println("sets3 VALUES : " + source.getCollectionAsync("sets3"));
     *
     * System.out.println("---------------------------------");
     * source.existsAsync(new CompletionHandler<Boolean, String>() {
     * @Override
     * public void completed(Boolean result, String attachment) {
     * System.out.println("key1 EXISTS : " + result);
     * }
     *
     * @Override
     * public void failed(Throwable exc, String attachment) {
     * exc.printStackTrace();
     * }
     * }, "key1");
     * Thread.sleep(500L);
     * //
     * source.setAsync(new CompletionHandler<Void, String>() {
     * @Override
     * public void completed(Void result, String attachment) {
     * System.out.println("key2 GET : " + source.getAsync("key2"));
     * }
     *
     * @Override
     * public void failed(Throwable exc, String attachment) {
     * exc.printStackTrace();
     * }
     * }, "key2", "keyvalue2");
     * Thread.sleep(500L);
     * }
     */
    private SimpleFuture<byte[]> send(final String command, final K key, final byte[]... args) {
        return send(command, false, key, args);
    }

    private SimpleFuture<byte[]> send(final String command, final boolean set, final K key, final byte[]... args) {
        return send(null, command, set, key, args);
    }

    private SimpleFuture<byte[]> send(final CompletionHandler callback, final String command, final K key, final byte[]... args) {
        return send(callback, command, false, key, args);
    }

    private SimpleFuture<byte[]> send(final CompletionHandler callback, final String command, final boolean set, final K key, final byte[]... args) {
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
            final SimpleFuture<byte[]> future = callback == null ? new SimpleFuture<>() : null;
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
                                        future.set(bs);
                                    }
                                } else if (sign == MINUS_BYTE) { // -
                                    String bs = readString();
                                    if (future == null) {
                                        callback.failed(new RuntimeException(bs), key);
                                    } else {
                                        future.set(new RuntimeException(bs));
                                    }
                                } else if (sign == COLON_BYTE) { // :
                                    long rs = readLong();
                                    if (future == null) {
                                        callback.completed("EXISTS".equals(command) ? (rs > 0) : null, key);
                                    } else {
                                        future.set(rs);
                                    }
                                } else if (sign == DOLLAR_BYTE) { // $
                                    long val = readLong();
                                    byte[] rs = val <= 0 ? null : readBytes();
                                    if (future == null) {
                                        callback.completed("GET".equals(command) ? convert.convertFrom(Object.class, rs) : null, key);
                                    } else {
                                        future.set(rs);
                                    }
                                } else if (sign == ASTERISK_BYTE) { // *
                                    final int len = readInt();
                                    if (len < 0) {
                                        if (future == null) {
                                            callback.completed(null, key);
                                        } else {
                                            future.set((byte[]) null);
                                        }
                                    } else {
                                        Collection rs = set ? new HashSet() : new ArrayList();
                                        for (int i = 0; i < len; i++) {
                                            if (readInt() > 0) rs.add(convert.convertFrom(Object.class, readBytes()));
                                        }
                                        if (future == null) {
                                            callback.completed(rs, key);
                                        } else {
                                            future.set(rs);
                                        }
                                    }
                                } else {
                                    String exstr = "Unknown reply: " + (char) sign;
                                    if (future == null) {
                                        callback.failed(new RuntimeException(exstr), key);
                                    } else {
                                        future.set(new RuntimeException(exstr));
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
                                future.set(exc instanceof RuntimeException ? (RuntimeException) exc : new RuntimeException(exc));
                            }
                        }

                    });
                }

                @Override
                public void failed(Throwable exc, ByteBuffer[] attachments) {
                    if (future == null) {
                        callback.failed(exc, attachments);
                    } else {
                        future.set(exc instanceof RuntimeException ? (RuntimeException) exc : new RuntimeException(exc));
                    }
                }
            });
            if (future != null) {
                future.get(10, TimeUnit.SECONDS);
                this.transport.offerConnection(false, conn);
            }
            return future;
        } catch (Exception e) {
            transport.offerBuffer(buffers);
            if (conn0 != null) this.transport.offerConnection(true, conn0);
            throw new RuntimeException(e);
        }
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

    @Override
    public boolean exists(K key) {
        SimpleFuture<byte[]> future = send("EXISTS", key, convert.convertTo(key));
        try {
            return future.getNumberResult() > 0;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void existsAsync(final AsyncHandler<Boolean, K> handler, @RpcAttachment final K key) {
        send(handler, "EXISTS", key, convert.convertTo(key));
    }

    @Override
    public V get(K key) {
        SimpleFuture<byte[]> future = send("GET", key, convert.convertTo(key));
        try {
            return (V) convert.convertFrom(Object.class, future.get());
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getAsync(final AsyncHandler<V, K> handler, @RpcAttachment final K key) {
        send(handler, "GET", key, convert.convertTo(key));
    }

    @Override
    public V getAndRefresh(K key, final int expireSeconds) {
        refresh(key, expireSeconds);
        return get(key);
    }

    @Override
    public void getAndRefreshAsync(final AsyncHandler<V, K> handler, @RpcAttachment final K key, final int expireSeconds) {
        refreshAsync(new AsyncHandler<Void, K>() {
            @Override
            public void completed(Void result, K attachment) {
                getAsync(handler, key);
            }

            @Override
            public void failed(Throwable exc, K attachment) {
                if (handler != null) handler.failed(exc, key);
            }

        }, key, expireSeconds);
    }

    @Override
    public void refresh(K key, final int expireSeconds) {
        setExpireSeconds(key, expireSeconds);
    }

    @Override
    public void refreshAsync(final AsyncHandler<Void, K> handler, @RpcAttachment final K key, final int expireSeconds) {
        setExpireSecondsAsync(handler, key, expireSeconds);
    }

    @Override
    public void set(K key, V value) {
        send("SET", key, convert.convertTo(key), convert.convertTo(Object.class, value)).waitDone();
    }

    @Override
    public void setAsync(final AsyncHandler<Void, K> handler, @RpcAttachment final K key, final V value) {
        send(handler, "SET", key, convert.convertTo(key), convert.convertTo(Object.class, value));
    }

    @Override
    public void set(int expireSeconds, K key, V value) {
        set(key, value);
        setExpireSeconds(key, expireSeconds);
    }

    @Override
    public void setAsync(final AsyncHandler<Void, K> handler, final int expireSeconds, @RpcAttachment final K key, final V value) {
        setAsync(new AsyncHandler<Void, K>() {
            @Override
            public void completed(Void result, K attachment) {
                setExpireSecondsAsync(handler, key, expireSeconds);
            }

            @Override
            public void failed(Throwable exc, K attachment) {
                if (handler != null) handler.failed(exc, key);
            }

        }, key, value);
    }

    @Override
    public void setExpireSeconds(K key, int expireSeconds) {
        send("EXPIRE", key, convert.convertTo(key), String.valueOf(expireSeconds).getBytes(UTF8)).waitDone();
    }

    @Override
    public void setExpireSecondsAsync(final AsyncHandler<Void, K> handler, @RpcAttachment final K key, final int expireSeconds) {
        send(handler, "EXPIRE", key, convert.convertTo(key), String.valueOf(expireSeconds).getBytes(UTF8));
    }

    @Override
    public void remove(K key) {
        send("DEL", key, convert.convertTo(key)).waitDone();
    }

    @Override
    public void removeAsync(final AsyncHandler<Void, K> handler, @RpcAttachment final K key) {
        send(handler, "DEL", key, convert.convertTo(key));
    }

    @Override
    public Collection<V> getCollection(K key) {
        SimpleFuture<byte[]> future = send("OBJECT", key, "ENCODING".getBytes(UTF8), convert.convertTo(key));
        try {
            final byte[] typeDesc = future.get();
            if (typeDesc == null) return null;
            if (new String(typeDesc).contains("list")) { //list
                future = send("LRANGE", false, key, convert.convertTo(key), new byte[]{'0'}, new byte[]{'-', '1'});
            } else {
                future = send("SMEMBERS", true, key, convert.convertTo(key));
            }
            return future.getCollectionResult();
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void getCollectionAsync(final AsyncHandler<Collection<V>, K> handler, @RpcAttachment final K key) {
        send(new AsyncHandler<byte[], K>() {
            @Override
            public void completed(byte[] result, K attachment) {
                if (result == null) {
                    if (handler != null) handler.completed(null, attachment);
                } else if (new String(result).contains("list")) { //list
                    send(handler, "LRANGE", false, key, convert.convertTo(key), new byte[]{'0'}, new byte[]{'-', '1'});
                } else {
                    send(handler, "SMEMBERS", true, key, convert.convertTo(key));
                }
            }

            @Override
            public void failed(Throwable exc, K attachment) {
                if (handler != null) handler.failed(exc, attachment);
            }
        }, "OBJECT", key, "ENCODING".getBytes(UTF8), convert.convertTo(key));
    }

    @Override
    public Collection<V> getCollectionAndRefresh(K key, final int expireSeconds) {
        refresh(key, expireSeconds);
        return getCollection(key);
    }

    @Override
    public void getCollectionAndRefreshAsync(final AsyncHandler<Collection<V>, K> handler, @RpcAttachment final K key, final int expireSeconds) {
        refreshAsync(new AsyncHandler<Void, K>() {
            @Override
            public void completed(Void result, K attachment) {
                getCollectionAsync(handler, key);
            }

            @Override
            public void failed(Throwable exc, K attachment) {
                if (handler != null) handler.failed(exc, attachment);
            }
        }, key, expireSeconds);
    }

    @Override
    public void appendListItem(K key, V value) {
        send("RPUSH", key, convert.convertTo(key), convert.convertTo(Object.class, value)).waitDone();
    }

    @Override
    public void appendListItemAsync(final AsyncHandler<Void, K> handler, @RpcAttachment final K key, final V value) {
        send(handler, "RPUSH", key, convert.convertTo(key), convert.convertTo(Object.class, value));
    }

    @Override
    public void removeListItem(K key, V value) {
        send("LREM", key, convert.convertTo(key), new byte[]{'0'}, convert.convertTo(Object.class, value));
    }

    @Override
    public void removeListItemAsync(final AsyncHandler<Void, K> handler, @RpcAttachment final K key, final V value) {
        send(handler, "LREM", key, convert.convertTo(key), new byte[]{'0'}, convert.convertTo(Object.class, value));
    }

    @Override
    public void appendSetItem(K key, V value) {
        send("SADD", key, convert.convertTo(key), convert.convertTo(Object.class, value));
    }

    @Override
    public void appendSetItemAsync(final AsyncHandler<Void, K> handler, @RpcAttachment final K key, final V value) {
        send(handler, "SADD", key, convert.convertTo(key), convert.convertTo(Object.class, value));
    }

    @Override
    public void removeSetItem(K key, V value) {
        send("SREM", key, convert.convertTo(key), convert.convertTo(Object.class, value));
    }

    @Override
    public void removeSetItemAsync(final AsyncHandler<Void, K> handler, @RpcAttachment final K key, final V value) {
        send(handler, "SREM", key, convert.convertTo(key), convert.convertTo(Object.class, value));
    }
}

final class SimpleFuture<T> implements Future<T> {

    private volatile boolean done;

    private long numberResult = -1;

    private Collection collectionResult;

    private T result;

    private RuntimeException ex;

    public SimpleFuture() {
    }

    public SimpleFuture(T result) {
        this.result = result;
        this.done = true;
    }

    public void set(T result) {
        this.result = result;
        this.done = true;
        synchronized (this) {
            notifyAll();
        }
    }

    public void set(Collection val) {
        this.collectionResult = val;
        this.done = true;
        synchronized (this) {
            notifyAll();
        }
    }

    public void set(int val) {
        this.numberResult = val;
        this.done = true;
        synchronized (this) {
            notifyAll();
        }
    }

    public void set(long val) {
        this.numberResult = val;
        this.done = true;
        synchronized (this) {
            notifyAll();
        }
    }

    public void set(RuntimeException ex) {
        this.ex = ex;
        this.done = true;
        synchronized (this) {
            notifyAll();
        }
    }

    public void waitDone() {
        try {
            get();
        } catch (RuntimeException rex) {
            throw rex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return done;
    }

    public Collection getCollectionResult() throws InterruptedException, ExecutionException {
        if (done) {
            if (ex != null) throw ex;
            return collectionResult;
        }
        synchronized (this) {
            if (!done) wait(10_000);
        }
        if (done) {
            if (ex != null) throw ex;
            return collectionResult;
        }
        throw new InterruptedException();
    }

    public long getNumberResult() throws InterruptedException, ExecutionException {
        if (done) {
            if (ex != null) throw ex;
            return numberResult;
        }
        synchronized (this) {
            if (!done) wait(10_000);
        }
        if (done) {
            if (ex != null) throw ex;
            return numberResult;
        }
        throw new InterruptedException();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        if (done) {
            if (ex != null) throw ex;
            return result;
        }
        synchronized (this) {
            if (!done) wait(10_000);
        }
        if (done) {
            if (ex != null) throw ex;
            return result;
        }
        throw new InterruptedException();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (done) {
            if (ex != null) throw ex;
            return result;
        }
        synchronized (this) {
            if (!done) wait(unit.toMillis(timeout));
        }
        if (done) {
            if (ex != null) throw ex;
            return result;
        }
        throw new TimeoutException();
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
