/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.*;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class RedisCacheRequest extends ClientRequest {

    static final byte[] BYTES_TRUE = new byte[]{'t'};

    static final byte[] BYTES_FALSE = new byte[]{'f'};

    static final byte[] BYTES_MATCH = "MATCH".getBytes(StandardCharsets.UTF_8);

    static final byte[] BYTES_COUNT = "COUNT".getBytes(StandardCharsets.UTF_8);

    protected static final byte[] CRLF = new byte[]{'\r', '\n'};

    private static final byte[][] starLengthBytes;

    private static final byte[][] dollarLengthBytes;

    static {
        starLengthBytes = new byte[1024][];
        dollarLengthBytes = new byte[1024][];
        for (int i = 0; i < dollarLengthBytes.length; i++) {
            starLengthBytes[i] = ("*" + i + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
            dollarLengthBytes[i] = ("$" + i + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
        }
    }

    protected RedisCommand command;

    protected String key;

    protected byte[][] args;

    public static RedisCacheRequest create(RedisCommand command, String key, String... args) {
        return new RedisCacheRequest().prepare(command, key, RedisCacheSource.keysArgs(key, args));
    }

    public static RedisCacheRequest create(RedisCommand command, String key, byte[]... args) {
        return new RedisCacheRequest().prepare(command, key, args);
    }

    public RedisCacheRequest prepare(RedisCommand command, String key, byte[]... args) {
        super.prepare();
        this.command = command;
        this.key = key;
        this.args = args;
        return this;
    }

    public RedisCacheRequest createTime() {
        this.createTime = System.currentTimeMillis();
        return this;
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray writer) {
        writer.put(mutliLengthBytes(args.length + 1));
        writer.put(command.getBytes());

        for (final byte[] arg : args) {
            putArgBytes(writer, arg);
        }
    }

    protected void putArgBytes(ByteArray writer, byte[] arg) {
        writer.put(bulkLengthBytes(arg.length));
        writer.put(arg);
        writer.put(CRLF);
    }

    protected static byte[] mutliLengthBytes(int length) {
        if (length >= 0 && length < starLengthBytes.length) {
            return starLengthBytes[length];
        } else {
            return ("*" + length + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
        }
    }

    protected static byte[] bulkLengthBytes(int length) {
        if (length >= 0 && length < dollarLengthBytes.length) {
            return dollarLengthBytes[length];
        } else {
            return ("$" + length + "\r\n").getBytes(StandardCharsets.ISO_8859_1);
        }
    }

    @Override
    public String toString() {
        if (args == null || args.length == 0) {
            return getClass().getSimpleName() + "{" + command + " " + key + "}";
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append(command);
            sb.append(" ").append(key);
            for (final byte[] arg : args) {
                sb.append(" ").append(arg == null ? null : new String(arg, StandardCharsets.UTF_8));
            }
            return sb.toString();
        }
    }
}
