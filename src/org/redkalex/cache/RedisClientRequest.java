/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.redkale.net.client.*;
import org.redkale.source.CacheSource.CacheEntryType;
import org.redkale.util.ByteArray;
import static org.redkalex.cache.RedisCacheSource.*;

/**
 *
 * @author zhangjx
 */
public class RedisClientRequest implements ClientRequest {

    protected static final byte[] CRLF = new byte[]{'\r', '\n'};

    protected static final Map<Integer, byte[]> ASTERISK_LENGTH_MAP = new HashMap<>();

    protected static final Map<Integer, byte[]> DOLLAR_LENGTH_MAP = new HashMap<>();

    static {
        ByteArray array = new ByteArray();
        for (int i = 0; i < 1024; i++) {
            array.clear();
            array.put(ASTERISK_BYTE);
            array.put(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            array.put(CRLF);
            ASTERISK_LENGTH_MAP.put(i, array.getBytes());

            array.clear();
            array.put(DOLLAR_BYTE);
            array.put(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            array.put(CRLF);
            DOLLAR_LENGTH_MAP.put(i, array.getBytes());
        }
    }

    protected final String command;

    protected final CacheEntryType cacheType;

    protected final Type resultType;

    protected final boolean set;

    protected final String key;

    protected final byte[][] args;

    public RedisClientRequest(String command, CacheEntryType cacheType, Type resultType, boolean set, String key, byte[]... args) {
        this.command = command;
        this.cacheType = cacheType;
        this.resultType = resultType;
        this.set = set;
        this.key = key;
        this.args = args;
    }

    @Override
    public void accept(ClientConnection t, ByteArray writer) {
        byte[] abs = ASTERISK_LENGTH_MAP.get(args.length + 1);
        if (abs == null) {
            writer.put(ASTERISK_BYTE);
            writer.put(String.valueOf(args.length + 1).getBytes(StandardCharsets.UTF_8));
            writer.put(CRLF);
        } else {
            writer.put(abs);
        }
        byte[] dbs = DOLLAR_LENGTH_MAP.get(command.length());
        if (dbs == null) {
            writer.put(DOLLAR_BYTE);
            writer.put(String.valueOf(command.length()).getBytes(StandardCharsets.UTF_8));
            writer.put(CRLF);
        } else {
            writer.put(dbs);
        }
        writer.put(command.getBytes(StandardCharsets.UTF_8));
        writer.put(CRLF);
        for (final byte[] arg : args) {
            dbs = DOLLAR_LENGTH_MAP.get(arg.length);
            if (dbs == null) {
                writer.put(DOLLAR_BYTE);
                writer.put(String.valueOf(arg.length).getBytes(StandardCharsets.UTF_8));
                writer.put(CRLF);
            } else {
                writer.put(dbs);
            }
            writer.put(arg);
            writer.put(CRLF);
        }
    }

    @Override
    public String toString() {
        return "RedisClientRequest{" + "command=" + command + ", cacheType=" + cacheType + ", resultType=" + resultType + ", set=" + set + ", key=" + key + ", args=" + args + '}';
    }

}
