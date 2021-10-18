/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class RedisCacheRequest extends ClientRequest {

    protected String key;

    protected String command;

    protected byte[][] args;

    public <T> RedisCacheRequest prepare(String command, String key, byte[]... args) {
        super.prepare();
        this.command = command;
        this.key = key;
        this.args = args;
        return this;
    }

    @Override
    public void accept(ClientConnection conn, ByteArray writer) {
        writer.put((byte) '*');
        writer.put(String.valueOf(args.length + 1).getBytes(StandardCharsets.UTF_8));
        writer.put((byte) '\r', (byte) '\n');
        writer.put((byte) '$');
        writer.put(String.valueOf(command.length()).getBytes(StandardCharsets.UTF_8));
        writer.put((byte) '\r', (byte) '\n');
        writer.put(command.getBytes(StandardCharsets.UTF_8));
        writer.put((byte) '\r', (byte) '\n');

        for (final byte[] arg : args) {
            writer.put((byte) '$');
            writer.put(String.valueOf(arg.length).getBytes(StandardCharsets.UTF_8));
            writer.put((byte) '\r', (byte) '\n');
            writer.put(arg);
            writer.put((byte) '\r', (byte) '\n');
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{command=" + command + ", key=" + key + "}";
    }
}
