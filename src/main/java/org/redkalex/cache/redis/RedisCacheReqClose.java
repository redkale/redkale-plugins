/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class RedisCacheReqClose extends RedisCacheRequest {

    private static final byte[] BYTES = new ByteArray()
            .put((byte) '*')
            .put((byte) '1')
            .put((byte) '\r', (byte) '\n')
            .put((byte) '$')
            .put((byte) '4')
            .put((byte) '\r', (byte) '\n')
            .put("QUIT".getBytes(StandardCharsets.UTF_8))
            .put((byte) '\r', (byte) '\n')
            .getBytes();

    @Override
    public final boolean isCloseType() {
        return true;
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray writer) {
        writer.put(BYTES);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{QUIT}";
    }
}
