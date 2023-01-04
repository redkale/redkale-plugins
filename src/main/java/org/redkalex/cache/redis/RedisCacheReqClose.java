/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class RedisCacheReqClose extends RedisCacheRequest {

    private static final byte[] PS = "QUIT".getBytes(StandardCharsets.UTF_8);

    @Override
    public final boolean isCloseType() {
        return true;
    }
    
    @Override
    public void accept(ClientConnection conn, ByteArray writer) {
        writer.put((byte) '*');
        writer.put((byte) '1');
        writer.put((byte) '\r', (byte) '\n');
        writer.put((byte) '$');
        writer.put((byte) '4');
        writer.put((byte) '\r', (byte) '\n');
        writer.put(PS);
        writer.put((byte) '\r', (byte) '\n');
    }
}
