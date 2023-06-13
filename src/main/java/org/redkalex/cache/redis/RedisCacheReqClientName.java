/*
 *
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class RedisCacheReqClientName extends RedisCacheRequest {

    private final String clientName;

    public RedisCacheReqClientName(String appName, String resourceName) {
        this.clientName = "redkalex" + (Utility.isEmpty(appName) ? "" : ("-" + appName))
            + (Utility.isEmpty(resourceName) ? "" : (":" + resourceName));
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray writer) {
        writer.put((byte) '*');
        writer.put((byte) '3');
        writer.put((byte) '\r', (byte) '\n');

        writer.put((byte) '$');
        writer.put((byte) '6');
        writer.put((byte) '\r', (byte) '\n');
        writer.put("CLIENT".getBytes(StandardCharsets.UTF_8));
        writer.put((byte) '\r', (byte) '\n');

        writer.put((byte) '$');
        writer.put((byte) '7');
        writer.put((byte) '\r', (byte) '\n');
        writer.put("SETNAME".getBytes(StandardCharsets.UTF_8));
        writer.put((byte) '\r', (byte) '\n');

        byte[] ns = clientName.getBytes(StandardCharsets.UTF_8);
        writer.put((byte) '$');
        writer.put(String.valueOf(ns.length).getBytes(StandardCharsets.UTF_8));
        writer.put((byte) '\r', (byte) '\n');
        writer.put(ns);
        writer.put((byte) '\r', (byte) '\n');
    }
}
