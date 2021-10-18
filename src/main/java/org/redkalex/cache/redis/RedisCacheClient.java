/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import org.redkale.net.*;
import org.redkale.net.client.*;

/**
 *
 * @author zhangjx
 */
public class RedisCacheClient extends Client<RedisCacheRequest, RedisCacheResult> {

    protected static final int DEFAULT_POOL_SIZE = Integer.getInteger("redkale.client.response.pool.size", 256);

    public RedisCacheClient(AsyncGroup group, String key, SocketAddress address, int maxConns, int maxPipelines, RedisCacheReqAuth authreq, RedisCacheReqDB dbreq) {
        super(group, true, address, maxConns, maxPipelines, p -> new RedisCacheCodec(), RedisCacheReqPing.INSTANCE, RedisCacheReqClose.INSTANCE, null); //maxConns
        this.connectionContextName = "redkalex-redis-client-connection-" + key;
        if (authreq != null || dbreq != null) {
            if (authreq != null && dbreq != null) {
                this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, authreq).thenCompose(v -> writeChannel(conn, dbreq)).thenApply(v -> conn));
            } else if (authreq != null) {
                this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, authreq).thenApply(v -> conn));
            } else {
                this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, dbreq).thenApply(v -> conn));
            }
        }
    }

    @Override
    protected ClientConnection createClientConnection(final int index, AsyncConnection channel) {
        return new RedisCacheConnection(this, index, channel);
    }

    CompletableFuture<RedisCacheConnection> pollConnection() {
        return (CompletableFuture) super.connect(null);
    }
}
