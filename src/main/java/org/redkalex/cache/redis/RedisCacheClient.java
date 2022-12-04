/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.util.concurrent.CompletableFuture;
import org.redkale.net.*;
import org.redkale.net.client.*;

/**
 *
 * @author zhangjx
 */
public class RedisCacheClient extends Client<RedisCacheRequest, RedisCacheResult> {

    public RedisCacheClient(AsyncGroup group, String key, ClientAddress address, int maxConns, int maxPipelines, RedisCacheReqAuth authReq, RedisCacheReqDB dbReq) {
        super(group, true, address, maxConns, maxPipelines, RedisCacheReqPing.INSTANCE, RedisCacheReqClose.INSTANCE, null); //maxConns
        this.connectionContextName = "redkalex-redis-client-connection-" + key;
        if (authReq != null || dbReq != null) {
            if (authReq != null && dbReq != null) {
                this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, authReq).thenCompose(v -> writeChannel(conn, dbReq)).thenApply(v -> conn));
            } else if (authReq != null) {
                this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, authReq).thenApply(v -> conn));
            } else {
                this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, dbReq).thenApply(v -> conn));
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
