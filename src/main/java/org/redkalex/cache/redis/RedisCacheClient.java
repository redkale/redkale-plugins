/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import org.redkale.net.*;
import org.redkale.net.client.*;

/**
 *
 * @author zhangjx
 */
public class RedisCacheClient extends Client<RedisCacheConnection, RedisCacheRequest, RedisCacheResult> {

    public RedisCacheClient(String appName, String name, AsyncGroup group, String key,
        ClientAddress address, int maxConns, int maxPipelines, RedisCacheReqAuth authReq, RedisCacheReqDB dbReq) {
        super(name, group, true, address, maxConns, maxPipelines, () -> new RedisCacheReqPing(), () -> new RedisCacheReqClose(), null); //maxConns
        RedisCacheReqClientName clientNameReq = new RedisCacheReqClientName(appName, name);
        if (authReq != null || dbReq != null) {
            if (authReq != null && dbReq != null) {
                this.authenticate = conn -> writeChannel(conn, authReq)
                    .thenCompose(v -> writeChannel(conn, dbReq))
                    .thenCompose(v -> writeChannel(conn, clientNameReq))
                    .thenApply(v -> conn);
            } else if (authReq != null) {
                this.authenticate = conn -> writeChannel(conn, authReq)
                    .thenCompose(v -> writeChannel(conn, clientNameReq))
                    .thenApply(v -> conn);
            } else {
                this.authenticate = conn -> writeChannel(conn, dbReq)
                    .thenCompose(v -> writeChannel(conn, clientNameReq))
                    .thenApply(v -> conn);
            }
        } else {
            this.authenticate = conn -> writeChannel(conn, clientNameReq).thenApply(v -> conn);
        }
        this.readTimeoutSeconds = 3;
        this.writeTimeoutSeconds = 3;
    }

    @Override
    protected RedisCacheConnection createClientConnection(final int index, AsyncConnection channel) {
        return new RedisCacheConnection(this, index, channel);
    }

}
