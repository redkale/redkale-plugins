/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import org.redkale.annotation.Nullable;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.util.Traces;

/** @author zhangjx */
public class RedisCacheClient extends Client<RedisCacheConnection, RedisCacheRequest, RedisCacheResult> {

    public RedisCacheClient(
            String appName,
            String name,
            AsyncGroup group,
            String key,
            ClientAddress address,
            int maxConns,
            int maxPipelines,
            RedisCacheReqAuth authReq,
            RedisCacheReqDB dbReq) {
        super(
                name,
                group,
                true,
                address,
                maxConns,
                maxPipelines,
                RedisCacheReqPing::new,
                RedisCacheReqClose::new,
                null); // maxConns
        RedisCacheReqClientName clientNameReq = new RedisCacheReqClientName(appName, name);
        if (authReq != null && dbReq != null) {
            this.authenticate = traceid -> {
                Traces.currentTraceid(traceid);
                return conn -> writeChannelBatch(
                                conn, authReq.createTime(), dbReq.createTime(), clientNameReq.createTime())
                        .thenApply(v -> conn);
            };
        } else if (authReq != null) {
            this.authenticate = traceid -> {
                Traces.currentTraceid(traceid);
                return conn -> writeChannelBatch(conn, authReq.createTime(), clientNameReq.createTime())
                        .thenApply(v -> conn);
            };
        } else if (dbReq != null) {
            this.authenticate = traceid -> {
                Traces.currentTraceid(traceid);
                return conn -> writeChannelBatch(conn, dbReq.createTime(), clientNameReq.createTime())
                        .thenApply(v -> conn);
            };
        } else {
            this.authenticate = traceid -> {
                Traces.currentTraceid(traceid);
                return conn -> writeChannel(conn, clientNameReq.createTime()).thenApply(v -> conn);
            };
        }
        this.connectTimeoutSeconds = 3;
        this.readTimeoutSeconds = 3;
        this.writeTimeoutSeconds = 3;
    }

    @Override
    public RedisCacheConnection createClientConnection(AsyncConnection channel) {
        return new RedisCacheConnection(this, channel);
    }

    @Override
    public CompletableFuture<RedisCacheConnection> connect(RedisCacheRequest request) {
        return super.connect(request);
    }

    @Override // 根据请求获取地址
    protected SocketAddress getAddress(@Nullable RedisCacheRequest request) {
        return super.getAddress(request);
    }
}
