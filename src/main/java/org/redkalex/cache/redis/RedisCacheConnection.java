/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.redkale.net.*;
import org.redkale.net.client.*;

/**
 *
 * @author zhangjx
 */
public class RedisCacheConnection extends ClientConnection<RedisCacheRequest, RedisCacheResult> {

    public RedisCacheConnection(Client client, AsyncConnection channel) {
        super(client, channel);
    }

    @Override
    protected ClientCodec createCodec() {
        return new RedisCacheCodec(this);
    }

    protected CompletableFuture<RedisCacheResult> writeRequest(RedisCacheRequest request) {
        return super.writeChannel(request);
    }

    protected CompletableFuture<List<RedisCacheResult>> writeRequest(RedisCacheRequest[] requests) {
        return super.writeChannel(requests);
    }

    protected <T> CompletableFuture<T> writeRequest(RedisCacheRequest request, Function<RedisCacheResult, T> respTransfer) {
        return super.writeChannel(request, respTransfer);
    }

    protected <T> CompletableFuture<List<T>> writeRequest(RedisCacheRequest[] requests, Function<RedisCacheResult, T> respTransfer) {
        return super.writeChannel(requests, respTransfer);
    }

    public RedisCacheResult pollResultSet(RedisCacheRequest request) {
        RedisCacheResult rs = new RedisCacheResult();
        return rs;
    }

    protected ClientFuture<RedisCacheRequest, RedisCacheResult> pollRespFuture(Serializable requestid) {
        return super.pollRespFuture(requestid);
    }
}
