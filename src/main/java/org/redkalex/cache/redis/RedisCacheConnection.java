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
class RedisCacheConnection<R extends RedisCacheRequest> extends ClientConnection<R, RedisCacheResult> {

    public RedisCacheConnection(Client client, int index, AsyncConnection channel) {
        super(client, index, channel);
    }

    @Override
    protected CompletableFuture<RedisCacheResult> writeChannel(R request) {
        return super.writeChannel(request);
    }

    public RedisCacheResult pollResultSet(RedisCacheRequest request) {
        RedisCacheResult rs = new RedisCacheResult();
        rs.request = request;
        return rs;
    }

    public RedisCacheRequest pollRequest(WorkThread workThread) {
        RedisCacheRequest rs = new RedisCacheRequest();
        rs.currThread(workThread);
        return rs;
    }
}
