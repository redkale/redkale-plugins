/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache;

import java.io.Serializable;
import java.net.SocketAddress;
import org.redkale.net.AsyncGroup;
import org.redkale.net.client.Client;

/**
 *
 * @author zhangjx
 */
public class RedisClient extends Client<RedisClientRequest, Serializable> {

    protected RedisReqAuth authreq;

    public RedisClient(AsyncGroup group, SocketAddress address, int maxConns, int maxPipelines, final RedisReqAuth authreq) {
        super(group, true, address, maxConns, maxPipelines, p -> new RedisClientCodec(), null, null, null);
        this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, authreq).thenApply(r -> r == null ? null : conn));
    }

}
