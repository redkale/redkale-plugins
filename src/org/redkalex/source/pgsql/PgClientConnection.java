/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.AsyncConnection;
import org.redkale.net.client.*;

/**
 *
 * @author zhangjx
 * @param <R> ClientRequest
 * @param <P> 响应对象
 */
public class PgClientConnection<R extends PgClientRequest, P> extends ClientConnection<R, P> {

    protected PgExtendedCommandNode currExtendedCacheNode;

    public PgClientConnection(Client client, int index, AsyncConnection channel) {
        super(client, index, channel);
    }

    @Override
    protected CompletableFuture<P> writeChannel(R request) {
        if (((PgClient) client).prepareCacheable && (request.getType() & 0x1) == 1) {  //缓存preparecache
            final PgReqExtendedCommand req = (PgReqExtendedCommand) request;
            PgExtendedCommandNode extendedNode;
            synchronized (this) {
                extendedNode = this.currExtendedCacheNode;
                if (extendedNode == null || extendedNode.statementIndex != req.statement) {
                    extendedNode = new PgExtendedCommandNode();
                    extendedNode.statementIndex = req.statement;
                    this.currExtendedCacheNode = extendedNode;
                }
                request.currExtendedRowDescSupplier = extendedNode;
                if (extendedNode.currExtendedRowDescFlag.compareAndSet(false, true)) {
                    req.firstPrepare = true;
                    return super.writeChannel(request).thenCompose(r -> {
                        req.firstPrepare = false;
                        return super.writeChannel(request);
                    });
                }
            }
        }
        return super.writeChannel(request);
    }

    @Deprecated
    ClientFuture impCreateClientFuture(R request) {
        final PgClient pgClient = (PgClient) client;
        PgExtendedCommandNode extendedNode = this.currExtendedCacheNode;
        if (request.getType() == PgClientRequest.REQ_TYPE_EXTEND_QUERY
            && extendedNode != null && extendedNode.rowDesc != null) {
            AtomicBoolean repeat = new AtomicBoolean(true);
            final PgReqExtendedCommand req = (PgReqExtendedCommand) request;
            final String sql = (req.parameters == null || req.parameters.length == 0)
                ? req.sql : (req.sql + " " + (req.parameters.length == 1 && req.parameters[0].length == 1 ? req.parameters[0][0] : JsonConvert.root().convertTo(req.parameters)));
            PgClientFuture<PgResultSet> future = pgClient.concurrentQueryBatchMap.computeIfAbsent(sql, c -> {
                PgClientFuture f = new PgClientFuture(request);
                f.callback = () -> pgClient.concurrentQueryBatchMap.remove(sql);
                repeat.set(false);
                return f;
            });
            if (repeat.get()) {
                return future;//(PgClientFuture) future.thenApply(r -> r.copy());
            } else {
                return future;
            }
        }
        return new PgClientFuture(request);
    }

}
