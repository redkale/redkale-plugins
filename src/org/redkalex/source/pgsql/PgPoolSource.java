/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Logger;
import org.redkale.net.*;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class PgPoolSource extends PoolTcpSource {

    protected PgClient client;

    public PgPoolSource(AsyncGroup asyncGroup, String rwtype, ArrayBlockingQueue queue, Semaphore semaphore, Properties prop, Logger logger) {
        super(asyncGroup, rwtype, queue, semaphore, prop, logger);
        PgReqAuthentication authreq = new PgReqAuthentication(this.username, this.password, this.database);
        this.client = new PgClient(rwtype, asyncGroup, this.servaddr, this.maxconns, prop, authreq);
    }

    @Override
    public void close() {
        super.close();
        this.client.close();
    }

    public CompletableFuture<PgResultSet> sendAsync(ChannelContext context, PgClientRequest req) {
        return client.sendAsync(context, req);
    }

    @Override
    protected ByteArray reqConnectBuffer(AsyncConnection conn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected void respConnectBuffer(final ByteBuffer buffer, CompletableFuture<AsyncConnection> future, AsyncConnection conn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected int getDefaultPort() {
        return 5432;
    }

    @Override
    protected CompletableFuture<AsyncConnection> sendPingCommand(AsyncConnection conn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected CompletableFuture<AsyncConnection> sendCloseCommand(AsyncConnection conn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
