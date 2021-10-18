/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.net.*;
import java.util.Properties;
import java.util.concurrent.*;
import org.redkale.net.*;
import org.redkale.net.client.*;

/**
 *
 * @author zhangjx
 */
public class MyClient extends Client<MyClientRequest, MyResultSet> {

    protected static final int DEFAULT_POOL_SIZE = Integer.getInteger("redkale.client.response.pool.size", 256);

    protected final boolean cachePreparedStatements;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public MyClient(AsyncGroup group, String key, SocketAddress address, int maxConns, int maxPipelines, final Properties prop,
        final String username, final String password, final String database, final String encoding, final Properties attributes) {
        super(group, true, address, maxConns, maxPipelines, p -> new MyClientCodec(), MyReqPing.INSTANCE, MyReqClose.INSTANCE, null); //maxConns
        this.connectionContextName = "redkalex-mysql-client-connection-" + key;
        this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, MyClientRequest.EMPTY).thenCompose((MyResultSet rs) -> {
            MyRespHandshakeResultSet handshake = (MyRespHandshakeResultSet) rs;
            CompletableFuture<MyResultSet> authFuture = writeChannel(conn, new MyReqAuthentication(handshake, username, password, database, attributes));
            return authFuture.thenCompose(v -> {
                MyRespAuthResultSet authrs = (MyRespAuthResultSet) v;
                if (authrs.authSwitch != null) return writeChannel(conn, authrs.authSwitch);
                return CompletableFuture.completedFuture(authrs);
            }).thenApply(v -> conn);
        }));
        this.cachePreparedStatements = prop == null || "true".equalsIgnoreCase(prop.getProperty("javax.persistence.jdbc.preparecache", "true"));
    }

    @Override
    protected ClientConnection createClientConnection(final int index, AsyncConnection channel) {
        return new MyClientConnection(this, index, channel);
    }

    @Override
    protected CompletableFuture<MyResultSet> writeChannel(ClientConnection conn, MyClientRequest request) {
        return super.writeChannel(conn, request);
    }

    @Override
    protected CompletableFuture<ClientConnection> connect(final ChannelContext context) {
        return super.connect(context);
    }

    @Override
    protected void handlePingResult(ClientConnection conn, MyResultSet result) {
        if (result != null) result.close();
    }

    public boolean cachePreparedStatements() {
        return cachePreparedStatements;
    }

}
