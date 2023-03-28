/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.AbstractDataSource.SourceUrlInfo;

/**
 *
 * @author zhangjx
 */
public class MyClient extends Client<MyClientConnection, MyClientRequest, MyResultSet> {

    protected final boolean cachePreparedStatements;

    protected final boolean autoddl;

    protected final SourceUrlInfo info;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public MyClient(String name, AsyncGroup group, String key, ClientAddress address, int maxConns, int maxPipelines,
        final Properties prop, final SourceUrlInfo info, boolean autoddl, final Properties attributes) {
        super(name, group, true, address, maxConns, maxPipelines, () -> new MyReqPing(), () -> new MyReqClose(), null); //maxConns
        this.info = info;
        this.autoddl = autoddl;
        this.authenticate = conn -> {
            MyRespHandshakeResultSet handshake = ((MyClientConnection) conn).handshake;
            return writeChannel(conn, new MyReqAuthentication(handshake, info.username, info.password, info.database, attributes)).thenCompose(v -> {
                MyRespAuthResultSet authrs = (MyRespAuthResultSet) v;
                if (authrs.authSwitch != null) {
                    return writeChannel(conn, authrs.authSwitch);
                }
                return CompletableFuture.completedFuture(authrs);
            }).thenApply(v -> conn);
        };
        this.cachePreparedStatements = prop == null || "true".equalsIgnoreCase(prop.getProperty("preparecache", "true"));
    }

    @Override
    protected MyClientConnection createClientConnection(final int index, AsyncConnection channel) {
        return new MyClientConnection(this, index, channel);
    }

    @Override  //构建连接上先从服务器拉取数据构建的虚拟请求
    protected MyClientRequest createVirtualRequestAfterConnect() {
        return new MyReqVirtual();
    }

    @Override
    protected CompletableFuture<MyResultSet> writeChannel(ClientConnection conn, MyClientRequest request) {
        return super.writeChannel(conn, request);
    }

    @Override
    protected CompletableFuture<MyClientConnection> connect() {
        return super.connect();
    }

    @Override
    protected void handlePingResult(MyClientConnection conn, MyResultSet result) {
        if (result != null) {
            result.close();
        }
    }

    protected Logger logger() {
        return logger;
    }

    public boolean cachePreparedStatements() {
        return cachePreparedStatements;
    }

}
