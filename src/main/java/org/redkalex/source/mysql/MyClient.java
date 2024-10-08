/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.logging.Logger;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.AbstractDataSource.SourceUrlInfo;
import org.redkale.util.Traces;

/** @author zhangjx */
public class MyClient extends Client<MyClientConnection, MyClientRequest, MyResultSet> {

    protected final boolean cachePreparedStatements;

    protected final boolean autoddl;

    protected final SourceUrlInfo info;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public MyClient(
            String name,
            AsyncGroup group,
            String key,
            ClientAddress address,
            int maxConns,
            int maxPipelines,
            final Properties prop,
            final SourceUrlInfo info,
            boolean autoddl,
            boolean clientNonBlocking,
            final Properties attributes) {
        super(name, group, true, address, maxConns, maxPipelines, MyReqPing::new, MyReqClose::new, null); // maxConns
        this.info = info;
        this.autoddl = autoddl;
        this.nonBlocking = clientNonBlocking;
        this.authenticate = (workThread, traceid) -> {
            Traces.currentTraceid(traceid);
            return conn -> {
                MyRespHandshakeResultSet handshake = ((MyClientConnection) conn).handshake;
                MyReqAuthentication authReq = new MyReqAuthentication(
                                handshake, info.username, info.password, info.database, attributes)
                        .workThread(workThread);
                return writeChannel(conn, authReq)
                        .thenCompose(v -> {
                            Traces.currentTraceid(traceid);
                            MyRespAuthResultSet authrs = (MyRespAuthResultSet) v;
                            if (authrs.authSwitch != null) {
                                return writeChannel(conn, authrs.authSwitch.workThread(workThread));
                            }
                            return CompletableFuture.completedFuture(authrs);
                        })
                        .thenApply(v -> conn);
            };
        };
        this.cachePreparedStatements =
                prop == null || "true".equalsIgnoreCase(prop.getProperty("preparecache", "true"));
    }

    @Override
    protected MyClientConnection createClientConnection(AsyncConnection channel) {
        return new MyClientConnection(this, channel);
    }

    @Override // 构建连接上先从服务器拉取数据构建的虚拟请求
    protected MyClientRequest createVirtualRequestAfterConnect() {
        return new MyReqVirtual();
    }

    @Override
    protected CompletableFuture<MyResultSet> writeChannel(ClientConnection conn, MyClientRequest request) {
        return super.writeChannel(conn, request);
    }

    @Override
    protected <T> CompletableFuture<T> writeChannel(
            ClientConnection conn, Function<MyResultSet, T> respTransfer, MyClientRequest request) {
        return super.writeChannel(conn, respTransfer, request);
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
