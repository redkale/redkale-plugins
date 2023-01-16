/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.source.AbstractDataSource.SourceUrlInfo;

/**
 *
 * @author zhangjx
 */
public class PgClient extends Client<PgClientConnection, PgClientRequest, PgResultSet> {

    private static final AtomicInteger extendedStatementIndex = new AtomicInteger();

    protected static final ConcurrentHashMap<String, Long> extendedStatementIndexMap = new ConcurrentHashMap();

    protected final boolean cachePreparedStatements;

    protected final boolean autoddl;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public PgClient(String name, AsyncGroup group, String key, ClientAddress address, int maxConns, int maxPipelines, boolean autoddl, final Properties prop, final SourceUrlInfo info) {
        super(name, group, true, address, maxConns, maxPipelines, () -> new PgReqPing(), () -> new PgReqClose(), null); //maxConns
        this.autoddl = autoddl;
        this.connectionContextName = "redkalex-pgsql-client-connection-" + key;
        this.authenticate = conn -> writeChannel(conn, new PgReqAuthentication(info)).thenCompose((PgResultSet rs0) -> {
            PgRespAuthResultSet rs = (PgRespAuthResultSet) rs0;
            if (rs.isAuthOK()) {
                return CompletableFuture.completedFuture(conn);
            }
            if (rs.getAuthSalt() != null) {
                return writeChannel(conn, new PgReqAuthMd5Password(info.username, info.password, rs.getAuthSalt())).thenApply(pg -> conn);
            }
            return writeChannel(conn, new PgReqAuthScramPassword(info.username, info.password, rs.getAuthMechanisms()))
                .thenCompose((PgResultSet rs2) -> {
                    PgReqAuthScramSaslContinueResult cr = ((PgRespAuthResultSet) rs2).getAuthSaslContinueResult();
                    if (cr == null) {
                        return CompletableFuture.completedFuture(conn);
                    }
                    return writeChannel(conn, new PgReqAuthScramSaslFinal(cr)).thenApply(pg -> conn);
                });
        });
        this.cachePreparedStatements = prop == null || "true".equalsIgnoreCase(prop.getProperty("preparecache", "true"));
    }

    @Override
    protected PgClientConnection createClientConnection(final int index, AsyncConnection channel) {
        return new PgClientConnection(this, index, channel);
    }

    @Override
    protected CompletableFuture<PgResultSet> writeChannel(ClientConnection conn, PgClientRequest request) {
        return super.writeChannel(conn, request);
    }

    @Override
    protected <T> CompletableFuture<T> writeChannel(ClientConnection conn, PgClientRequest request, Function< PgResultSet, T> respTransfer) {
        return super.writeChannel(conn, request, respTransfer);
    }

    @Override
    protected CompletableFuture<PgClientConnection> connect(final ChannelContext context) {
        return super.connect(context);
    }

    @Override
    protected void handlePingResult(PgClientConnection conn, PgResultSet result) {
        if (result != null) {
            result.close();
        }
    }

    public boolean cachePreparedStatements() {
        return cachePreparedStatements;
    }

    public long extendedStatementid(String sql) {
        if (!cachePreparedStatements) {
            return 0L;
        }
        return extendedStatementIndexMap.computeIfAbsent(sql, s -> {
            short val = (short) extendedStatementIndex.getAndIncrement();
            long next = 0x30_30_30_00_00_00_00_00L;
            next |= toHex(val >> 12 & 0xF) << 32;
            next |= toHex(val >> 8 & 0xF) << 24;
            next |= toHex(val >> 4 & 0xF) << 16;
            next |= toHex(val >> 0 & 0xF) << 8;
            return next;
        });
    }

    private static long toHex(int c) {
        if (c < 10) {
            return (byte) ('0' + c);
        } else {
            return (byte) ('A' + c - 10);
        }
    }

}
