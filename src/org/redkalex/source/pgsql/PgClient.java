/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.net.*;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.Logger;
import org.redkale.net.*;
import org.redkale.net.client.*;

/**
 *
 * @author zhangjx
 */
public class PgClient extends Client<PgClientRequest, PgResultSet> {

    private static final AtomicInteger seq = new AtomicInteger();

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected PgReqAuthentication authreq;

    protected final boolean prepareCacheable;

    protected static final AtomicInteger extendedStatementIndex = new AtomicInteger();

    protected static final ConcurrentHashMap<String, Long> extendedStatementIndexMap = new ConcurrentHashMap();

    protected final ConcurrentHashMap<String, PgClientFuture> concurrentQueryBatchMap = new ConcurrentHashMap<>();

    protected final ThreadLocal<ClientConnection> localConnection = ThreadLocal.withInitial(() -> null);

    protected final ConcurrentLinkedDeque<ClientConnection> threadConnections = new ConcurrentLinkedDeque();

    private final AtomicBoolean prepareThreadFinished = new AtomicBoolean();

    private final AtomicInteger prepareThreadIndex = new AtomicInteger();

    private final String pgconnectionAttrName;

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public PgClient(String rwtype, AsyncGroup group, SocketAddress address, int maxConns, final Properties prop, final PgReqAuthentication authreq) {
        super(group, true, address, maxConns, 20, p -> new PgClientCodec(), PgReqPing.INSTANCE, PgReqClose.INSTANCE, null); //maxConns
        this.pgconnectionAttrName = "pg-client-" + rwtype + "-connection";
        this.authenticate = future -> future.thenCompose(conn -> writeChannel(conn, authreq).thenCompose((PgResultSet rs) -> {
            if (rs.isAuthOK()) return CompletableFuture.completedFuture(conn);
            return writeChannel(conn, new PgReqAuthentication.PgReqAuthPassword(authreq.username, authreq.password, rs.getAuthSalt())).thenApply(pg -> conn);
        }));
        this.prepareCacheable = prop != null && "true".equalsIgnoreCase(prop.getProperty("javax.persistence.jdbc.preparecache"));
        if (this.prepareCacheable) {
            int seqid = seq.getAndIncrement();
            if (seqid > 0) {
                try {
                    Thread.sleep(seqid * 200L);
                } catch (Exception e) {
                }
            }
            long s = System.currentTimeMillis();
            CompletableFuture[] futures = new CompletableFuture[connArray.length];
            for (int i = 0; i < connArray.length; i++) {
                futures[i] = connect(null);
            }
            CompletableFuture.allOf(futures).whenComplete((r, t) -> {
                for (int i = 0; i < connArray.length; i++) {
                    threadConnections.add(connArray[i]);
                }
                prepareThreadFinished.set(true);
                logger.info("PgClient " + rwtype + " " + connArray.length + " conns, connect: " + (System.currentTimeMillis() - s) + " ms");
            });
        }
    }

    @Override
    protected CompletableFuture<ClientConnection> connect(ChannelContext context) {
        if (!prepareThreadFinished.get()) return super.connect(context);
        ClientConnection conn;
        if (context == null) {
            conn = localConnection.get();
            if (conn != null && conn.isOpen()) return CompletableFuture.completedFuture(conn);
            conn = threadConnections.poll();
            if (conn != null && conn.isOpen()) {
                localConnection.set(conn);
                return CompletableFuture.completedFuture(conn);
            }
            conn = connArray[prepareThreadIndex.getAndIncrement() % connArray.length];
            localConnection.set(conn);
            return CompletableFuture.completedFuture(conn);
        } else {
            conn = context.getAttribute(pgconnectionAttrName);
            if (conn != null && conn.isOpen()) return CompletableFuture.completedFuture(conn);
            conn = threadConnections.poll();
            if (conn != null && conn.isOpen()) {
                context.setAttribute(pgconnectionAttrName, conn);
                return CompletableFuture.completedFuture(conn);
            }
            conn = connArray[prepareThreadIndex.getAndIncrement() % connArray.length];
            context.setAttribute(pgconnectionAttrName, conn);
            return CompletableFuture.completedFuture(conn);
        }
    }

    protected long extendedStatementIndex(String sql) {
        if (!prepareCacheable) return 0L;
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

    @Override
    protected ClientConnection createClientConnection(final int index, AsyncConnection channel) {
        return new PgClientConnection(this, index, channel);
    }

    //调试用
    public ClientConnection<PgClientRequest, PgResultSet>[] getConnArray() {
        return connArray;
    }

    public AtomicLong getWriteReqCounter() {
        return writeReqCounter;
    }

    public AtomicLong getPollRespCounter() {
        return pollRespCounter;
    }

}
