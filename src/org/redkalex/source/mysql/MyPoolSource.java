/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.logging.Logger;
import org.redkale.net.AsyncConnection;
import org.redkale.source.PoolTcpSource;
import org.redkale.util.*;
import static org.redkalex.source.mysql.MySQLDataSource.*;

/**
 *
 * @author zhangjx
 */
public class MyPoolSource extends PoolTcpSource {

    protected static final String CONN_ATTR_BYTESBAME = "BYTESBAME";

    public MyPoolSource(String rwtype, ArrayBlockingQueue queue, Properties prop,
        Logger logger, ObjectPool<ByteBuffer> bufferPool, ThreadPoolExecutor executor) {
        super(rwtype, queue, prop, logger, bufferPool, executor);
    }

    @Override
    protected ByteBuffer reqConnectBuffer(AsyncConnection conn) {
        if (conn.getAttribute(CONN_ATTR_BYTESBAME) == null) conn.setAttribute(CONN_ATTR_BYTESBAME, new byte[1024]);
        return null;
    }

    public static void main(String[] args) throws Throwable {
        MySQLTest.main(args);
    }

    protected static final String CONN_ATTR_PROTOCOL_VERSION = "PROTOCOL_VERSION";

    @Override
    protected void respConnectBuffer(final ByteBuffer buffer, CompletableFuture<AsyncConnection> future, AsyncConnection conn) {
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        if (true) {
            //MySQLIO.doHandshake
            int packetLength = (buffer.get() & 0xff) + ((buffer.get() & 0xff) << 8) + ((buffer.get() & 0xff) << 16);
            byte multiPacketSeq = buffer.get();
            final int pkgstart = buffer.position();
            int protocolVersion = buffer.get();
            if (protocolVersion < 10) {
                conn.dispose();
                future.completeExceptionally(new SQLException("Not supported protocolVersion(" + protocolVersion + "), must greaterthan 10"));
                return;
            }
            String serverVersion = readASCIIString(buffer, bytes);
            if (serverVersion.startsWith("0.") || serverVersion.startsWith("1.")
                || serverVersion.startsWith("2.") || serverVersion.startsWith("3.") || serverVersion.startsWith("4.")) {
                conn.dispose();
                future.completeExceptionally(new SQLException("Not supported serverVersion(" + serverVersion + "), must greaterthan 5.0"));
                return;
            }
            long threadId = readLong(buffer);
            String seed = null;
            if (protocolVersion > 9) {
                // read auth-plugin-data-part-1 (string[8])
                seed = readASCIIString(buffer, 8);
                // read filler ([00])
                buffer.get();
            } else {
                // read scramble (string[NUL])
                seed = readASCIIString(buffer, bytes);
            }

            System.out.println("protocolVersion = " + protocolVersion);
            System.out.println("serverVersion = " + serverVersion);
            System.out.println("threadId = " + threadId);
            System.out.println("seed = " + seed);
            future.completeExceptionally(new SQLException("mysql connect error"));
            return;
        }
        char cmd = (char) buffer.get();
        int length = buffer.getInt();

        if (cmd == 'Z') { //ReadyForQuery
            bufferPool.accept(buffer);
            future.complete(conn);
            return;
        }
        bufferPool.accept(buffer);
        future.completeExceptionally(new SQLException("mysql connect resp error"));
        conn.dispose();
    }

    @Override
    protected int getDefaultPort() {
        return 3306;
    }

    @Override
    protected CompletableFuture<AsyncConnection> sendCloseCommand(AsyncConnection conn) {
        return null;
    }

}
