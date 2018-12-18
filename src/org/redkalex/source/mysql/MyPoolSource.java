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
import static org.redkalex.source.mysql.MySQLs.*;

/**
 *
 * @author zhangjx
 */
public class MyPoolSource extends PoolTcpSource {

    protected static final String CONN_ATTR_BYTESBAME = "BYTESBAME";

    public MyPoolSource(String rwtype, ArrayBlockingQueue queue, Semaphore semaphore, Properties prop,
        Logger logger, ObjectPool<ByteBuffer> bufferPool, ThreadPoolExecutor executor) {
        super(rwtype, queue, semaphore, prop, logger, bufferPool, executor);
    }

    @Override
    protected ByteBuffer reqConnectBuffer(AsyncConnection conn) {
        if (conn.getAttribute(CONN_ATTR_BYTESBAME) == null) conn.setAttribute(CONN_ATTR_BYTESBAME, new byte[1024]);
        return null;
    }

    protected static final String CONN_ATTR_PROTOCOL_VERSION = "PROTOCOL_VERSION";

    @Override
    protected void respConnectBuffer(final ByteBuffer buffer, CompletableFuture<AsyncConnection> future, AsyncConnection conn) {
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        if (true) {
            //MySQLIO.doHandshake
            final int packetLength = (buffer.get() & 0xff) + ((buffer.get() & 0xff) << 8) + ((buffer.get() & 0xff) << 16);
            final byte multiPacketSeq = buffer.get();
            final int pkgstart = buffer.position();
            long clientParam = 0;
            int protocolVersion = buffer.get();
            if (protocolVersion < 10) { //小于10的版本暂不实现                
                bufferPool.accept(buffer);
                conn.dispose();
                future.completeExceptionally(new SQLException("Not supported protocolVersion(" + protocolVersion + "), must greaterthan 10"));
                return;
            }
            String serverVersion = readASCIIString(buffer, bytes);
            if (serverVersion.startsWith("0.") || serverVersion.startsWith("1.")
                || serverVersion.startsWith("2.") || serverVersion.startsWith("3.") || serverVersion.startsWith("4.")) {
                bufferPool.accept(buffer);
                conn.dispose();
                future.completeExceptionally(new SQLException("Not supported serverVersion(" + serverVersion + "), must greaterthan 5.0"));
                return;
            }
            final boolean useNewLargePackets = true;
            final long threadId = readLong(buffer);
            String seed = null;
            if (protocolVersion > 9) {
                // read auth-plugin-data-part-1 (string[8])
                seed = readASCIIString(buffer, 8);
                // read filler ([00])
                byte b = buffer.get();
                System.out.println("-------------b: " + (int) b);
            } else {
                // read scramble (string[NUL])
                seed = readASCIIString(buffer, bytes);
            }
            int authPluginDataLength = 0;
            boolean hasLongColumnInfo = false;
            int serverCapabilities = buffer.hasRemaining() ? readInt(buffer) : 0;
            final int serverCharsetIndex = buffer.get() & 0xff;
            final int serverStatus = readInt(buffer);
            serverCapabilities |= readInt(buffer) << 16;
            if ((serverCapabilities & CLIENT_PLUGIN_AUTH) != 0) {
                // read length of auth-plugin-data (1 byte)
                authPluginDataLength = buffer.get() & 0xff;
            } else {
                // read filler ([00])
                buffer.get();
            }
            // next 10 bytes are reserved (all [00])
            buffer.position(buffer.position() + 10);

            if ((serverCapabilities & CLIENT_SECURE_CONNECTION) != 0) {
                String seedPart2;
                StringBuilder newSeed;
                // read string[$len] auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
                if (authPluginDataLength > 0) {
                    // TODO: disabled the following check for further clarification
                    //         			if (this.authPluginDataLength < 21) {
                    //                      forceClose();
                    //                      throw SQLError.createSQLException(Messages.getString("MysqlIO.103"), 
                    //                          SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
                    //         			}
                    seedPart2 = readASCIIString(buffer, authPluginDataLength - 8); //buf.readString("ASCII", getExceptionInterceptor(), this.authPluginDataLength - 8);
                    newSeed = new StringBuilder(authPluginDataLength);
                } else {
                    seedPart2 = readASCIIString(buffer, bytes);
                    newSeed = new StringBuilder(SEED_LENGTH);
                }
                newSeed.append(seed);
                newSeed.append(seedPart2);
                seed = newSeed.toString();
            }
            if (((serverCapabilities & CLIENT_COMPRESS) != 0)) {
                clientParam |= CLIENT_COMPRESS;
            }
            if (this.database != null && !this.database.isEmpty()) {
                clientParam |= CLIENT_CONNECT_WITH_DB;
            }
            if ((serverCapabilities & CLIENT_LONG_FLAG) != 0) {
                // We understand other column flags, as well
                clientParam |= CLIENT_LONG_FLAG;
                hasLongColumnInfo = true;
            }
            clientParam |= CLIENT_LONG_PASSWORD; // for long passwords
            //
            // 4.1 has some differences in the protocol
            //
            boolean has41NewNewProt = false;
            boolean use41Extensions = false;
            if ((serverCapabilities & CLIENT_RESERVED) != 0) {
                if ((serverCapabilities & CLIENT_PROTOCOL_41) != 0) {
                    clientParam |= CLIENT_PROTOCOL_41;
                    has41NewNewProt = true;

                    // Need this to get server status values
                    clientParam |= CLIENT_TRANSACTIONS;

                    // We always allow multiple result sets
                    clientParam |= CLIENT_MULTI_RESULTS;

                    // We allow the user to configure whether
                    // or not they want to support multiple queries
                    // (by default, this is disabled).
                    //if (this.connection.getAllowMultiQueries()) {
                    clientParam |= CLIENT_MULTI_STATEMENTS;
                    //}
                } else {
                    clientParam |= CLIENT_RESERVED;
                    has41NewNewProt = false;
                }

                use41Extensions = true;
            }

            int passwordLength = 16;
            int userLength = (this.username != null) ? this.username.length() : 0;
            int databaseLength = (this.database != null) ? database.length() : 0;

            int packLength = ((userLength + passwordLength + databaseLength) * 3) + 7 + HEADER_LENGTH + AUTH_411_OVERHEAD;
            boolean ssl = false;
            if (!ssl) {
                if ((serverCapabilities & CLIENT_SECURE_CONNECTION) != 0) {
                    clientParam |= CLIENT_SECURE_CONNECTION;
                    //secureAuth411(null, packLength, username, password, database, true, false);
                    { //start secureAuth411
                        buffer.clear();
                        if (use41Extensions) {
                            buffer.putLong(clientParam);
                            buffer.putLong(255 * 255 * 255);
                            { //appendCharsetByteForHandshake
                                int charsetIndex = CharsetMapping.getCollationIndexForJavaEncoding(this.encoding == null || this.encoding.isEmpty() ? "UTF-8" : this.encoding);
                                if (charsetIndex == 0) charsetIndex = CharsetMapping.MYSQL_COLLATION_INDEX_utf8;
                                buffer.put((byte) charsetIndex);
                                System.out.println("charsetIndex = " + charsetIndex);
                            }
                            // Set of bytes reserved for future use.
                            buffer.put(new byte[23]);

                        } else {
                            buffer.putInt((int) clientParam);
                            buffer.putInt(255 * 255 * 255);
                        }

                        {  // User/Password data
                            MySQLs.writeUTF8String(buffer, this.username);
                            if (this.password.length() != 0) {
                                buffer.put((byte) 0x14);
                                try {
                                    buffer.put(scramble411(password, seed, this.encoding));
                                } catch (Exception se) {
                                    bufferPool.accept(buffer);
                                    conn.dispose();
                                    future.completeExceptionally(se);
                                    return;
                                }
                            } else {
                                /* For empty password */
                                buffer.put((byte) 0);
                            }
                        }
                        if (databaseLength > 0) {
                            MySQLs.writeUTF8String(buffer, this.database);
                        }
                        buffer.flip();
                        try {
                            conn.write(buffer);
                            System.out.println("----------------发送完成: " + buffer.remaining());
                            buffer.clear();
                            conn.read(buffer);
                            System.out.println("----------------读取完成: " + buffer.position());
                            buffer.flip();
                            try {
                                checkErrorPacket(buffer, bytes);
                            } catch (Exception se) {
                                bufferPool.accept(buffer);
                                conn.dispose();
                                future.completeExceptionally(se);
                                se.printStackTrace();
                                return;
                            }
                            bufferPool.accept(buffer);
                            future.complete(conn);
                            if (true) return;
                        } catch (Exception ee) {
                            bufferPool.accept(buffer);
                            conn.dispose();
                            future.completeExceptionally(ee);
                            ee.printStackTrace();
                            return;
                        }
                    }
                    //end secureAuth411
                } else {
                    // Passwords can be 16 chars long
                    bufferPool.accept(buffer);
                    conn.dispose();
                    future.completeExceptionally(new SQLException("mysql connect error"));
                    return;
                }
            } else {
                bufferPool.accept(buffer);
                conn.dispose();
                future.completeExceptionally(new SQLException("not supported ssl connect mysql"));
                return;
            }

            System.out.println("(serverCapabilities & CLIENT_PROTOCOL_41) = " + (serverCapabilities & CLIENT_PROTOCOL_41));
            System.out.println("(serverCapabilities & CLIENT_SECURE_CONNECTION) = " + (serverCapabilities & CLIENT_SECURE_CONNECTION));

            System.out.println("protocolVersion = " + protocolVersion);
            System.out.println("serverVersion = " + serverVersion);
            System.out.println("threadId = " + threadId);
            System.out.println("seed = " + seed);
            System.out.println("has41NewNewProt = " + has41NewNewProt);
            System.out.println("use41Extensions = " + use41Extensions);
            System.out.println("authPluginDataLength = " + authPluginDataLength);
            System.out.println("serverCapabilities = 0x" + Long.toHexString(serverCapabilities));
            bufferPool.accept(buffer);
            conn.dispose();
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
        conn.dispose();
        future.completeExceptionally(new SQLException("mysql connect resp error"));
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
