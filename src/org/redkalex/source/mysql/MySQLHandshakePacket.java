/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.sql.SQLException;

/**
 *
 * @author zhangjx
 */
public class MySQLHandshakePacket extends MySQLPacket {

    private static final byte[] FILLER_13 = new byte[13];

    public byte protocolVersion;

    public String serverVersion;

    public long threadId;

    public byte[] seed;

    public int serverCapabilities;

    public int serverCharsetIndex;

    public int serverStatus;

    public byte[] seed2;

    public MySQLHandshakePacket(ByteBuffer buffer, byte[] array) throws SQLException {
        packetLength = MySQLs.readUB3(buffer);
        packetId = buffer.get();
        protocolVersion = buffer.get();
        if (protocolVersion < 10) {
            throw new SQLException("Not supported protocolVersion(" + protocolVersion + "), must greaterthan 10");
        }
        serverVersion = MySQLs.readASCIIString(buffer, array);
        if (Integer.parseInt(serverVersion.substring(0, serverVersion.indexOf('.'))) < 8) {
            throw new SQLException("Not supported serverVersion(" + serverVersion + "), must greaterthan 8.0");
        }
        threadId = MySQLs.readUB4(buffer);
        seed = MySQLs.readBytes(buffer, array);
        serverCapabilities = buffer.hasRemaining() ? MySQLs.readUB2(buffer) : 0;
        serverCharsetIndex = buffer.get() & 0xff;
        serverStatus = MySQLs.readUB2(buffer);
        buffer.get(FILLER_13); //预留字节
        seed2 = MySQLs.readBytes(buffer, array);
    }
}
