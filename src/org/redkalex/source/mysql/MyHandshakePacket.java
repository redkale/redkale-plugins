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
public class MyHandshakePacket extends MyPacket {

    private static final byte[] FILLER_13 = new byte[13];

    public byte protocolVersion;

    public String serverVersion;

    public byte[] authPlugin;

    public long threadId;

    public byte[] seed;

    public int serverCapabilities;

    public int serverCharsetIndex;

    public int serverStatus;

    public byte[] seed2;

    public MyHandshakePacket(ByteBuffer buffer, byte[] array) throws SQLException {
        packetLength = Mysqls.readUB3(buffer);
        packetIndex = buffer.get();
        protocolVersion = buffer.get();
        if (protocolVersion < 10) {
            throw new SQLException("Not supported protocolVersion(" + protocolVersion + "), must greaterthan 10");
        }
        serverVersion = Mysqls.readASCIIString(buffer, array);
        if (Integer.parseInt(serverVersion.substring(0, serverVersion.indexOf('.'))) < 5) {
            throw new SQLException("Not supported serverVersion(" + serverVersion + "), must greaterthan 5.0");
        }
        threadId = Mysqls.readUB4(buffer);
        seed = Mysqls.readBytes(buffer, array);
        serverCapabilities = buffer.hasRemaining() ? Mysqls.readUB2(buffer) : 0;
        serverCharsetIndex = buffer.get() & 0xff;
        serverStatus = Mysqls.readUB2(buffer);
        buffer.get(FILLER_13); //预留字节
        seed2 = Mysqls.readBytes(buffer, array);
        if (buffer.hasRemaining()) {
            authPlugin = Mysqls.readBytes(buffer, array);
        }
    }
}
