/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class MySQLAuthPacket extends MySQLPacket {

    private static final byte[] FILLER = new byte[23];

    public long clientFlags;

    public long maxPacketSize;

    public int charsetIndex;

    public byte[] extra;

    public String username;

    public byte[] password;

    public String database;

    public MySQLAuthPacket(MySQLHandshakePacket handshakePacket, String username, String password, String database) {
        byte[] seed = Utility.append(handshakePacket.seed, handshakePacket.seed2);
        this.packetId = 1;
        this.clientFlags = getClientCapabilities();
        this.maxPacketSize = 1024 * 1024 * 1024;
        this.username = username;
        this.password = MySQLs.scramble411(password, seed);
        this.database = database;
    }

    public ByteBuffer writeTo(ByteBuffer buffer) {
        MySQLs.writeUB3(buffer, calcPacketSize());
        buffer.put(packetId);
        MySQLs.writeUB4(buffer, clientFlags);
        MySQLs.writeUB4(buffer, maxPacketSize);
        buffer.put((byte) 8);
        buffer.put(FILLER);
        if (username == null || username.isEmpty()) {
            buffer.put((byte) 0);
        } else {
            MySQLs.writeWithNull(buffer, username.getBytes());
        }
        if (password == null) {
            buffer.put((byte) 0);
        } else {
            MySQLs.writeWithLength(buffer, password);
        }
        if (database == null || database.isEmpty()) {
            buffer.put((byte) 0);
        } else {
            MySQLs.writeWithNull(buffer, database.getBytes());
        }
        return buffer;
    }

    private int calcPacketSize() {
        int size = 32;// 4+4+1+23;
        size += (username == null || username.isEmpty()) ? 1 : username.length() + 1;
        size += (password == null) ? 1 : MySQLs.getLength(password);
        size += (database == null || database.isEmpty()) ? 1 : database.length() + 1;
        return size;
    }

    private int getClientCapabilities() {
        int flag = 0;
        flag |= MySQLs.CLIENT_LONG_PASSWORD;
        flag |= MySQLs.CLIENT_FOUND_ROWS;
        flag |= MySQLs.CLIENT_LONG_FLAG;
        flag |= MySQLs.CLIENT_CONNECT_WITH_DB;
        flag |= MySQLs.CLIENT_COMPRESS;
        flag |= MySQLs.CLIENT_LOCAL_FILES;
        flag |= MySQLs.CLIENT_PROTOCOL_41;
        flag |= MySQLs.CLIENT_INTERACTIVE;
        flag |= MySQLs.CLIENT_TRANSACTIONS;
        flag |= MySQLs.CLIENT_SECURE_CONNECTION;
        return flag;
    }
}
