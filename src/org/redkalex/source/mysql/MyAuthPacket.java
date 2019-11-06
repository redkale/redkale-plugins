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
public class MyAuthPacket extends MyPacket {

    private static final byte[] FILLER = new byte[23];

    public int charsetIndex;

    public byte[] extra;

    public String username;

    public byte[] password;

    public String database;

    public MyAuthPacket(MyHandshakePacket handshakePacket, String username, String password, String database) {
        byte[] seed = Utility.append(handshakePacket.seed, handshakePacket.seed2);
        this.packetIndex = 1;
        this.username = username;
        this.password = Mysqls.scramble411(password, seed);
        this.database = database;
    }

    public ByteBuffer writeTo(ByteBuffer buffer) {
        Mysqls.writeUB3(buffer, calcPacketSize());
        buffer.put(packetIndex);
        Mysqls.writeUB4(buffer, getClientCapabilities());
        Mysqls.writeUB4(buffer, Mysqls.MAX_PACKET_SIZE);
        buffer.put((byte) 8);
        buffer.put(FILLER);
        if (username == null || username.isEmpty()) {
            buffer.put((byte) 0);
        } else {
            Mysqls.writeWithNull(buffer, username.getBytes());
        }
        if (password == null) {
            buffer.put((byte) 0);
        } else {
            Mysqls.writeWithLength(buffer, password);
        }
        if (database == null || database.isEmpty()) {
            buffer.put((byte) 0);
        } else {
            Mysqls.writeWithNull(buffer, database.getBytes());
        }
        return buffer;
    }

    protected int calcPacketSize() {
        int size = 32;// 4+4+1+23;
        size += (username == null || username.isEmpty()) ? 1 : username.length() + 1;
        size += (password == null) ? 1 : Mysqls.getLength(password);
        size += (database == null || database.isEmpty()) ? 1 : database.length() + 1;
        return size;
    }

    private int getClientCapabilities() {
        int flag = 0;
        flag |= Mysqls.CLIENT_LONG_PASSWORD;
        flag |= Mysqls.CLIENT_FOUND_ROWS;
        flag |= Mysqls.CLIENT_LONG_FLAG;
        flag |= Mysqls.CLIENT_CONNECT_WITH_DB;
        flag |= Mysqls.CLIENT_ODBC;
        flag |= Mysqls.CLIENT_IGNORE_SPACE;
        flag |= Mysqls.CLIENT_PROTOCOL_41;
        flag |= Mysqls.CLIENT_INTERACTIVE;
        flag |= Mysqls.CLIENT_IGNORE_SIGPIPE;
        flag |= Mysqls.CLIENT_TRANSACTIONS;
        flag |= Mysqls.CLIENT_SECURE_CONNECTION;
        return flag;
    }

}
