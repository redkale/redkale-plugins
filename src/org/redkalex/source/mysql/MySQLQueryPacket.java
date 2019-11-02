/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.util.ByteBufferWriter;

/**
 *
 * @author zhangjx
 */
public class MySQLQueryPacket extends MySQLPacket {

    public static final byte COM_QUERY = 3;

    public byte[] message;

    public MySQLQueryPacket(String sql) {
        this.message = sql.getBytes();
        this.packetIndex = 0;
    }

    public ByteBufferWriter writeTo(ByteBufferWriter buffer) {
        int size = calcPacketSize();
        MySQLs.writeUB3(buffer, size);
        buffer.put(packetIndex);
        buffer.put(COM_QUERY);
        buffer.put(message);
        return buffer;
    }

    protected int calcPacketSize() {
        int size = 1;
        if (message != null) {
            size += message.length;
        }
        return size;
    }
}
