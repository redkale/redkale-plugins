/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import org.redkale.util.ByteBufferWriter;

/**
 *
 * @author zhangjx
 */
public class MyQueryPacket extends MyPacket {

    public byte[] message;

    public MyQueryPacket(byte[] sqlBytes) {
        this.message = sqlBytes;
        this.packetIndex = 0;
    }

    public ByteBufferWriter writeTo(ByteBufferWriter writer) {
        int size = calcPacketSize();
        Mysqls.writeUB3(writer, size);
        writer.put(packetIndex);
        writer.put(COM_QUERY);
        writer.put(message);
        return writer;
    }

    public ByteBuffer writeTo(ByteBuffer buffer) {
        int size = calcPacketSize();
        Mysqls.writeUB3(buffer, size);
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
