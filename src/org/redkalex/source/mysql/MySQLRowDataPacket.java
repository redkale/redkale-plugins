/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import org.redkale.util.ByteBufferReader;

/**
 *
 * @author zhangjx
 */
public class MySQLRowDataPacket extends MySQLPacket {

    public final MySQLColumnDescPacket[] columns;

    public int columnCount;

    public final byte[][] values;

    public MySQLRowDataPacket(MySQLColumnDescPacket[] columns, int len, ByteBufferReader buffer, int columnCount, byte[] array) {
        this.columns = columns;
        this.columnCount = columnCount;
        this.packetLength = len;
        this.packetIndex = buffer.get();
        this.values = new byte[columnCount][];
        for (int i = 0; i < columnCount; i++) {
            this.values[i] = MySQLs.readBytesWithLength(buffer);
        }
    }

    public byte[] getValue(int i) {
        return values[i];
    }

    public Serializable getObject(int i) {
        byte[] bs = values[i];
        //MySQLColumnDesc colDesc = rowDesc.getColumn(i);
        if (bs == null) return null;
        //return colDesc.getObject(bs);
        return null;
    }

    public int length() {
        if (values == null) return -1;
        return values.length;
    }

    @Override
    public String toString() {
        return "{cols:" + (values == null ? -1 : values.length) + "}";
    }
}
