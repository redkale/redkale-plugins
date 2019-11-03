/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.util.ByteBufferReader;

/**
 *
 * @author zhangjx
 */
public class MySQLColumnDescPacket extends MySQLPacket {

    private static final byte[] DEFAULT_CATALOG = "def".getBytes();

    private static final byte NEXT_LENGTH = 0x0c;

    private static final byte[] FILLER = {00, 00};

    public byte[] def = DEFAULT_CATALOG;// always "def"

    public byte[] catalog;

    public byte[] tableLabel;

    public byte[] tableName;

    public String columnLabel;

    public String columnName;

    public byte nextLength = NEXT_LENGTH;// always 0x0c

    public int charsetSet;

    public long length;

    public int type;

    public int flags;

    public byte decimals;

    public byte[] filler = FILLER;

    public byte[] defaultValues;

    public MySQLColumnDescPacket(ByteBufferReader buffer, byte[] array) {
        this.packetLength = MySQLs.readUB3(buffer);
        this.packetIndex = buffer.get();
        this.def = MySQLs.readBytesWithLength(buffer);
        this.catalog = MySQLs.readBytesWithLength(buffer);
        this.tableLabel = MySQLs.readBytesWithLength(buffer);
        this.tableName = MySQLs.readBytesWithLength(buffer);
        this.columnLabel = new String(MySQLs.readBytesWithLength(buffer));
        this.columnName = new String(MySQLs.readBytesWithLength(buffer));
        int nextLength = buffer.get() & 0xff;
        this.charsetSet = MySQLs.readUB2(buffer);
        this.length = MySQLs.readUB4(buffer);
        this.type = buffer.get() & 0xff;
        this.flags = MySQLs.readUB2(buffer);
        this.decimals = buffer.get();
        this.filler = MySQLs.readBytesWithLength(buffer);
        if (buffer.hasRemaining()) {
            this.defaultValues = MySQLs.readBytesWithLength(buffer);
        }
    }
}
