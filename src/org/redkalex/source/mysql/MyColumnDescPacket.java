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
public class MyColumnDescPacket extends MyPacket {

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

    public MyColumnDescPacket(ByteBufferReader reader, byte[] array) {
        this.packetLength = Mysqls.readUB3(reader);
        this.packetIndex = reader.get();
        this.def = Mysqls.readBytesWithLength(reader);
        this.catalog = Mysqls.readBytesWithLength(reader);
        this.tableLabel = Mysqls.readBytesWithLength(reader);
        this.tableName = Mysqls.readBytesWithLength(reader);
        this.columnLabel = new String(Mysqls.readBytesWithLength(reader));
        this.columnName = new String(Mysqls.readBytesWithLength(reader));
        int nextLength = reader.get() & 0xff;
        this.charsetSet = Mysqls.readUB2(reader);
        this.length = Mysqls.readUB4(reader);
        this.type = reader.get() & 0xff;
        this.flags = Mysqls.readUB2(reader);
        this.decimals = reader.get();
        this.filler = Mysqls.readBytesWithLength(reader);
        if (reader.hasRemaining()) {
            this.defaultValues = Mysqls.readBytesWithLength(reader);
        }
    }
}
