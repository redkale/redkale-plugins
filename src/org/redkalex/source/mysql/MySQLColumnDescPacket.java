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

    public byte[] catalog = DEFAULT_CATALOG;// always "def"

    public byte[] schema;

    public byte[] table;

    public byte[] orgTable;

    public byte[] name;

    public byte[] orgName;

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
        this.catalog = MySQLs.readBytesWithLength(buffer);
        this.schema = MySQLs.readBytesWithLength(buffer);
        this.table = MySQLs.readBytesWithLength(buffer);
        this.orgTable = MySQLs.readBytesWithLength(buffer);
        this.name = MySQLs.readBytesWithLength(buffer);
        this.orgName = MySQLs.readBytesWithLength(buffer);
        this.nextLength = buffer.get();
        this.charsetSet = MySQLs.readUB2(buffer);
        this.length = MySQLs.readUB4(buffer);
        this.type = buffer.get() & 0xff;
        this.flags = MySQLs.readUB2(buffer);
        this.decimals = buffer.get();
        this.filler = MySQLs.readBytes(buffer, array, 2);
        if (buffer.hasRemaining()) {
            this.defaultValues = MySQLs.readBytesWithLength(buffer);
        }
    }
}
