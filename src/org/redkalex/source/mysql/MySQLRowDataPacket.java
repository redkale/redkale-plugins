/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
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
        if (bs == null) return null;
        final int mytype = columns[i].type;
        if (mytype == MySQLType.FIELD_TYPE_BIT) {
            return bs[0];
        } else if (mytype == MySQLType.FIELD_TYPE_BLOB) {
            return bs;
        } else if (mytype == MySQLType.FIELD_TYPE_DATE) {
            throw new UnsupportedOperationException("FIELD_TYPE_DATE not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_DATETIME) {
            throw new UnsupportedOperationException("FIELD_TYPE_DATETIME not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_DECIMAL) {
            throw new UnsupportedOperationException("FIELD_TYPE_DECIMAL not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_DOUBLE) {
            return Double.parseDouble(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_ENUM) {
            throw new UnsupportedOperationException("FIELD_TYPE_ENUM not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_FLOAT) {
            return Float.parseFloat(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_GEOMETRY) {
            throw new UnsupportedOperationException("FIELD_TYPE_GEOMETRY not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_INT24) {
            return Integer.parseInt(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_JSON) {
            throw new UnsupportedOperationException("FIELD_TYPE_JSON not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_LONG) {
            return Long.parseLong(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_LONGLONG) {
            return Long.parseLong(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_LONG_BLOB) {
            return bs;
        } else if (mytype == MySQLType.FIELD_TYPE_MEDIUM_BLOB) {
            return bs;
        } else if (mytype == MySQLType.FIELD_TYPE_NEWDECIMAL) {
            throw new UnsupportedOperationException("FIELD_TYPE_NEWDECIMAL not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_NULL) {
            return null;
        } else if (mytype == MySQLType.FIELD_TYPE_SET) {
            throw new UnsupportedOperationException("FIELD_TYPE_SET not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_SHORT) {
            throw new UnsupportedOperationException("FIELD_TYPE_SHORT not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_STRING) {
            throw new UnsupportedOperationException("FIELD_TYPE_STRING not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_TIME) {
            throw new UnsupportedOperationException("FIELD_TYPE_TIME not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_TIMESTAMP) {
            throw new UnsupportedOperationException("FIELD_TYPE_TIMESTAMP not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_TINY) {
            throw new UnsupportedOperationException("FIELD_TYPE_TINY not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_TINY_BLOB) {
            return bs;
        } else if (mytype == MySQLType.FIELD_TYPE_VARCHAR) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MySQLType.FIELD_TYPE_VAR_STRING) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MySQLType.FIELD_TYPE_YEAR) {
            throw new UnsupportedOperationException("FIELD_TYPE_JSON not supported yet.");
        }
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
