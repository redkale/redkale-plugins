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
        MySQLColumnDescPacket coldesc = columns[i];
        final int mytype = coldesc.type;
        final boolean binary = (coldesc.flags & MySQLType.FIELD_FLAG_BINARY) > 0;
        if (mytype == MySQLType.FIELD_TYPE_BIT) {
            return bs[0];
        } else if (mytype == MySQLType.FIELD_TYPE_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MySQLType.FIELD_TYPE_DATE) {
            return java.sql.Date.valueOf(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_DATETIME) {
            return java.sql.Timestamp.valueOf(new String(bs));
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
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MySQLType.FIELD_TYPE_MEDIUM_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MySQLType.FIELD_TYPE_NEWDECIMAL) {
            return Double.parseDouble(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_NULL) {
            return null;
        } else if (mytype == MySQLType.FIELD_TYPE_SET) {
            throw new UnsupportedOperationException("FIELD_TYPE_SET not supported yet.");
        } else if (mytype == MySQLType.FIELD_TYPE_SHORT) {
            return Short.parseShort(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_STRING) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MySQLType.FIELD_TYPE_TIME) {
            return java.sql.Time.valueOf(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_TIMESTAMP) {
            return java.sql.Timestamp.valueOf(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_TINY) {
            return bs.length == 1 && "1".equals(new String(bs));
        } else if (mytype == MySQLType.FIELD_TYPE_TINY_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
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
