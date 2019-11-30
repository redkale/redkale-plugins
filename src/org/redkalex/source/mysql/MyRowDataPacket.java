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
public class MyRowDataPacket extends MyPacket {

    public final MyColumnDescPacket[] columns;

    public int columnCount;

    public final byte[][] values;

    private int currValueIndex = 0;

    private int currValueLength = -2;

    public MyRowDataPacket(MyColumnDescPacket[] columns, int len, int index, ByteBufferReader reader, int columnCount, byte[] array) {
        this.columns = columns;
        this.columnCount = columnCount;
        this.packetLength = len;
        this.packetIndex = index < -999 ? reader.get() : (byte) index;
        this.values = new byte[columnCount][];
    }

    //返回true表示读取完毕，返回false表示ByteBuffer数据不够
    public boolean readColumnValue(ByteBufferReader reader) {
        if (this.currValueIndex > columnCount) return true;
        int start = this.currValueIndex;
        for (int i = start; i < columnCount; i++) {
            if (this.currValueLength == -2) {
                if (!Mysqls.checkLength(reader)) return false;
                this.currValueLength = (int) Mysqls.readLength(reader);
            }
            if (this.currValueLength > -1 && reader.remaining() < this.currValueLength) {
                return false;
            }
            this.values[i] = (this.currValueLength == -1) ? null : Mysqls.readBytes(reader, this.currValueLength);
            this.currValueIndex++;
            this.currValueLength = -2;
        }
        this.currValueIndex++;
        return true;
    }

    public byte[] getValue(int i) {
        return values[i];
    }

    public Serializable getObject(int i) {
        byte[] bs = values[i];
        if (bs == null) return null;
        MyColumnDescPacket coldesc = columns[i];
        final int mytype = coldesc.type;
        final boolean binary = (coldesc.flags & MysqlType.FIELD_FLAG_BINARY) > 0;
        if (mytype == MysqlType.FIELD_TYPE_BIT) {
            return bs[0];
        } else if (mytype == MysqlType.FIELD_TYPE_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MysqlType.FIELD_TYPE_DATE) {
            return java.sql.Date.valueOf(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_DATETIME) {
            return java.sql.Timestamp.valueOf(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_DECIMAL) {
            throw new UnsupportedOperationException("FIELD_TYPE_DECIMAL not supported yet.");
        } else if (mytype == MysqlType.FIELD_TYPE_DOUBLE) {
            return Double.parseDouble(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_ENUM) {
            throw new UnsupportedOperationException("FIELD_TYPE_ENUM not supported yet.");
        } else if (mytype == MysqlType.FIELD_TYPE_FLOAT) {
            return Float.parseFloat(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_GEOMETRY) {
            throw new UnsupportedOperationException("FIELD_TYPE_GEOMETRY not supported yet.");
        } else if (mytype == MysqlType.FIELD_TYPE_INT24) {
            return Integer.parseInt(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_JSON) {
            throw new UnsupportedOperationException("FIELD_TYPE_JSON not supported yet.");
        } else if (mytype == MysqlType.FIELD_TYPE_LONG) {
            return Long.parseLong(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_LONGLONG) {
            return Long.parseLong(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_LONG_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MysqlType.FIELD_TYPE_MEDIUM_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MysqlType.FIELD_TYPE_NEWDECIMAL) {
            return Double.parseDouble(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_NULL) {
            return null;
        } else if (mytype == MysqlType.FIELD_TYPE_SET) {
            throw new UnsupportedOperationException("FIELD_TYPE_SET not supported yet.");
        } else if (mytype == MysqlType.FIELD_TYPE_SHORT) {
            return Short.parseShort(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_STRING) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MysqlType.FIELD_TYPE_TIME) {
            return java.sql.Time.valueOf(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_TIMESTAMP) {
            return java.sql.Timestamp.valueOf(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_TINY) {
            return bs.length == 1 && "1".equals(new String(bs));
        } else if (mytype == MysqlType.FIELD_TYPE_TINY_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MysqlType.FIELD_TYPE_VARCHAR) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MysqlType.FIELD_TYPE_VAR_STRING) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == MysqlType.FIELD_TYPE_YEAR) {
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
