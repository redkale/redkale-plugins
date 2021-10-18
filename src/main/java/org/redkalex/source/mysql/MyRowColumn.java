/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import static org.redkalex.source.mysql.MysqlType.*;

/**
 *
 * @author zhangjx
 */
public class MyRowColumn {

    public byte[] catalog;

    public byte[] schema;

    public byte[] tableLabel; //AS表名

    public byte[] tableName; //原始表名

    public String columnLabel;  //AS字段名

    public String columnName;  //原始字段名

    public int charsetSet;

    public long length;

    public int type;

    public int flags;

    public byte decimals;

    public byte[] defaultValues;

    public boolean binary;

    public boolean unsign;

    public Serializable getObject(byte[] bs) {
        if (bs == null) return null;
        final int mytype = type;
        if (mytype == FIELD_TYPE_BIT) {
            return bs[0];
        } else if (mytype == FIELD_TYPE_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == FIELD_TYPE_DATE) {
            return java.sql.Date.valueOf(new String(bs));
        } else if (mytype == FIELD_TYPE_DATETIME) {
            return java.sql.Timestamp.valueOf(new String(bs));
        } else if (mytype == FIELD_TYPE_DECIMAL) {
            throw new UnsupportedOperationException("FIELD_TYPE_DECIMAL not supported yet.");
        } else if (mytype == FIELD_TYPE_DOUBLE) {
            return Double.parseDouble(new String(bs));
        } else if (mytype == FIELD_TYPE_ENUM) {
            throw new UnsupportedOperationException("FIELD_TYPE_ENUM not supported yet.");
        } else if (mytype == FIELD_TYPE_FLOAT) {
            return Float.parseFloat(new String(bs));
        } else if (mytype == FIELD_TYPE_GEOMETRY) {
            throw new UnsupportedOperationException("FIELD_TYPE_GEOMETRY not supported yet.");
        } else if (mytype == FIELD_TYPE_INT24) {
            return Integer.parseInt(new String(bs));
        } else if (mytype == FIELD_TYPE_JSON) {
            throw new UnsupportedOperationException("FIELD_TYPE_JSON not supported yet.");
        } else if (mytype == FIELD_TYPE_LONG) {
            return Long.parseLong(new String(bs));
        } else if (mytype == FIELD_TYPE_LONGLONG) {
            return Long.parseLong(new String(bs));
        } else if (mytype == FIELD_TYPE_LONG_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == FIELD_TYPE_MEDIUM_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == FIELD_TYPE_NEWDECIMAL) {
            return Double.parseDouble(new String(bs));
        } else if (mytype == FIELD_TYPE_NULL) {
            return null;
        } else if (mytype == FIELD_TYPE_SET) {
            throw new UnsupportedOperationException("FIELD_TYPE_SET not supported yet.");
        } else if (mytype == FIELD_TYPE_SHORT) {
            return Short.parseShort(new String(bs));
        } else if (mytype == FIELD_TYPE_STRING || mytype == FIELD_TYPE_VAR_STRING || mytype == FIELD_TYPE_VARCHAR) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == FIELD_TYPE_TIME) {
            return java.sql.Time.valueOf(new String(bs));
        } else if (mytype == FIELD_TYPE_TIMESTAMP) {
            return java.sql.Timestamp.valueOf(new String(bs));
        } else if (mytype == FIELD_TYPE_TINY) {
            return bs.length == 1 && "1".equals(new String(bs));
        } else if (mytype == FIELD_TYPE_TINY_BLOB) {
            return binary ? bs : new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == FIELD_TYPE_VARCHAR) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == FIELD_TYPE_VAR_STRING) {
            return new String(bs, StandardCharsets.UTF_8);
        } else if (mytype == FIELD_TYPE_YEAR) {
            throw new UnsupportedOperationException("FIELD_TYPE_JSON not supported yet.");
        }
        return null;
    }

    @Override
    public String toString() {
        return MyRowColumn.class.getSimpleName()
            + "{catalog=" + format(catalog)
            + ", schema=" + format(schema)
            + ", tableLabel=" + format(tableLabel)
            + ", tableName=" + format(tableName)
            + ", columnLabel=" + columnLabel
            + ", columnName=" + columnName
            + ", charsetSet=" + charsetSet
            + ", flags=0x" + Integer.toHexString(flags)
            + ", type=" + type
            + ", length=" + length
            + ", defaultValues=" + format(defaultValues)
            + "}";
    }

    private static String format(byte[] bs) {
        return bs == null ? null : new String(bs, StandardCharsets.UTF_8);
    }
}
