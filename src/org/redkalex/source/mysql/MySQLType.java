/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

/**
 *
 * @author zhangjx
 */
public class MySQLType {

    // Protocol field type numbers
    public static final int FIELD_TYPE_DECIMAL = 0;

    public static final int FIELD_TYPE_TINY = 1;

    public static final int FIELD_TYPE_SHORT = 2;

    public static final int FIELD_TYPE_LONG = 3;

    public static final int FIELD_TYPE_FLOAT = 4;

    public static final int FIELD_TYPE_DOUBLE = 5;

    public static final int FIELD_TYPE_NULL = 6;

    public static final int FIELD_TYPE_TIMESTAMP = 7;

    public static final int FIELD_TYPE_LONGLONG = 8;

    public static final int FIELD_TYPE_INT24 = 9;

    public static final int FIELD_TYPE_DATE = 10;

    public static final int FIELD_TYPE_TIME = 11;

    public static final int FIELD_TYPE_DATETIME = 12;

    public static final int FIELD_TYPE_YEAR = 13;

    public static final int FIELD_TYPE_VARCHAR = 15;

    public static final int FIELD_TYPE_BIT = 16;

    public static final int FIELD_TYPE_JSON = 245;

    public static final int FIELD_TYPE_NEWDECIMAL = 246;

    public static final int FIELD_TYPE_ENUM = 247;

    public static final int FIELD_TYPE_SET = 248;

    public static final int FIELD_TYPE_TINY_BLOB = 249;

    public static final int FIELD_TYPE_MEDIUM_BLOB = 250;

    public static final int FIELD_TYPE_LONG_BLOB = 251;

    public static final int FIELD_TYPE_BLOB = 252;

    public static final int FIELD_TYPE_VAR_STRING = 253;

    public static final int FIELD_TYPE_STRING = 254;

    public static final int FIELD_TYPE_GEOMETRY = 255;

    public static int getBinaryEncodedLength(int type) {
        switch (type) {
            case MySQLType.FIELD_TYPE_TINY:
                return 1;
            case MySQLType.FIELD_TYPE_SHORT:
            case MySQLType.FIELD_TYPE_YEAR:
                return 2;
            case MySQLType.FIELD_TYPE_LONG:
            case MySQLType.FIELD_TYPE_INT24:
            case MySQLType.FIELD_TYPE_FLOAT:
                return 4;
            case MySQLType.FIELD_TYPE_LONGLONG:
            case MySQLType.FIELD_TYPE_DOUBLE:
                return 8;
            case MySQLType.FIELD_TYPE_TIME:
            case MySQLType.FIELD_TYPE_DATE:
            case MySQLType.FIELD_TYPE_DATETIME:
            case MySQLType.FIELD_TYPE_TIMESTAMP:
            case MySQLType.FIELD_TYPE_TINY_BLOB:
            case MySQLType.FIELD_TYPE_MEDIUM_BLOB:
            case MySQLType.FIELD_TYPE_LONG_BLOB:
            case MySQLType.FIELD_TYPE_BLOB:
            case MySQLType.FIELD_TYPE_VAR_STRING:
            case MySQLType.FIELD_TYPE_VARCHAR:
            case MySQLType.FIELD_TYPE_STRING:
            case MySQLType.FIELD_TYPE_DECIMAL:
            case MySQLType.FIELD_TYPE_NEWDECIMAL:
            case MySQLType.FIELD_TYPE_GEOMETRY:
            case MySQLType.FIELD_TYPE_BIT:
            case MySQLType.FIELD_TYPE_JSON:
            case MySQLType.FIELD_TYPE_NULL:
                return 0;
        }
        return -1; // unknown type
    }
}
