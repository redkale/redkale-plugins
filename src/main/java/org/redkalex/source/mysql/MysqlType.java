/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.*;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class MysqlType {

    // Protocol field flag numbers
    public static final int FIELD_FLAG_NOT_NULL = 1;

    public static final int FIELD_FLAG_PRIMARY_KEY = 2;

    public static final int FIELD_FLAG_UNIQUE_KEY = 4;

    public static final int FIELD_FLAG_MULTIPLE_KEY = 8;

    public static final int FIELD_FLAG_BLOB = 16;

    public static final int FIELD_FLAG_UNSIGNED = 32;

    public static final int FIELD_FLAG_ZEROFILL = 64;

    public static final int FIELD_FLAG_BINARY = 128;

    public static final int FIELD_FLAG_AUTO_INCREMENT = 512;

    private static final boolean IS_DECIMAL = true;

    private static final boolean IS_NOT_DECIMAL = false;

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

    /*
     * https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
     */
    public static final class ColumnFlags {

        public static final int NOT_NULL_FLAG = 0x00000001;

        public static final int PRI_KEY_FLAG = 0x00000002;

        public static final int UNIQUE_KEY_FLAG = 0x00000004;

        public static final int MULTIPLE_KEY_FLAG = 0x00000008;

        public static final int BLOB_FLAG = 0x00000010;

        public static final int UNSIGNED_FLAG = 0x00000020;

        public static final int ZEROFILL_FLAG = 0x00000040;

        public static final int BINARY_FLAG = 0x00000080;

        public static final int ENUM_FLAG = 0x00000100;

        public static final int AUTO_INCREMENT_FLAG = 0x00000200;

        public static final int TIMESTAMP_FLAG = 0x00000400;

        public static final int SET_FLAG = 0x00000800;

        public static final int NO_DEFAULT_VALUE_FLAG = 0x00001000;

        public static final int ON_UPDATE_NOW_FLAG = 0x00002000;

        public static final int NUM_FLAG = 0x00008000;

        public static final int PART_KEY_FLAG = 0x00004000;

        public static final int GROUP_FLAG = 0x00008000;

        public static final int UNIQUE_FLAG = 0x00010000;

        public static final int BINCMP_FLAG = 0x00020000;

        public static final int GET_FIXED_FIELDS_FLAG = 0x00040000;

        public static final int FIELD_IN_PART_FUNC_FLAG = 0x00080000;

        public static final int FIELD_IN_ADD_INDEX = 0x00100000;

        public static final int FIELD_IS_RENAMED = 0x00200000;

        public static final int FIELD_FLAGS_STORAGE_MEDIA = 22;

        public static final int FIELD_FLAGS_STORAGE_MEDIA_MASK = 3 << FIELD_FLAGS_STORAGE_MEDIA;

        public static final int FIELD_FLAGS_COLUMN_FORMAT = 24;

        public static final int FIELD_FLAGS_COLUMN_FORMAT_MASK = 3 << FIELD_FLAGS_COLUMN_FORMAT;

        public static final int FIELD_IS_DROPPED = 0x04000000;

        public static final int EXPLICIT_NULL_FLAG = 0x08000000;

        public static final int FIELD_IS_MARKED = 0x10000000;
    }

    public static int getBinaryEncodedLength(int type) {
        switch (type) {
            case FIELD_TYPE_TINY:
                return 1;
            case FIELD_TYPE_SHORT:
            case FIELD_TYPE_YEAR:
                return 2;
            case FIELD_TYPE_LONG:
            case FIELD_TYPE_INT24:
            case FIELD_TYPE_FLOAT:
                return 4;
            case FIELD_TYPE_LONGLONG:
            case FIELD_TYPE_DOUBLE:
                return 8;
            case FIELD_TYPE_TIME:
            case FIELD_TYPE_DATE:
            case FIELD_TYPE_DATETIME:
            case FIELD_TYPE_TIMESTAMP:
            case FIELD_TYPE_TINY_BLOB:
            case FIELD_TYPE_MEDIUM_BLOB:
            case FIELD_TYPE_LONG_BLOB:
            case FIELD_TYPE_BLOB:
            case FIELD_TYPE_VAR_STRING:
            case FIELD_TYPE_VARCHAR:
            case FIELD_TYPE_STRING:
            case FIELD_TYPE_DECIMAL:
            case FIELD_TYPE_NEWDECIMAL:
            case FIELD_TYPE_GEOMETRY:
            case FIELD_TYPE_BIT:
            case FIELD_TYPE_JSON:
            case FIELD_TYPE_NULL:
                return 0;
        }
        return -1; // unknown type
    }

    public static int getTypeFromObject(Object value) {
        if (value == null) {
            // ProtocolBinary::MYSQL_TYPE_NULL
            return FIELD_TYPE_NULL;
        } else if (value instanceof Byte) {
            // ProtocolBinary::MYSQL_TYPE_TINY
            return FIELD_TYPE_TINY;
        } else if (value instanceof Boolean) {
            // ProtocolBinary::MYSQL_TYPE_TINY
            return FIELD_TYPE_TINY;
        } else if (value instanceof Short) {
            // ProtocolBinary::MYSQL_TYPE_SHORT, ProtocolBinary::MYSQL_TYPE_YEAR
            return FIELD_TYPE_SHORT;
        } else if (value instanceof Integer) {
            // ProtocolBinary::MYSQL_TYPE_LONG, ProtocolBinary::MYSQL_TYPE_INT24
            return FIELD_TYPE_LONG;
        } else if (value instanceof Long) {
            // ProtocolBinary::MYSQL_TYPE_LONGLONG
            return FIELD_TYPE_LONGLONG;
        } else if (value instanceof Float) {
            // ProtocolBinary::MYSQL_TYPE_FLOAT
            return FIELD_TYPE_FLOAT;
        } else if (value instanceof Double) {
            // ProtocolBinary::MYSQL_TYPE_DOUBLE
            return FIELD_TYPE_DOUBLE;
        } else if (value instanceof byte[]) {
            // ProtocolBinary::MYSQL_TYPE_LONG_BLOB, ProtocolBinary::MYSQL_TYPE_MEDIUM_BLOB, ProtocolBinary::MYSQL_TYPE_BLOB, ProtocolBinary::MYSQL_TYPE_TINY_BLOB
            return FIELD_TYPE_BLOB;
        } else if (value instanceof LocalDate) {
            // ProtocolBinary::MYSQL_TYPE_DATE
            return FIELD_TYPE_DATE;
        } else if (value instanceof Duration || value instanceof LocalTime) {
            // ProtocolBinary::MYSQL_TYPE_TIME
            return FIELD_TYPE_TIME;
        } else if (value instanceof LocalDateTime) {
            // ProtocolBinary::MYSQL_TYPE_DATETIME, ProtocolBinary::MYSQL_TYPE_TIMESTAMP
            return FIELD_TYPE_DATETIME;
        } else {
            /*
             * ProtocolBinary::MYSQL_TYPE_STRING, ProtocolBinary::MYSQL_TYPE_VARCHAR, ProtocolBinary::MYSQL_TYPE_VAR_STRING,
             * ProtocolBinary::MYSQL_TYPE_ENUM, ProtocolBinary::MYSQL_TYPE_SET, ProtocolBinary::MYSQL_TYPE_GEOMETRY,
             * ProtocolBinary::MYSQL_TYPE_BIT, ProtocolBinary::MYSQL_TYPE_DECIMAL, ProtocolBinary::MYSQL_TYPE_NEWDECIMAL
             */
            return FIELD_TYPE_STRING;
        }
    }

    public static void writePrepareParam(ByteArray array, int type, Object param) {
        if (type == FIELD_TYPE_TINY) {
            array.putByte(param instanceof Boolean ? ((Boolean) param ? 1 : 0) : ((Number) param).byteValue());
        } else if (type == FIELD_TYPE_SHORT) {
            Mysqls.writeUB2(array, ((Number) param).shortValue());
        } else if (type == FIELD_TYPE_LONG) {
            Mysqls.writeInt(array, ((Number) param).intValue());
        } else if (type == FIELD_TYPE_LONGLONG) {
            Mysqls.writeLong(array, ((Number) param).longValue());
        } else if (type == FIELD_TYPE_FLOAT) {
            Mysqls.writeFloat(array, ((Number) param).floatValue());
        } else if (type == FIELD_TYPE_DOUBLE) {
            Mysqls.writeDouble(array, ((Number) param).doubleValue());
        } else if (type == FIELD_TYPE_STRING || type == FIELD_TYPE_VAR_STRING || type == FIELD_TYPE_VARCHAR) {
            Mysqls.writeWithLength(array, param.toString().getBytes(StandardCharsets.UTF_8));
        } else if (type == FIELD_TYPE_BLOB) {
            Mysqls.writeWithLength(array, (byte[]) param);
        } else if (type == FIELD_TYPE_DATE) {
            LocalDate value = (LocalDate) param;
            array.putByte(4);
            Mysqls.writeUB2(array, value.getYear());
            array.putByte(value.getMonthValue());
            array.putByte(value.getDayOfMonth());
        } else if (type == FIELD_TYPE_TIME) {
            if (param instanceof Duration) {
                Duration value = (Duration) param;
                long secondsOfDuration = value.getSeconds();
                int nanosOfDuration = value.getNano();
                if (secondsOfDuration == 0 && nanosOfDuration == 0) {
                    array.putByte(0);
                    return;
                }
                byte isNegative = 0;
                if (secondsOfDuration < 0) {
                    isNegative = 1;
                    secondsOfDuration = -secondsOfDuration;
                }

                int days = (int) (secondsOfDuration / 86400);
                int secondsOfADay = (int) (secondsOfDuration % 86400);
                int hour = secondsOfADay / 3600;
                int minute = ((secondsOfADay % 3600) / 60);
                int second = secondsOfADay % 60;

                if (nanosOfDuration == 0) {
                    array.putByte(8);
                    array.putByte(isNegative);
                    Mysqls.writeInt(array, days);
                    array.putByte(hour);
                    array.putByte(minute);
                    array.putByte(second);
                    return;
                }
                int microSecond;
                if (isNegative == 1 && nanosOfDuration > 0) {
                    second = second - 1;
                    microSecond = (1000_000_000 - nanosOfDuration) / 1000;
                } else {
                    microSecond = nanosOfDuration / 1000;
                }
                array.putByte(12);
                array.putByte(isNegative);
                Mysqls.writeInt(array, days);
                array.putByte(hour);
                array.putByte(minute);
                array.putByte(second);
                Mysqls.writeInt(array, microSecond);
            } else {
                LocalTime value = (LocalTime) param;
                array.putByte(4);
                int hour = value.getHour();
                int minute = value.getMinute();
                int second = value.getSecond();
                int nano = value.getNano();
                if (nano == 0) {
                    if (hour == 0 && minute == 0 && second == 0) {
                        array.putByte(0);
                    } else {
                        array.putByte(8);
                        array.putByte(0);
                        Mysqls.writeInt(array, 0);
                        array.putByte(hour);
                        array.putByte(minute);
                        array.putByte(second);
                    }
                } else {
                    int microSecond = nano / 1000;
                    array.putByte(12);
                    array.putByte(0);
                    Mysqls.writeInt(array, 0);
                    array.putByte(hour);
                    array.putByte(minute);
                    array.putByte(second);
                    Mysqls.writeInt(array, microSecond);
                }
            }
        } else if (type == FIELD_TYPE_DATETIME) {
            LocalDateTime value = (LocalDateTime) param;
            int year = value.getYear();
            int month = value.getMonthValue();
            int day = value.getDayOfMonth();
            int hour = value.getHour();
            int minute = value.getMinute();
            int second = value.getSecond();
            int microsecond = value.getNano() / 1000;
            // LocalDateTime does not have a zero value of month or day
            if (hour == 0 && minute == 0 && second == 0 && microsecond == 0) {
                array.putByte(4);
                Mysqls.writeUB2(array, year);
                array.putByte(month);
                array.putByte(day);
            } else if (microsecond == 0) {
                array.putByte(7);
                Mysqls.writeUB2(array, year);
                array.putByte(month);
                array.putByte(day);
                array.putByte(hour);
                array.putByte(minute);
                array.putByte(second);
            } else {
                array.putByte(11);
                Mysqls.writeUB2(array, year);
                array.putByte(month);
                array.putByte(day);
                array.putByte(hour);
                array.putByte(minute);
                array.putByte(second);
                Mysqls.writeInt(array, microsecond);
            }
        }
    }

    public static <T> byte[] formatPrepareParam(EntityInfo<T> info, Attribute<T, Serializable> attr, Object param) {
        if (param == null && info.isNotNullJson(attr)) return new byte[0];
        if (param == null) return null;
        if (param instanceof CharSequence) {
            return param.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (param instanceof Boolean) {
            return (Boolean) param ? new byte[]{0x31} : new byte[]{0x30};
        }
        if (param instanceof byte[]) {
            return (byte[]) param;
        }
        if (param instanceof java.sql.Blob) {
            java.sql.Blob blob = (java.sql.Blob) param;
            try {
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new SourceException(e);
            }
        }
        if (!(param instanceof Number) && !(param instanceof CharSequence) && !(param instanceof java.util.Date)
            && !param.getClass().getName().startsWith("java.sql.") && !param.getClass().getName().startsWith("java.time.")) {
            if (attr == null) return info.getJsonConvert().convertTo(param).getBytes(StandardCharsets.UTF_8);
            return info.getJsonConvert().convertTo(attr.genericType(), param).getBytes(StandardCharsets.UTF_8);
        }
        return String.valueOf(param).getBytes(StandardCharsets.UTF_8);
    }
}
