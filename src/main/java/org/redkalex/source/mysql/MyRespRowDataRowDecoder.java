/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.*;
import org.redkale.util.ByteArray;
import static org.redkalex.source.mysql.MysqlType.*;

/**
 *
 * @author zhangjx
 */
public class MyRespRowDataRowDecoder extends MyRespDecoder<MyRowData> {

    public static final MyRespRowDataRowDecoder instance = new MyRespRowDataRowDecoder();

    @Override
    public MyRowData read(MyClientConnection conn, ByteBuffer buffer, int length, byte index, ByteArray array, MyClientRequest request, MyResultSet dataset) {
        boolean binary = dataset != null && dataset.status == MyResultSet.STATUS_EXTEND_ROWDATA;
        int typeid = buffer.get(buffer.position()) & 0xff;
        if (typeid == Mysqls.TYPE_ID_EOF) {
            buffer.get();// 读掉 typeid
            MyRespOK ok = MyRespDecoder.readOKPacket(conn, buffer, length, index, array);
            return null;
        }

        final MyRowColumn[] columns = dataset.rowDesc.columns;
        int size = columns.length;
        if (binary) { // BINARY row decoding
            // 0x00 packet header
            // null_bitmap
            final int startPos = buffer.position();
            int nullBitmapLength = (size + 7 + 2) >> 3;
            int nullBitmapIdx = 1 + buffer.get();
            buffer.position(buffer.position() + nullBitmapLength);
            // values
            Serializable[] values = new Serializable[size];
            for (int i = 0; i < size; i++) {
                int val = i + 2;
                int bytePos = val >> 3;
                int bitPos = val & 7;
                byte mask = (byte) (1 << bitPos);
                byte nullByte = (byte) (buffer.get(startPos + nullBitmapIdx + bytePos) & mask);
                if (nullByte == 0) { // non-null
                    values[i] = getColumnData(columns[i], buffer);
                }
            }
            return new MyRowData(null, values);
        } else {
            byte[][] values = new byte[size][];
            for (int i = 0; i < size; i++) {
                //if (buffer.get(buffer.position()) != Mysqls.NULL_MARK) {
                values[i] = Mysqls.readBytesWithLength(buffer);
                //}
            }
            return new MyRowData(values, null);
        }
    }

    private Serializable getColumnData(MyRowColumn column, ByteBuffer buffer) {
        switch (column.type) {
            case FIELD_TYPE_TINY: return column.unsign ? buffer.get() & 0xff : buffer.get();
            case FIELD_TYPE_SHORT: return column.unsign ? Mysqls.readUB2(buffer) : Mysqls.readShort(buffer);
            case FIELD_TYPE_LONG: return column.unsign ? Mysqls.readUB4(buffer) : Mysqls.readInt(buffer);
            case FIELD_TYPE_LONGLONG: return column.unsign ? Mysqls.readLong(buffer) : Mysqls.readLong(buffer);
            case FIELD_TYPE_FLOAT: return Float.intBitsToFloat(Mysqls.readInt(buffer));
            case FIELD_TYPE_DOUBLE: return Double.longBitsToDouble(Mysqls.readLong(buffer));
            case FIELD_TYPE_STRING:
            case FIELD_TYPE_VARCHAR:
            case FIELD_TYPE_VAR_STRING:
                return Mysqls.readUTF8StringWithLength(buffer);
            case FIELD_TYPE_DATE:
                return binaryDecodeDate(buffer);
            case FIELD_TYPE_TIME:
                return binaryDecodeTime(buffer);
            case FIELD_TYPE_DATETIME:
            case FIELD_TYPE_TIMESTAMP:
                return binaryDecodeDatetime(buffer);
            case FIELD_TYPE_BLOB:
                return Mysqls.readBytesWithLength(buffer);
        }
        return Mysqls.readBytesWithLength(buffer);
    }

    private static LocalDate binaryDecodeDate(ByteBuffer buffer) {
        LocalDateTime ldt = binaryDecodeDatetime(buffer);
        return ldt == null ? null : ldt.toLocalDate();
    }

    private static LocalDateTime binaryDecodeDatetime(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) return null;

        int length = buffer.get();
        if (length == 0) return null; // invalid value '0000-00-00' or '0000-00-00 00:00:00'

        int year = Mysqls.readShort(buffer);
        byte month = buffer.get();
        byte day = buffer.get();
        if (length == 4) return LocalDateTime.of(year, month, day, 0, 0, 0);

        byte hour = buffer.get();
        byte minute = buffer.get();
        byte second = buffer.get();
        if (length == 11) {
            int microsecond = Mysqls.readInt(buffer);
            return LocalDateTime.of(year, month, day, hour, minute, second, microsecond * 1000);
        } else if (length == 7) {
            return LocalDateTime.of(year, month, day, hour, minute, second, 0);
        }
        throw new RuntimeException("Invalid Datetime");
    }

    private static Duration binaryDecodeTime(ByteBuffer buffer) {
        byte length = buffer.get();
        if (length == 0) {
            return Duration.ZERO;
        } else {
            boolean isNegative = (buffer.get() == 1);
            int days = Mysqls.readInt(buffer);
            int hour = buffer.get();
            int minute = buffer.get();
            int second = buffer.get();
            if (isNegative) {
                days = -days;
                hour = -hour;
                minute = -minute;
                second = -second;
            }

            if (length == 8) {
                return Duration.ofDays(days).plusHours(hour).plusMinutes(minute).plusSeconds(second);
            }
            if (length == 12) {
                long microsecond = Mysqls.readUB4(buffer);
                if (isNegative) {
                    microsecond = -microsecond;
                }
                return Duration.ofDays(days).plusHours(hour).plusMinutes(minute).plusSeconds(second).plusNanos(microsecond * 1000);
            }
            throw new RuntimeException("Invalid time format");
        }
    }
}
