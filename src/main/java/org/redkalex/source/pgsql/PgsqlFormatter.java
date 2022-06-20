/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.time.*;
import java.time.format.*;
import static java.time.format.DateTimeFormatter.*;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.source.EntityInfo;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public abstract class PgsqlFormatter {

    //attr为空就返回byte[], 不为空返回attr对应类型的对象
    public static <T> Serializable decodeRowColumnValue(ByteBuffer buffer, ByteArray tmp, Attribute<T, Serializable> attr, int bslen) {
        if (bslen == -1) return null;
        if (attr == null) {
            byte[] bs = new byte[bslen];
            buffer.get(bs);
            return bs;
        }
        Class type = attr.type();
        if (type == int.class || type == Integer.class) {
            return buffer.getInt();
        } else if (type == String.class) {
            if (bslen == 0) return "";
            tmp.clear().put(buffer, bslen);
            return tmp.toString(StandardCharsets.UTF_8);
        } else if (type == long.class || type == Long.class) {
            return buffer.getLong();
        } else if (type == boolean.class || type == Boolean.class) {
            return buffer.get() == 1;
        } else if (type == short.class || type == Short.class) {
            return buffer.getShort();
        } else if (type == float.class || type == Float.class) {
            return Float.intBitsToFloat(buffer.getInt());
        } else if (type == double.class || type == Double.class) {
            return Double.longBitsToDouble(buffer.getLong());
        } else if (type == byte[].class) {
            byte[] bs = new byte[bslen];
            buffer.get(bs);
            return bs;
        } else if (type == AtomicInteger.class) {
            return new AtomicInteger(buffer.getInt());
        } else if (type == AtomicLong.class) {
            return new AtomicLong(buffer.getLong());
        } else if (type == byte.class || type == Byte.class) {
            return buffer.get();
        } else if (type == char.class || type == Character.class) {
            return buffer.getChar();
        } else if (type == java.sql.Date.class) {
            return new java.sql.Date(LOCAL_DATE_EPOCH.plus(buffer.getInt(), ChronoUnit.DAYS).toEpochSecond(LocalTime.MIN, ZoneOffset.UTC) * 1000);
        } else if (type == java.util.Date.class) {
            return new java.util.Date(LOCAL_DATE_EPOCH.plus(buffer.getInt(), ChronoUnit.DAYS).toEpochSecond(LocalTime.MIN, ZoneOffset.UTC) * 1000);
        } else if (type == java.time.LocalDate.class) {
            return LOCAL_DATE_EPOCH.plus(buffer.getInt(), ChronoUnit.DAYS);
        } else if (type == java.time.LocalTime.class) {
            return LocalTime.ofNanoOfDay(buffer.getLong() * 1000);
        } else {
            if (bslen == 0) return null;
            tmp.clear().put(buffer, bslen);
            return JsonConvert.root().convertFrom(attr.genericType(), tmp.toString(StandardCharsets.UTF_8));
            //throw new RuntimeException("Not supported column: " + attr.field() + ", type: " + attr.type());
        }
    }

    public static <T> void encodePrepareParamValue(ByteArray array, EntityInfo<T> info, boolean binary, Attribute<T, Serializable> attr, Object param) {
        byte[] bs = formatPrepareParamText(info, attr, param);
        if (bs == null) {
            array.putInt(-1);
        } else {
            array.putInt(bs.length);
            array.put(bs);
        }
    }

    //--------------------------------- binary -----------------------------
    //---------------------------------- text ------------------------------
    private static <T> byte[] formatPrepareParamText(EntityInfo<T> info, Attribute<T, Serializable> attr, Object param) {
        if (param == null && info.isNotNullJson(attr)) return new byte[0];
        if (param == null) return null;
        if (param instanceof byte[]) return (byte[]) param;
        if (param instanceof Boolean) return (Boolean) param ? TRUE_BYTES : FALSE_BYTES;
        if (param instanceof java.sql.Date) return ISO_LOCAL_DATE.format(((java.sql.Date) param).toLocalDate()).getBytes(UTF_8);
        if (param instanceof java.sql.Time) return ISO_LOCAL_TIME.format(((java.sql.Time) param).toLocalTime()).getBytes(UTF_8);
        if (param instanceof java.sql.Timestamp) return TIMESTAMP_FORMAT.format(((java.sql.Timestamp) param).toLocalDateTime()).getBytes(UTF_8);
        if (param instanceof java.time.LocalDate) return ISO_LOCAL_DATE.format((java.time.LocalDate) param).getBytes(UTF_8);
        if (param instanceof java.time.LocalTime) return ISO_LOCAL_TIME.format((java.time.LocalTime) param).getBytes(UTF_8);
        if (param instanceof java.time.LocalDateTime) return TIMESTAMP_FORMAT.format((java.time.LocalDateTime) param).getBytes(UTF_8);
        if (!(param instanceof Number) && !(param instanceof CharSequence) && !(param instanceof java.util.Date)
            && !param.getClass().getName().startsWith("java.sql.") && !param.getClass().getName().startsWith("java.time.")) {
            if (attr == null) return info.getJsonConvert().convertTo(param).getBytes(StandardCharsets.UTF_8);
            return info.getJsonConvert().convertTo(attr.genericType(), param).getBytes(StandardCharsets.UTF_8);
        }
        return String.valueOf(param).getBytes(UTF_8);
    }

    static final byte[] TRUE_BYTES = new byte[]{'t'};

    static final byte[] FALSE_BYTES = new byte[]{'f'};

    static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder().parseCaseInsensitive().append(ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME).toFormatter();

    static final DateTimeFormatter TIMESTAMPZ_FORMAT = new DateTimeFormatterBuilder().parseCaseInsensitive().append(ISO_LOCAL_DATE).appendLiteral(' ').append(ISO_LOCAL_TIME).appendOffset("+HH:mm", "").toFormatter();

    static final DateTimeFormatter TIMEZ_FORMAT = new DateTimeFormatterBuilder().parseCaseInsensitive().append(ISO_LOCAL_TIME).appendOffset("+HH:mm", "").toFormatter();

    static final LocalDate LOCAL_DATE_EPOCH = LocalDate.of(2000, 1, 1);

    static final LocalDateTime LOCAL_DATE_TIME_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0, 0);

    static final OffsetDateTime OFFSET_DATE_TIME_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0, 0).atOffset(ZoneOffset.UTC);
}
