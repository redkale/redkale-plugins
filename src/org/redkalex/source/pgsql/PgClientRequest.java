/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.time.format.*;
import static java.time.format.DateTimeFormatter.*;
import java.util.function.Supplier;
import org.redkale.net.client.ClientRequest;
import org.redkale.source.EntityInfo;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public abstract class PgClientRequest implements ClientRequest {

    public static final int REQ_TYPE_AUTH = 1 << 1;

    public static final int REQ_TYPE_QUERY = 1 << 2;

    public static final int REQ_TYPE_UPDATE = 1 << 3;

    public static final int REQ_TYPE_INSERT = 1 << 4;

    public static final int REQ_TYPE_DELETE = 1 << 5;

    public static final int REQ_TYPE_EXTEND_QUERY = (1 << 2) + 1;

    public static final int REQ_TYPE_EXTEND_UPDATE = (1 << 3) + 1;

    public static final int REQ_TYPE_EXTEND_INSERT = (1 << 4) + 1;

    public static final int REQ_TYPE_EXTEND_DELETE = (1 << 5) + 1;

    static final byte[] TRUE_BYTES = new byte[]{'t'};

    static final byte[] FALSE_BYTES = new byte[]{'f'};

    static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .append(ISO_LOCAL_TIME)
        .toFormatter();

    static final DateTimeFormatter TIMESTAMPZ_FORMAT = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_DATE)
        .appendLiteral(' ')
        .append(ISO_LOCAL_TIME)
        .appendOffset("+HH:mm", "")
        .toFormatter();

    static final DateTimeFormatter TIMEZ_FORMAT = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(ISO_LOCAL_TIME)
        .appendOffset("+HH:mm", "")
        .toFormatter();

    protected Supplier<PgRowDesc> currExtendedRowDescSupplier;

    public abstract int getType();

    protected static ByteArray writeUTF8String(ByteArray array, String string) {
        array.put(string.getBytes(StandardCharsets.UTF_8));
        array.put((byte) 0);
        return array;
    }

    protected static <T> byte[] formatPrepareParam(EntityInfo<T> info, Attribute<T, Serializable> attr, Object param) {
        if (param == null && info.isNotNullJson(attr)) return new byte[0];
        if (param == null) return null;
        if (param instanceof byte[]) return (byte[]) param;
        if (param instanceof Boolean) return (Boolean) param ? TRUE_BYTES : FALSE_BYTES;
        if (param instanceof java.sql.Date) return ISO_LOCAL_DATE.format(((java.sql.Date) param).toLocalDate()).getBytes(UTF_8);
        if (param instanceof java.sql.Time) return ISO_LOCAL_TIME.format(((java.sql.Time) param).toLocalTime()).getBytes(UTF_8);
        if (param instanceof java.sql.Timestamp) return TIMESTAMP_FORMAT.format(((java.sql.Timestamp) param).toLocalDateTime()).getBytes(UTF_8);
        if (!(param instanceof Number) && !(param instanceof CharSequence) && !(param instanceof java.util.Date)
            && !param.getClass().getName().startsWith("java.sql.") && !param.getClass().getName().startsWith("java.time.")) {
            if (attr == null) return info.getJsonConvert().convertTo(param).getBytes(StandardCharsets.UTF_8);
            return info.getJsonConvert().convertTo(attr.genericType(), param).getBytes(StandardCharsets.UTF_8);
        }
        return String.valueOf(param).getBytes(UTF_8);
    }
}
