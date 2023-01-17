/*
 *
 */
package org.redkalex.source.pgsql;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.sql.JDBCType;
import java.time.*;
import java.time.temporal.*;
import java.util.*;
import org.redkale.source.EntityInfo.EntityColumn;
import org.redkale.util.Attribute;
import static org.redkalex.source.pgsql.PgsqlFormatter.LOCAL_DATE_EPOCH;

/**
 *
 * @author zhangjx
 */
public enum PgColumnFormat {

    BOOL(16, true, Boolean.class, JDBCType.BOOLEAN, (array, val) -> {
        array.put((Boolean) val ? (byte) 1 : (byte) 0);
    }, (buffer, array, len) -> {
        return buffer.get() > 0;
    }),
    BOOL_ARRAY(1000, true, Boolean[].class, JDBCType.BOOLEAN, null, null),
    INT2(21, true, Short.class, JDBCType.SMALLINT, (array, val) -> {
        array.putShort(((Number) val).shortValue());
    }, (buffer, array, len) -> {
        return buffer.getShort();
    }),
    INT2_ARRAY(1005, true, Short[].class, JDBCType.SMALLINT, null, null),
    INT4(23, true, Integer.class, JDBCType.INTEGER, (array, val) -> {
        array.putInt(((Number) val).intValue());
    }, (buffer, array, len) -> {
        return buffer.getInt();
    }),
    INT4_ARRAY(1007, true, Integer[].class, JDBCType.INTEGER, null, null),
    INT8(20, true, Long.class, JDBCType.BIGINT, (array, val) -> {
        array.putLong(((Number) val).longValue());
    }, (buffer, array, len) -> {
        return buffer.getLong();
    }),
    INT8_ARRAY(1016, true, Long[].class, JDBCType.BIGINT, null, null),
    FLOAT4(700, true, Float.class, JDBCType.REAL, (array, val) -> {
        array.putFloat(((Number) val).floatValue());
    }, (buffer, array, len) -> {
        return Float.intBitsToFloat(buffer.getInt());
    }),
    FLOAT4_ARRAY(1021, true, Float[].class, JDBCType.REAL, null, null),
    FLOAT8(701, true, Double.class, JDBCType.DOUBLE, (array, val) -> {
        array.putDouble(((Number) val).doubleValue());
    }, (buffer, array, len) -> {
        return Double.longBitsToDouble(buffer.getLong());
    }),
    FLOAT8_ARRAY(1022, true, Double[].class, JDBCType.DOUBLE, null, null),
    NUMERIC(1700, false, Number.class, JDBCType.NUMERIC, null, null),
    NUMERIC_ARRAY(1231, false, Number[].class, JDBCType.NUMERIC, null, null),
    MONEY(790, true, Object.class, null, null, null),
    MONEY_ARRAY(791, true, Object[].class, null, null, null),
    BIT(1560, true, Object.class, JDBCType.BIT, null, null),
    BIT_ARRAY(1561, true, Object[].class, JDBCType.BIT, null, null),
    VARBIT(1562, true, Object.class, JDBCType.OTHER, null, null),
    VARBIT_ARRAY(1563, true, Object[].class, JDBCType.BIT, null, null),
    CHAR(18, true, String.class, JDBCType.BIT, (array, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    CHAR_ARRAY(1002, true, String[].class, JDBCType.CHAR, null, null),
    VARCHAR(1043, true, String.class, JDBCType.VARCHAR, (array, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    VARCHAR_ARRAY(1015, true, String[].class, JDBCType.VARCHAR, null, null),
    BPCHAR(1042, true, String.class, JDBCType.VARCHAR, (array, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    BPCHAR_ARRAY(1014, true, String[].class, JDBCType.VARCHAR, null, null),
    TEXT(25, true, String.class, JDBCType.LONGVARCHAR, (array, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    TEXT_ARRAY(1009, true, String[].class, JDBCType.LONGVARCHAR, null, null),
    NAME(19, true, String.class, JDBCType.VARCHAR, (array, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    NAME_ARRAY(1003, true, String[].class, JDBCType.VARCHAR, null, null),
    DATE(1082, true, LocalDate.class, JDBCType.DATE, (array, val) -> {
        int v;
        if (val == LocalDate.MAX) {
            v = Integer.MAX_VALUE;
        } else if (val == LocalDate.MIN) {
            v = Integer.MIN_VALUE;
        } else {
            v = (int) -((LocalDate) val).until(LOCAL_DATE_EPOCH, ChronoUnit.DAYS);
        }
        array.putInt(v);
    }, (buffer, array, len) -> {
        int val = buffer.getInt();
        switch (val) {
            case Integer.MAX_VALUE:
                return LocalDate.MAX;
            case Integer.MIN_VALUE:
                return LocalDate.MIN;
            default:
                return LOCAL_DATE_EPOCH.plus(val, ChronoUnit.DAYS);
        }
    }),
    DATE_ARRAY(1182, true, LocalDate[].class, JDBCType.DATE, null, null),
    TIME(1083, true, LocalTime.class, JDBCType.TIME, (array, val) -> {
        array.putLong(((LocalTime) val).getLong(ChronoField.MICRO_OF_DAY));
    }, (buffer, array, len) -> {
        return LocalTime.ofNanoOfDay(buffer.getLong() * 1000);
    }),
    TIME_ARRAY(1183, true, LocalTime[].class, JDBCType.TIME, null, null),
    TIMETZ(1266, true, OffsetTime.class, JDBCType.TIME_WITH_TIMEZONE, null, null),
    TIMETZ_ARRAY(1270, true, OffsetTime[].class, JDBCType.TIME_WITH_TIMEZONE, null, null),
    TIMESTAMP(1114, true, LocalDateTime.class, JDBCType.TIMESTAMP, null, null),
    TIMESTAMP_ARRAY(1115, true, LocalDateTime[].class, JDBCType.TIMESTAMP, null, null),
    TIMESTAMPTZ(1184, true, OffsetDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE, null, null),
    TIMESTAMPTZ_ARRAY(1185, true, OffsetDateTime[].class, JDBCType.TIMESTAMP_WITH_TIMEZONE, null, null),
    INTERVAL(1186, true, java.sql.Date.class, JDBCType.DATE, null, null),
    INTERVAL_ARRAY(1187, true, java.sql.Date[].class, JDBCType.DATE, null, null),
    BYTEA(17, true, byte[].class, JDBCType.BINARY, null, null),
    BYTEA_ARRAY(1001, true, byte[][].class, JDBCType.BINARY, null, null),
    MACADDR(829, true, Object.class, JDBCType.OTHER, null, null),
    INET(869, true, Object[].class, JDBCType.OTHER, null, null),
    CIDR(650, true, Object.class, JDBCType.OTHER, null, null),
    MACADDR8(774, true, Object[].class, JDBCType.OTHER, null, null),
    UUID(2950, true, UUID.class, JDBCType.OTHER, null, null),
    UUID_ARRAY(2951, true, UUID[].class, JDBCType.OTHER, null, null),
    JSON(114, true, Object.class, JDBCType.OTHER, null, null),
    JSON_ARRAY(199, true, Object[].class, JDBCType.OTHER, null, null),
    JSONB(3802, true, Object.class, JDBCType.OTHER, null, null),
    JSONB_ARRAY(3807, true, Object[].class, JDBCType.OTHER, null, null),
    XML(142, true, Object.class, JDBCType.OTHER, null, null),
    XML_ARRAY(143, true, Object[].class, JDBCType.OTHER, null, null),
    POINT(600, true, Object.class, JDBCType.OTHER, null, null),
    POINT_ARRAY(1017, true, Object[].class, JDBCType.OTHER, null, null),
    LINE(628, true, Object.class, JDBCType.OTHER, null, null),
    LINE_ARRAY(629, true, Object[].class, JDBCType.OTHER, null, null),
    LSEG(601, true, Object.class, JDBCType.OTHER, null, null),
    LSEG_ARRAY(1018, true, Object[].class, JDBCType.OTHER, null, null),
    BOX(603, true, Object.class, JDBCType.OTHER, null, null),
    BOX_ARRAY(1020, true, Object[].class, JDBCType.OTHER, null, null),
    PATH(602, true, Object.class, JDBCType.OTHER, null, null),
    PATH_ARRAY(1019, true, Object[].class, JDBCType.OTHER, null, null),
    POLYGON(604, true, Object.class, JDBCType.OTHER, null, null),
    POLYGON_ARRAY(1027, true, Object[].class, JDBCType.OTHER, null, null),
    CIRCLE(718, true, Object.class, JDBCType.OTHER, null, null),
    CIRCLE_ARRAY(719, true, Object[].class, JDBCType.OTHER, null, null),
    HSTORE(33670, true, Object.class, JDBCType.OTHER, null, null),
    OID(26, true, Object.class, JDBCType.OTHER, null, null),
    OID_ARRAY(1028, true, Object[].class, JDBCType.OTHER, null, null),
    VOID(2278, true, Object.class, JDBCType.OTHER, null, null),
    UNKNOWN(705, false, String.class, JDBCType.OTHER, (array, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    TS_VECTOR(3614, false, String.class, JDBCType.OTHER, (array, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    TS_VECTOR_ARRAY(3643, false, String[].class, JDBCType.OTHER, null, null),
    TS_QUERY(3615, false, String.class, JDBCType.OTHER, (array, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    TS_QUERY_ARRAY(3645, false, String[].class, JDBCType.OTHER, null, null);

    private final int oid;

    private final boolean array;

    private final boolean supportsBinary;

    private final Class<?> dataType;

    private final JDBCType jdbcType;

    private final PgColumnEncodeable encoder; //null值不会调用到此方法

    private final PgColumnDecodeable decoder;//null值不会调用到此方法

    private static final Map<Integer, PgColumnFormat> map = new HashMap<>();

    static {
        for (PgColumnFormat t : PgColumnFormat.values()) {
            map.put(t.oid, t);
        }
    }

    private PgColumnFormat(int oid, boolean supportsBinary, Class<?> dataType,
        JDBCType jdbcType, PgColumnEncodeable encoder, PgColumnDecodeable decoder) {
        this.oid = oid;
        this.supportsBinary = supportsBinary;
        this.dataType = dataType;
        this.jdbcType = jdbcType;
        this.encoder = encoder;
        this.decoder = decoder;
        this.array = dataType.isArray();
    }

    public static PgColumnFormat valueOf(int oid) {
        return map.getOrDefault(oid, UNKNOWN);
    }

    public static PgColumnFormat valueOf(Attribute attr, EntityColumn column) {
        Class clazz = attr.type();
        if (clazz == int.class || clazz == Integer.class) {
            return PgColumnFormat.INT4;
        } else if (clazz == long.class || clazz == Long.class) {
            return PgColumnFormat.INT8;
        } else if (clazz == String.class) {
            if (column == null || column.length < 65535) {
                return PgColumnFormat.VARCHAR;
            } else if (column.length == 65535) { //TEXT
                return PgColumnFormat.TEXT;
            } else if (column.length <= 16777215) { //MEDIUMTEXT
                return PgColumnFormat.TEXT;
            } else { //LONGTEXT
                return PgColumnFormat.TEXT;
            }
        } else if (clazz == boolean.class || clazz == Boolean.class) {
            return PgColumnFormat.BOOL;
        } else if (clazz == short.class || clazz == Short.class) {
            return PgColumnFormat.INT2;
        } else if (clazz == float.class || clazz == Float.class) {
            return PgColumnFormat.FLOAT4;
        } else if (clazz == double.class || clazz == Double.class) {
            return PgColumnFormat.FLOAT8;
        } else {
            return PgColumnFormat.UNKNOWN;
        }
    }

    public int oid() {
        return oid;
    }

    public boolean array() {
        return array;
    }

    public boolean supportsBinary() {
        return supportsBinary;
    }

    public Class<?> dataType() {
        return dataType;
    }

    public JDBCType jdbcType() {
        return jdbcType;
    }

    public PgColumnEncodeable encoder() {
        return encoder;
    }

    public PgColumnDecodeable decoder() {
        return decoder;
    }

}
