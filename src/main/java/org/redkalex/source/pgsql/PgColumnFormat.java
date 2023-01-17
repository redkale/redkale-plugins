/*
 *
 */
package org.redkalex.source.pgsql;

import java.math.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.sql.JDBCType;
import java.time.*;
import java.time.temporal.*;
import java.util.*;
import org.redkale.source.EntityInfo.EntityColumn;
import org.redkale.source.SourceException;
import org.redkale.util.Attribute;
import static org.redkalex.source.pgsql.PgsqlFormatter.LDT_MINUS_INFINITY;
import static org.redkalex.source.pgsql.PgsqlFormatter.LDT_PLUS_INFINITY;
import static org.redkalex.source.pgsql.PgsqlFormatter.LOCAL_DATE_EPOCH;
import static org.redkalex.source.pgsql.PgsqlFormatter.LOCAL_DATE_TIME_EPOCH;

/**
 *
 * @author zhangjx
 */
public enum PgColumnFormat {

    BOOL(16, true, Boolean.class, JDBCType.BOOLEAN, (array, attr, val) -> {
        array.put((Boolean) val ? (byte) 1 : (byte) 0);
    }, (buffer, array, attr, len) -> {
        return buffer.get() > 0;
    }),
    BOOL_ARRAY(1000, true, Boolean[].class, JDBCType.BOOLEAN, null, null),
    INT2(21, true, Short.class, JDBCType.SMALLINT, (array, attr, val) -> {
        array.putShort(((Number) val).shortValue());
    }, (buffer, array, attr, len) -> {
        return buffer.getShort();
    }),
    INT2_ARRAY(1005, true, Short[].class, JDBCType.SMALLINT, null, null),
    INT4(23, true, Integer.class, JDBCType.INTEGER, (array, attr, val) -> {
        array.putInt(((Number) val).intValue());
    }, (buffer, array, attr, len) -> {
        return buffer.getInt();
    }),
    INT4_ARRAY(1007, true, Integer[].class, JDBCType.INTEGER, null, null),
    INT8(20, true, Long.class, JDBCType.BIGINT, (array, attr, val) -> {
        array.putLong(((Number) val).longValue());
    }, (buffer, array, attr, len) -> {
        return buffer.getLong();
    }),
    INT8_ARRAY(1016, true, Long[].class, JDBCType.BIGINT, null, null),
    FLOAT4(700, true, Float.class, JDBCType.REAL, (array, attr, val) -> {
        array.putFloat(((Number) val).floatValue());
    }, (buffer, array, attr, len) -> {
        return Float.intBitsToFloat(buffer.getInt());
    }),
    FLOAT4_ARRAY(1021, true, Float[].class, JDBCType.REAL, null, null),
    FLOAT8(701, true, Double.class, JDBCType.DOUBLE, (array, attr, val) -> {
        array.putDouble(((Number) val).doubleValue());
    }, (buffer, array, attr, len) -> {
        return Double.longBitsToDouble(buffer.getLong());
    }),
    FLOAT8_ARRAY(1022, true, Double[].class, JDBCType.DOUBLE, null, null),
    NUMERIC(1700, false, Number.class, JDBCType.NUMERIC, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
        String v = array.clear().put(buffer, len).toString(UTF_8);
        Class clazz = attr.type();
        if (clazz == BigInteger.class) {
            return new BigInteger(v);
        } else if (clazz == BigDecimal.class) {
            return new BigDecimal(v);
        } else {
            return v;
        }
    }),
    NUMERIC_ARRAY(1231, false, Number[].class, JDBCType.NUMERIC, null, null),
    MONEY(790, true, Object.class, null, null, null),
    MONEY_ARRAY(791, true, Object[].class, null, null, null),
    BIT(1560, true, Object.class, JDBCType.BIT, null, null),
    BIT_ARRAY(1561, true, Object[].class, JDBCType.BIT, null, null),
    VARBIT(1562, true, Object.class, JDBCType.OTHER, null, null),
    VARBIT_ARRAY(1563, true, Object[].class, JDBCType.BIT, null, null),
    CHAR(18, true, String.class, JDBCType.BIT, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    CHAR_ARRAY(1002, true, String[].class, JDBCType.CHAR, null, null),
    VARCHAR(1043, true, String.class, JDBCType.VARCHAR, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    VARCHAR_ARRAY(1015, true, String[].class, JDBCType.VARCHAR, null, null),
    BPCHAR(1042, true, String.class, JDBCType.VARCHAR, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    BPCHAR_ARRAY(1014, true, String[].class, JDBCType.VARCHAR, null, null),
    TEXT(25, true, String.class, JDBCType.LONGVARCHAR, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    TEXT_ARRAY(1009, true, String[].class, JDBCType.LONGVARCHAR, null, null),
    NAME(19, true, String.class, JDBCType.VARCHAR, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    NAME_ARRAY(1003, true, String[].class, JDBCType.VARCHAR, null, null),
    DATE(1082, true, LocalDate.class, JDBCType.DATE, (array, attr, val) -> {
        int v;
        Class clazz = val.getClass();
        if (clazz == java.util.Date.class) {
            val = LocalDate.ofInstant(((java.util.Date) val).toInstant(), ZoneId.systemDefault());
        } else if (clazz == java.sql.Date.class) {
            val = LocalDate.ofInstant(((java.sql.Date) val).toInstant(), ZoneId.systemDefault());
        }
        clazz = val.getClass();
        if (clazz == LocalDate.class) {
            if (val == LocalDate.MAX) {
                v = Integer.MAX_VALUE;
            } else if (val == LocalDate.MIN) {
                v = Integer.MIN_VALUE;
            } else {
                v = (int) -((LocalDate) val).until(LOCAL_DATE_EPOCH, ChronoUnit.DAYS);
            }
        } else {
            throw new SourceException("Unsupported pgcolumn : " + attr + ", value = " + val);
        }
        array.putInt(v);
    }, (buffer, array, attr, len) -> {
        int val = buffer.getInt();
        LocalDate rs;
        switch (val) {
            case Integer.MAX_VALUE:
                rs = LocalDate.MAX;
                break;
            case Integer.MIN_VALUE:
                rs = LocalDate.MIN;
                break;
            default:
                rs = LOCAL_DATE_EPOCH.plus(val, ChronoUnit.DAYS);
        }
        if (attr != null && attr.type() == java.util.Date.class) {
            Instant instant = rs.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
            return java.util.Date.from(instant);
        } else if (attr != null && attr.type() == java.sql.Date.class) {
            Instant instant = rs.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
            return java.sql.Date.from(instant);
        }
        return rs;
    }),
    DATE_ARRAY(1182, true, LocalDate[].class, JDBCType.DATE, null, null),
    TIME(1083, true, LocalTime.class, JDBCType.TIME, (array, attr, val) -> {
        if (val.getClass() == java.sql.Time.class) {
            val = ((java.sql.Time) val).toLocalTime();
        }
        array.putLong(((LocalTime) val).getLong(ChronoField.MICRO_OF_DAY));
    }, (buffer, array, attr, len) -> {
        LocalTime rs = LocalTime.ofNanoOfDay(buffer.getLong() * 1000);
        if (attr != null && attr.type() == java.sql.Time.class) {
            return java.sql.Time.valueOf(rs);
        }
        return rs;
    }),
    TIME_ARRAY(1183, true, LocalTime[].class, JDBCType.TIME, null, null),
    TIMETZ(1266, true, OffsetTime.class, JDBCType.TIME_WITH_TIMEZONE, null, null),
    TIMETZ_ARRAY(1270, true, OffsetTime[].class, JDBCType.TIME_WITH_TIMEZONE, null, null),
    TIMESTAMP(1114, true, LocalDateTime.class, JDBCType.TIMESTAMP, (array, attr, val) -> {
        if (val.getClass() == java.sql.Timestamp.class) {
            val = ((java.sql.Timestamp) val).toLocalDateTime();
        }
        if (((LocalDateTime) val).compareTo(LDT_PLUS_INFINITY) >= 0) {
            val = LDT_PLUS_INFINITY;
        } else if (((LocalDateTime) val).compareTo(LDT_MINUS_INFINITY) <= 0) {
            val = LDT_MINUS_INFINITY;
        }
        array.putLong(-((LocalDateTime) val).until(LOCAL_DATE_TIME_EPOCH, ChronoUnit.MICROS));
        array.putLong(((LocalDateTime) val).getLong(ChronoField.MICRO_OF_DAY));
    }, (buffer, array, attr, len) -> {
        LocalDateTime rs = LOCAL_DATE_TIME_EPOCH.plus(buffer.getLong(), ChronoUnit.MICROS);
        if (attr != null && attr.type() == java.sql.Timestamp.class) {
            return java.sql.Timestamp.valueOf(rs);
        }
        return rs;
    }),
    TIMESTAMP_ARRAY(1115, true, LocalDateTime[].class, JDBCType.TIMESTAMP, null, null),
    TIMESTAMPTZ(1184, true, OffsetDateTime.class, JDBCType.TIMESTAMP_WITH_TIMEZONE, null, null),
    TIMESTAMPTZ_ARRAY(1185, true, OffsetDateTime[].class, JDBCType.TIMESTAMP_WITH_TIMEZONE, null, null),
    INTERVAL(1186, true, java.sql.Date.class, JDBCType.DATE, null, null),
    INTERVAL_ARRAY(1187, true, java.sql.Date[].class, JDBCType.DATE, null, null),
    BYTEA(17, true, byte[].class, JDBCType.BINARY, (array, attr, val) -> {
        array.put((byte[]) val);
    }, (buffer, array, attr, len) -> {
        byte[] rs = new byte[len];
        buffer.get(rs);
        return rs;
    }),
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
    UNKNOWN(705, false, String.class, JDBCType.OTHER, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    TS_VECTOR(3614, false, String.class, JDBCType.OTHER, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
        return array.clear().put(buffer, len).toString(UTF_8);
    }),
    TS_VECTOR_ARRAY(3643, false, String[].class, JDBCType.OTHER, null, null),
    TS_QUERY(3615, false, String.class, JDBCType.OTHER, (array, attr, val) -> {
        array.put(val.toString().getBytes(UTF_8));
    }, (buffer, array, attr, len) -> {
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
        } else if (clazz == byte.class || clazz == Byte.class) {
            return PgColumnFormat.INT2;
        } else if (clazz == char.class || clazz == Character.class) {
            return PgColumnFormat.INT2;
        } else if (clazz == float.class || clazz == Float.class) {
            return PgColumnFormat.FLOAT4;
        } else if (clazz == double.class || clazz == Double.class) {
            return PgColumnFormat.FLOAT8;
        } else if (clazz == BigInteger.class || clazz == BigDecimal.class) {
            return PgColumnFormat.NUMERIC;
        } else if (clazz == byte[].class) {
            return PgColumnFormat.BYTEA;
        } else if (clazz == java.time.LocalDate.class || clazz == java.util.Date.class || "java.sql.Date".equals(clazz.getName())) {
            return PgColumnFormat.DATE;
        } else if (clazz == java.time.LocalTime.class || "java.sql.Time".equals(clazz.getName())) {
            return PgColumnFormat.TIME;
        } else if (clazz == java.time.LocalDateTime.class || "java.sql.Timestamp".equals(clazz.getName())) {
            return PgColumnFormat.TIMESTAMP;
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
