/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.math.BigDecimal;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.sql.*;
import java.time.*;
import static java.time.format.DateTimeFormatter.*;
import java.time.format.DateTimeParseException;
import org.redkale.source.SourceException;
import org.redkale.util.Utility;
import static org.redkalex.source.pgsql.PgsqlOid.*;

/**
 *
 * @author zhangjx
 */
public class PgRowColumn {

    final String name;

    final int oid;

    final PgColumnFormat format;

    public PgRowColumn(String name, int oid) {
        this.name = name;
        this.oid = oid;
        this.format = PgColumnFormat.valueOf(oid);
        if (this.format.encoder() == null || this.format.decoder() == null) {
            throw new SourceException("Unsupported oid: " + oid);
        }
    }

    public String getName() {
        return name;
    }

    public int getOid() {
        return oid;
    }

    public PgColumnFormat getFormat() {
        return format;
    }

    @Override
    public String toString() {
        return "{\"name\"=\"" + name + "\", \"oid\"=" + oid + "}";
    }

    public Serializable getObject(byte[] value) {
        if (value == null) {
            return null;
        }
        switch (oid) {
            case TEXT: // fallthrough
            case CHAR: // fallthrough
            case BPCHAR: // fallthrough
            case VARCHAR: return new String(value, UTF_8);
            case INT2: return Short.valueOf(new String(value, UTF_8));
            case INT4: return Integer.valueOf(new String(value, UTF_8));
            case INT8: return Long.valueOf(new String(value, UTF_8));
            case FLOAT4: return Float.valueOf(new String(value, UTF_8));
            case NUMERIC: // fallthrough
            case FLOAT8: return new BigDecimal(new String(value, UTF_8));
            case BIT: return Utility.hexToBin(new String(value, 2, value.length - 2, UTF_8))[0];
            case BIT_ARRAY:// fallthrough
            case BYTEA: return Utility.hexToBin(new String(value, 2, value.length - 2, UTF_8));
            case DATE: String date = new String(value, UTF_8);
                try {
                    return Date.valueOf(LocalDate.parse(date, ISO_LOCAL_DATE));
                } catch (DateTimeParseException e) {
                    throw new SourceException("Invalid date: " + date);
                }
            case TIMETZ: return Time.valueOf(OffsetTime.parse(new String(value, UTF_8), PgsqlFormatter.TIMEZ_FORMAT).toLocalTime());
            case TIME: return Time.valueOf(LocalTime.parse(new String(value, UTF_8), ISO_LOCAL_TIME));
            case TIMESTAMP: return Timestamp.valueOf(LocalDateTime.parse(new String(value, UTF_8), PgsqlFormatter.TIMESTAMP_FORMAT));
            case TIMESTAMPTZ: return Timestamp.valueOf(OffsetDateTime.parse(new String(value, UTF_8), PgsqlFormatter.TIMESTAMPZ_FORMAT).toLocalDateTime());
            case UUID: return java.util.UUID.fromString(new String(value, UTF_8));
            case BOOL: return 't' == value[0];

            case INT2_ARRAY:
            case INT4_ARRAY:
            case INT8_ARRAY:
            case FLOAT4_ARRAY:
            case FLOAT8_ARRAY:
            case TEXT_ARRAY:
            case CHAR_ARRAY:
            case BPCHAR_ARRAY:
            case VARCHAR_ARRAY:
            case TIMESTAMP_ARRAY:
            case TIMESTAMPTZ_ARRAY:
            case TIMETZ_ARRAY:
            case TIME_ARRAY:
            case BOOL_ARRAY:
                throw new IllegalStateException("Unsupported array type: " + oid);
            default:
                throw new IllegalStateException("Unsupported array type: " + oid);
        }
    }

}
