/*
 *
 */
package org.redkalex.source.pgsql;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redkale.net.client.ClientConnection;
import org.redkale.source.EntityInfo.EntityColumn;
import org.redkale.util.*;
import static org.redkalex.source.pgsql.PgClientRequest.writeUTF8String;

/**
 *
 * @author zhangjx
 */
public class PgPrepareDesc {

    public static enum PgExtendMode {
        FIND_ENTITY, FINDS_ENTITY, INSERT_ENTITY, UPDATE_ENTITY, UPCASE_ENTITY, LISTALL_ENTITY;
    }

    private final int type;

    private final PgExtendMode mode;

    private final String sql;

    private final byte[] statement;

    private final Attribute[] paramAttrs;

    private final EntityColumn[] paramCols;

    private final PgColumnFormat[] paramFormats;

    private final Attribute[] resultAttrs;

    private final EntityColumn[] resultCols;

    private final PgColumnFormat[] resultFormats;

    private final AtomicBoolean completed = new AtomicBoolean();

    private final byte[] bindPrefixBytes;

    private final byte[] bindPostfixBytes;

    private final byte[] bindNoParamBytes;

    private PgRowDesc rowDesc;

    public PgPrepareDesc(int type, PgExtendMode mode, String sql, byte[] statement,
        Attribute[] paramAttrs, EntityColumn[] paramCols, Attribute[] resultAttrs, EntityColumn[] resultCols) {
        Objects.requireNonNull(sql);
        Objects.requireNonNull(statement);
        Objects.requireNonNull(paramAttrs);
        Objects.requireNonNull(paramCols);
        Objects.requireNonNull(resultAttrs);
        Objects.requireNonNull(resultCols);
        this.type = type;
        this.mode = mode;
        this.sql = sql;
        this.statement = statement;
        this.paramAttrs = paramAttrs;
        this.paramCols = paramCols;
        this.resultAttrs = resultAttrs;
        this.resultCols = resultCols;
        this.paramFormats = new PgColumnFormat[paramAttrs.length];
        for (int i = 0; i < paramAttrs.length; i++) {
            this.paramFormats[i] = PgColumnFormat.valueOf(paramAttrs[i], paramCols[i]);
        }
        this.resultFormats = new PgColumnFormat[resultAttrs.length];
        for (int i = 0; i < resultAttrs.length; i++) {
            this.resultFormats[i] = PgColumnFormat.valueOf(resultAttrs[i], resultCols[i]);
        }
        ByteArray array = new ByteArray(128);
        {
            array.clear();
            array.putInt(0); //command-length
            array.putByte(0); // portal  
            array.put(statement); //prepared statement

            // Param columns are all in Binary format
            PgColumnFormat[] pformats = paramFormats;
            int paramLen = pformats.length;
            array.putShort(paramLen);
            for (PgColumnFormat f : pformats) {
                array.putShort(f.supportsBinary() ? 1 : 0);
            }
            array.putShort(paramLen);
            this.bindPrefixBytes = array.getBytes();
        }
        {
            array.clear();
            // Result columns are all in Binary format
            PgColumnFormat[] rformats = resultFormats;
            if (rformats.length > 0) {
                array.putShort(rformats.length);
                for (PgColumnFormat f : rformats) {
                    array.putShort(f.supportsBinary() ? 1 : 0);
                }
            } else {
                array.putShort(1);
                array.putShort(1);
            }
            this.bindPostfixBytes = array.getBytes();
        }
        {
            array.clear();
            array.putByte('B');
            int start = array.length();
            array.put(bindPrefixBytes);
            array.put(bindPostfixBytes);
            array.putInt(start, array.length() - start);
            this.bindNoParamBytes = array.getBytes();
        }
    }

    public void writeTo(ClientConnection conn, ByteArray array) {
        writeParse(array);
        writeDescribe(array);
    }

    private void writeParse(ByteArray array) { // PARSE
        array.putByte('P');
        int start = array.length();
        array.putInt(0); //command-length
        array.put(statement);
        writeUTF8String(array, sql);
        PgColumnFormat[] formats = paramFormats();
        if (formats.length == 0) {
            array.putShort(0); // no parameter types
        } else {
            array.putShort(formats.length);
            for (PgColumnFormat f : formats) {
                array.putInt(f.oid());
            }
        }
        array.putInt(start, array.length() - start);
    }

    private void writeDescribe(ByteArray array) { // DESCRIBE
        array.putByte('D');
        array.putInt(4 + 1 + statement.length);
        array.putByte('S');
        array.put(statement);
    }

    @Override
    public String toString() {
        StringBuilder paramsb = new StringBuilder();
        paramsb.append('[');
        for (PgColumnFormat f : paramFormats) {
            if (paramsb.length() > 1) {
                paramsb.append(',');
            }
            paramsb.append(f.name());
        }
        paramsb.append(']');
        StringBuilder resultsb = new StringBuilder();
        resultsb.append('[');
        for (PgColumnFormat f : resultFormats) {
            if (resultsb.length() > 1) {
                resultsb.append(',');
            }
            resultsb.append(f.name());
        }
        resultsb.append(']');
        return "PgPrepareDesc_" + Objects.hashCode(this) + "{sql=" + sql + ", rowDesc=" + rowDesc + ", paramFormats=" + paramsb + ", resultFormats=" + resultsb + "}";
    }

    public void complete() {
        this.completed.set(true);
    }

    public void uncomplete() {
        this.completed.set(false);
    }

    public byte[] bindNoParamBytes() {
        return bindNoParamBytes;
    }

    public byte[] bindPrefixBytes() {
        return bindPrefixBytes;
    }

    public byte[] bindPostfixBytes() {
        return bindPostfixBytes;
    }

    public int type() {
        return type;
    }

    public PgExtendMode mode() {
        return mode;
    }

    public String sql() {
        return sql;
    }

    public byte[] statement() {
        return statement;
    }

    public Attribute[] paramAttrs() {
        return paramAttrs;
    }

    public EntityColumn[] paramCols() {
        return paramCols;
    }

    public PgColumnFormat[] paramFormats() {
        return paramFormats;
    }

    public Attribute[] resultAttrs() {
        return resultAttrs;
    }

    public EntityColumn[] resultCols() {
        return resultCols;
    }

    public PgColumnFormat[] resultFormats() {
        return resultFormats;
    }

    public PgRowDesc getRowDesc() {
        return rowDesc;
    }

    public void setRowDesc(PgRowDesc rowDesc) {
        this.rowDesc = rowDesc;
    }

}
