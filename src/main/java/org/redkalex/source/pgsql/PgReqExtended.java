/*
 *
 */
package org.redkalex.source.pgsql;

import static org.redkalex.source.pgsql.PgClientCodec.logger;

import java.io.Serializable;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Stream;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;
import org.redkalex.source.pgsql.PgPrepareDesc.PgExtendMode;

/** @author zhangjx */
public class PgReqExtended extends PgClientRequest {

    protected int type;

    protected String sql;

    protected PgExtendMode mode;

    protected boolean sendPrepare;

    protected int fetchSize;

    protected int paramLen = -1;

    protected Serializable[][] paramValues;

    protected Serializable[] pkValues;

    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + Objects.hashCode(this) + "{sql = '" + sql + "', type = " + getType()
                + ", traceid = " + getTraceid()
                + (pkValues == null
                        ? (", paramValues = "
                                + (paramValues != null && paramValues.length > 10
                                        ? ("size " + paramValues.length)
                                        : JsonConvert.root().convertTo(paramValues)))
                        : (", pkValues = "
                                + (pkValues.length > 10
                                        ? ("size " + pkValues.length)
                                        : JsonConvert.root().convertTo(pkValues))))
                + "}";
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    protected void prepare() {
        super.prepare();
    }

    @Override
    protected boolean recycle() {
        boolean rs = super.recycle();
        this.type = 0;
        this.sql = null;
        this.mode = null;
        this.sendPrepare = false;
        this.fetchSize = 0;
        this.paramValues = null;
        this.pkValues = null;
        return rs;
    }

    public <T> void prepare(int type, PgExtendMode mode, String sql, int fetchSize) {
        super.prepare();
        this.type = type;
        this.mode = mode;
        this.sql = sql;
        this.fetchSize = fetchSize;
    }

    public <T> void preparePrimarys(
            int type, PgExtendMode mode, String sql, int fetchSize, final Serializable... pkValues) {
        prepare(type, mode, sql, fetchSize);
        this.pkValues = pkValues;
    }

    public <T> void prepareParams(
            int type, PgExtendMode mode, String sql, int fetchSize, final Serializable[][] paramValues) {
        prepare(type, mode, sql, fetchSize);
        this.paramValues = paramValues;
    }

    public <T> void prepareParams(
            int type, PgExtendMode mode, String sql, int fetchSize, int paramLen, final Stream<Serializable> stream) {
        prepare(type, mode, sql, fetchSize);
        this.paramLen = paramLen;
        this.paramValues = new Serializable[][] {stream.toArray(v -> new Serializable[v])};
    }

    private void writeBind(ByteArray array, PgPrepareDesc prepareDesc, Serializable... params) { // BIND
        // BIND
        array.putByte('B');
        int start = array.length();
        array.put(prepareDesc.bindPrefixBytes());
        //        array.putInt(0); //command-length
        //        array.putByte(0); // portal
        //        array.put(prepareDesc.statement()); //prepared statement
        //
        //        // Param columns are all in Binary format
        //        PgColumnFormat[] pformats = prepareDesc.paramFormats();
        //        int paramLen = pformats.length;
        //        array.putShort(paramLen);
        //        for (PgColumnFormat f : pformats) {
        //            array.putShort(f.supportsBinary() ? 1 : 0);
        //        }
        //        array.putShort(paramLen);

        PgColumnFormat[] pformats = prepareDesc.paramFormats();
        Attribute[] pattrs = prepareDesc.paramAttrs();
        for (int c = 0; c < pformats.length; c++) {
            Serializable param = params[c];
            if (param == null) {
                array.putInt(-1); // NULL value
            } else {
                int s2 = array.length();
                array.putInt(0); // value-length
                PgColumnFormat f = pformats[c];
                (f == null ? PgColumnFormat.VARCHAR : f).encoder().encode(array, pattrs[c], param);
                array.putInt(s2, array.length() - s2 - 4);
            }
        }

        //        // Result columns are all in Binary format
        //        PgColumnFormat[] rformats = prepareDesc.resultFormats();
        //        if (rformats.length > 0) {
        //            array.putShort(rformats.length);
        //            for (PgColumnFormat f : rformats) {
        //                array.putShort(f.supportsBinary() ? 1 : 0);
        //            }
        //        } else {
        //            array.putShort(1);
        //            array.putShort(1);
        //        }
        array.put(prepareDesc.bindPostfixBytes());

        array.putInt(start, array.length() - start);
        // EXECUTE
        writeExecute(array, fetchSize);
        // SYNC
        writeSync(array);
    }

    private void writeBind(ByteArray array, PgPrepareDesc prepareDesc) { // BIND
        // BIND
        array.put(prepareDesc.bindNoParamBytes());
        // EXECUTE
        writeExecute(array, fetchSize);
        // SYNC
        writeSync(array);
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        PgClientConnection pgconn = (PgClientConnection) conn;
        PgPrepareDesc prepareDesc = pgconn.getPgPrepareDesc(sql);
        this.syncCount = 0;
        if (prepareDesc != null) {
            this.sendPrepare = false;
            // 绑定参数
            if (prepareDesc.paramFormats().length > 0) {
                if (pkValues != null) {
                    for (Serializable pk : pkValues) {
                        writeBind(array, prepareDesc, pk);
                    }
                } else {
                    for (Serializable[] params : paramValues) {
                        writeBind(array, prepareDesc, params);
                    }
                }
            } else {
                writeBind(array, prepareDesc);
            }
            if (PgsqlDataSource.debug) {
                logger.log(
                        Level.FINEST,
                        "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", "
                                + getClass().getSimpleName() + ".sql: " + sql + ", BIND("
                                + (paramValues != null ? paramValues.length : 0) + "), EXECUTE, SYNC");
            }
        } else {
            prepareDesc = pgconn.createPgPrepareDesc(type, mode, info, sql, paramLen);
            this.sendPrepare = true;
            prepareDesc.writeTo(conn, array);
            // SYNC
            writeSync(array);
            // 绑定参数
            if (prepareDesc.paramFormats().length > 0) {
                if (pkValues != null) {
                    for (Serializable pk : pkValues) {
                        writeBind(array, prepareDesc, pk);
                    }
                } else {
                    for (Serializable[] params : paramValues) {
                        writeBind(array, prepareDesc, params);
                    }
                }
            } else {
                writeBind(array, prepareDesc);
            }
            if (PgsqlDataSource.debug) {
                logger.log(
                        Level.FINEST,
                        "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", "
                                + getClass().getSimpleName() + ".sql: " + sql + ", PARSE, DESCRIBE, BIND("
                                + (paramValues != null ? paramValues.length : 0) + "), EXECUTE, SYNC");
            }
        }
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 71 * hash + this.type;
        hash = 71 * hash + Objects.hashCode(this.sql);
        hash = 71 * hash + Objects.hashCode(this.mode);
        hash = 71 * hash + (this.sendPrepare ? 1 : 0);
        hash = 71 * hash + this.fetchSize;
        hash = 71 * hash + Arrays.deepHashCode(this.paramValues);
        hash = 71 * hash + Arrays.deepHashCode(this.pkValues);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PgReqExtended other = (PgReqExtended) obj;
        if (this.type != other.type) {
            return false;
        }
        if (this.mode != other.mode) {
            return false;
        }
        if (this.sendPrepare != other.sendPrepare) {
            return false;
        }
        if (this.fetchSize != other.fetchSize) {
            return false;
        }
        if (!Objects.equals(this.sql, other.sql)) {
            return false;
        }
        if (!Arrays.deepEquals(this.paramValues, other.paramValues)) {
            return false;
        }
        return Arrays.deepEquals(this.pkValues, other.pkValues);
    }

    public static enum PgReqExtendMode {
        FIND,
        FINDS,
        LIST_ALL,
        OTHER;
    }
}
