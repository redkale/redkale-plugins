/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;
import static org.redkalex.source.pgsql.PgClientCodec.logger;

/**
 *
 * @author zhangjx
 */
public class PgReqExtended extends PgClientRequest {

    private static final Object[][] ONE_EMPTY_PARAMS = new Object[][]{new Object[0]};

    protected int type;

    protected int fetchSize;

    protected String sql;

    protected PgReqExtendMode mode;

    protected boolean sendPrepare;

    protected Attribute[] resultAttrs;

    protected Attribute[] paramAttrs;

    protected Object[][] paramValues;

    protected boolean finds;

    protected int[] findsCount;

    public PgReqExtended() {
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + Objects.hashCode(this) + "{sql = '" + sql + "', type = " + getType() + ", mergeCount = " + getMergeCount() + ", paramValues = " + (paramValues != null && paramValues.length > 10 ? ("size " + paramValues.length) : JsonConvert.root().convertTo(paramValues)) + "}";
    }

    @Override
    public int getType() {
        return type;
    }

    public <T> void prepare(int type, PgReqExtendMode mode, String sql, int fetchSize, final Attribute<T, Serializable>[] resultAttrs, final Attribute<T, Serializable>[] paramAttrs, final Object[]... paramValues) {
        super.prepare();
        this.type = type;
        this.mode = mode;
        this.sql = sql;
        this.fetchSize = fetchSize;
        this.resultAttrs = resultAttrs;
        this.paramAttrs = paramAttrs;
        this.paramValues = paramValues;
    }

//    @Override //是否能合并
//    protected boolean canMerge(ClientConnection conn) {
//        if (this.type != REQ_TYPE_EXTEND_QUERY) return false;
//        // && this.mode != PgReqExtendMode.FINDS  暂时屏蔽
//        if (this.mode != PgReqExtendMode.FIND && this.mode != PgReqExtendMode.LIST_ALL) return false; //只支持find sql和list all 
//        AtomicBoolean prepared = ((PgClientConnection) conn).getPrepareFlag(sql);
//        return prepared.get();
//    }

//    @Override //合并成功了返回true
//    protected boolean merge(ClientConnection conn, ClientRequest other) {
//        PgClientRequest req = (PgClientRequest) other;
//        if (this.workThread != req.getWorkThread()) return false;
//        if (req.getType() != REQ_TYPE_EXTEND_QUERY) return false;
//        PgReqExtended extreq = (PgReqExtended) req;
//        if (this.mode != extreq.mode) return false;
//        if (this.paramValues != null && this.paramValues.length > 256) return false;
//        if (extreq.mode != PgReqExtendMode.FIND && extreq.mode != PgReqExtendMode.FINDS && extreq.mode != PgReqExtendMode.LIST_ALL) return false; //只支持find sql和list all 
//        if (!this.sql.equals(extreq.sql)) return false;
//        if (mode == PgReqExtendMode.FINDS) {
//            if (paramValues.length + extreq.paramValues.length > 100) return false;
//            if (findsCount == null) findsCount = new int[]{paramValues.length};
//            this.findsCount = Utility.append(findsCount, extreq.paramValues.length);
//        }
//        this.paramValues = Utility.append(paramValues == null || paramValues.length == 0 ? ONE_EMPTY_PARAMS : paramValues,
//            extreq.paramValues == null || extreq.paramValues.length == 0 ? ONE_EMPTY_PARAMS : extreq.paramValues);
//        return true;
//    }

    private void writeParse(ByteArray array, Long statementIndex) { // PARSE
        array.putByte('P');
        int start = array.length();
        array.putInt(0);
        if (statementIndex == null) {
            array.putByte(0); // unnamed prepared statement
        } else {
            array.putLong(statementIndex);
        }
        writeUTF8String(array, sql);
        array.putShort(0); // no parameter types
        array.putInt(start, array.length() - start);
    }

    private void writeDescribe(ByteArray array, Long statementIndex) { // DESCRIBE
        array.putByte('D');
        array.putInt(4 + 1 + (statementIndex == null ? 1 : 8));
        if (statementIndex == null) {
            array.putByte('S');
            array.putByte(0);
        } else {
            array.putByte('S');
            array.putLong(statementIndex);
        }
    }

    private void writeBind(ByteArray array, Long statementIndex) { // BIND
        if (paramValues != null && paramValues.length > 0) {
            for (Object[] params : paramValues) {
                { // BIND
                    array.putByte('B');
                    int start = array.length();
                    array.putInt(0);
                    array.putByte(0); // portal
                    if (statementIndex == null) {
                        array.putByte(0); // prepared statement
                    } else {
                        array.putLong(statementIndex);
                    }

                    int size = params == null ? 0 : params.length;
                    // number of format codes  // 后面跟着的参数格式代码的数目(在下面的 C 中说明)。这个数值可以是零，表示没有参数，或者是参数都使用缺省格式(文本)
                    if (size == 0 || paramAttrs == null) {
                        array.putShort(0);  //参数全部为文本格式
                    } else {
                        array.putShort(0);
                    }

                    // number of parameters //number of format codes 参数格式代码。目前每个都必须是0(文本)或者1(二进制)。
                    if (size == 0) {
                        array.putShort(0); //结果全部为文本格式
                    } else {
                        array.putShort(size);
                        int i = -1;
                        for (Object param : params) {
                            PgsqlFormatter.encodePrepareParamValue(array, info, false, paramAttrs == null ? null : paramAttrs[++i], param);
                        }
                    }

                    if (type == REQ_TYPE_EXTEND_QUERY && resultAttrs != null) {   //Text format
                        // Result columns are all in Binary format
                        array.putShort(1);
                        array.putShort(1);
                    } else {
                        array.putShort(0);
                    }
                    array.putInt(start, array.length() - start);
                }
                writeExecute(array, fetchSize); // EXECUTE
            writeSync(array); //SYNC
            }
        } else {
            { // BIND
                array.putByte('B');
                int start = array.length();
                array.putInt(0);
                array.putByte(0); // portal  
                if (statementIndex == null) {
                    array.putByte(0); // prepared statement
                } else {
                    array.putLong(statementIndex);
                }
                array.putShort(0); // 后面跟着的参数格式代码的数目(在下面的 C 中说明)。这个数值可以是零，表示没有参数，或者是参数都使用缺省格式(文本)
                array.putShort(0);  //number of format codes 参数格式代码。目前每个都必须是0(文本)或者1(二进制)。

                if (type == REQ_TYPE_EXTEND_QUERY && resultAttrs != null) {   //Text format
                    // Result columns are all in Binary format
                    array.putShort(1);
                    array.putShort(1);
                } else {
                    array.putShort(0);
                }
                array.putInt(start, array.length() - start);
            }
            writeExecute(array, fetchSize); // EXECUTE
            writeSync(array); //SYNC            
        }
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        PgClientConnection pgconn = (PgClientConnection) conn;
        AtomicBoolean prepared = pgconn.getPrepareFlag(sql);
        this.syncedCount = 0;
        if (prepared.get()) {
            this.sendPrepare = false;
            writeBind(array, pgconn.getStatementIndex(sql));
            if (PgsqlDataSource.debug) logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", " + getClass().getSimpleName() + ".PARSE: " + sql + ", DESCRIBE, BIND(" + (paramValues != null ? paramValues.length : 0) + "), EXECUTE, SYNC");
        } else {
            Long statementIndex = pgconn.createStatementIndex(sql);
            prepared.set(true);
            this.sendPrepare = true;
            writeParse(array, statementIndex);
            writeDescribe(array, statementIndex);
            //绑定参数
            writeBind(array, statementIndex);
            if (PgsqlDataSource.debug) logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", " + getClass().getSimpleName() + ".PARSE: " + sql + ", DESCRIBE, BIND(" + (paramValues != null ? paramValues.length : 0) + "), EXECUTE, SYNC");
        }
    }

    public static enum PgReqExtendMode {
        FIND, FINDS, LIST_ALL, OTHER;
    }
}
