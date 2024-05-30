/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import static org.redkalex.source.pgsql.PgClientCodec.logger;

import java.io.Serializable;
import java.util.Objects;
import java.util.logging.Level;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;

/** @author zhangjx */
public class PgReqUpdate extends PgClientRequest {

    protected int fetchSize;

    protected String sql;

    protected Attribute[] paramAttrs;

    protected Object[][] paramValues;

    public <T> PgReqUpdate prepare(String sql) {
        prepare(sql, 0, null, null);
        return this;
    }

    public <T> PgReqUpdate prepare(
            String sql, int fetchSize, final Attribute<T, Serializable>[] paramAttrs, final Object[][] paramValues) {
        super.prepare();
        this.sql = sql;
        this.fetchSize = fetchSize;
        this.paramAttrs = paramAttrs;
        this.paramValues = paramValues;
        return this;
    }

    @Override
    public int getType() {
        return REQ_TYPE_UPDATE;
    }

    @Override
    public String toString() {
        return "PgReqUpdate_" + Objects.hashCode(this) + "{sql=" + sql + ", paramValues="
                + JsonConvert.root().convertTo(paramValues) + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        { // PARSE
            array.putByte('P');
            int start = array.length();
            array.putInt(0);
            array.putByte(0); // unnamed prepared statement
            writeUTF8String(array, sql);
            array.putShort((short) 0); // no parameter types
            array.putInt(start, array.length() - start);
        }
        { // DESCRIBE
            array.putByte('D');
            array.putInt(4 + 1 + 1);
            array.putByte('S');
            array.putByte(0);
        }
        if (paramValues != null && paramValues.length > 0) {
            for (Object[] params : paramValues) {
                { // BIND
                    array.putByte('B');
                    int start = array.length();
                    array.putInt(0);
                    array.putByte(0); // portal
                    array.putByte(0); // prepared statement
                    array.putShort((short) 0); // number of format codes
                    if (params == null || params.length == 0) {
                        array.putShort((short) 0); // number of parameters
                    } else {
                        array.putShort((short) params.length); // number of parameters
                        int i = -1;
                        for (Object param : params) {
                            PgsqlFormatter.encodePrepareParamValue(
                                    array, info, false, paramAttrs == null ? null : paramAttrs[++i], param);
                        }
                    }
                    array.putShort((short) 0);
                    array.putInt(start, array.length() - start);
                }
                writeExecute(array, fetchSize); // EXECUTE
            }
            writeSync(array); // SYNC
            if (PgsqlDataSource.debug) {
                logger.log(
                        Level.FINEST,
                        "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", "
                                + getClass().getSimpleName() + ".PARSE: " + sql + ", DESCRIBE, BIND("
                                + paramValues.length + "), EXECUTE, SYNC");
            }
        } else {
            { // BIND
                array.putByte('B');
                int start = array.length();
                array.putInt(0);
                array.putByte(0); // portal
                array.putByte(0); // prepared statement
                array.putShort((short) 0); // 后面跟着的参数格式代码的数目(在下面的 C 中说明)。这个数值可以是零，表示没有参数，或者是参数都使用缺省格式(文本)
                array.putShort((short) 0); // number of format codes 参数格式代码。目前每个都必须是零(文本)或者一(二进制)。
                array.putShort((short) 0); // number of parameters 后面跟着的参数值的数目(可能为零)。这些必须和查询需要的参数个数匹配。
                array.putInt(start, array.length() - start);
            }
            writeExecute(array, fetchSize); // EXECUTE
            writeSync(array); // SYNC
            if (PgsqlDataSource.debug) {
                logger.log(
                        Level.FINEST,
                        "[" + Times.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", "
                                + getClass().getSimpleName() + ".PARSE: " + sql + ", DESCRIBE, BIND(0), EXECUTE, SYNC");
            }
        }
    }
}
