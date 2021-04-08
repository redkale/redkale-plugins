/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.client.ClientConnection;
import org.redkale.source.EntityInfo;
import org.redkale.util.*;
import static org.redkalex.source.pgsql.PgClientRequest.formatPrepareParam;

/**
 *
 * @author zhangjx
 */
public class PgReqExtendedCommand extends PgClientRequest {

    protected int type;

    protected EntityInfo info;

    protected int fetchSize;

    protected Attribute[] attrs;

    protected String sql;

    protected Object[][] parameters;

    protected long statement;

    //只用一次
    protected boolean firstPrepare;

    public <T> PgReqExtendedCommand(int type, long statement, final EntityInfo<T> info, String sql, int fetchSize, final Attribute<T, Serializable>[] attrs, final Object[]... parameters) {
        this.type = type;
        this.statement = statement;
        this.info = info;
        this.sql = sql;
        this.fetchSize = fetchSize;
        this.attrs = attrs;
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        return "PgReqExtendedCommand{\"sql\":\"" + sql + ", type:" + getType() + "\", params:" + JsonConvert.root().convertTo(parameters) + ", hash:" + hashCode() + ", firstPrepare:" + firstPrepare + "}";
    }

    @Override
    public int getType() {
        return type;
    }

    private void writeParse(ByteArray array) { // PARSE
        array.put((byte) 'P');
        int start = array.length();
        array.putInt(0);
        if (statement == 0) {
            array.put((byte) 0); // unnamed prepared statement
        } else {
            array.putLong(statement);
        }
        writeUTF8String(array, sql);
        array.putShort((short) 0); // no parameter types
        array.putInt(start, array.length() - start);
    }

    private void writeDescribe(ByteArray array) { // DESCRIBE
        array.put((byte) 'D');
        array.putInt(4 + 1 + (statement == 0 ? 1 : 8));
        if (statement == 0) {
            array.put((byte) 'S');
            array.put((byte) 0);
        } else {
            array.put((byte) 'S');
            array.putLong(statement);
        }
    }

    private void writeBind(ByteArray array) { // BIND
        if (parameters != null && parameters.length > 0) {
            for (Object[] params : parameters) {
                { // BIND
                    array.put((byte) 'B');
                    int start = array.length();
                    array.putInt(0);
                    array.put((byte) 0); // portal
                    if (statement == 0) {
                        array.put((byte) 0); // prepared statement
                    } else {
                        array.putLong(statement);
                    }
                    array.putShort((short) 0); // number of format codes
                    if (params == null || params.length == 0) {
                        array.putShort((short) 0); // number of parameters
                    } else {
                        array.putShort((short) params.length); // number of parameters
                        int i = -1;
                        for (Object param : params) {
                            byte[] bs = formatPrepareParam(info, attrs == null ? null : attrs[++i], param);
                            if (bs == null) {
                                array.putInt(-1);
                            } else {
                                array.putInt(bs.length);
                                array.put(bs);
                            }
                        }
                    }
                    array.putShort((short) 0);
                    array.putInt(start, array.length() - start);
                }
                { // EXECUTE
                    array.put((byte) 'E');
                    array.putInt(4 + 1 + 4);
                    array.put((byte) 0); //portal 要执行的入口的名字(空字符串选定未命名的入口)。
                    array.putInt(fetchSize); //要返回的最大行数，如果入口包含返回行的查询(否则忽略)。零标识"没有限制"。
                }
            }
        } else {
            { // BIND
                array.put((byte) 'B');
                int start = array.length();
                array.putInt(0);
                array.put((byte) 0); // portal  
                if (statement == 0) {
                    array.put((byte) 0); // prepared statement
                } else {
                    array.putLong(statement);
                }
                array.putShort((short) 0); // 后面跟着的参数格式代码的数目(在下面的 C 中说明)。这个数值可以是零，表示没有参数，或者是参数都使用缺省格式(文本)
                array.putShort((short) 0);  //number of format codes 参数格式代码。目前每个都必须是零(文本)或者一(二进制)。
                array.putShort((short) 0);// number of parameters 后面跟着的参数值的数目(可能为零)。这些必须和查询需要的参数个数匹配。
                array.putInt(start, array.length() - start);
            }
            { // EXECUTE
                array.put((byte) 'E');
                array.putInt(4 + 1 + 4);
                array.put((byte) 0); //portal 要执行的入口的名字(空字符串选定未命名的入口)。
                array.putInt(fetchSize); //要返回的最大行数，如果入口包含返回行的查询(否则忽略)。零标识"没有限制"。
            }
        }
    }

    private void writeSync(ByteArray array) { // SYNC
        array.put((byte) 'S');
        array.putInt(4);
    }

    @Override
    public void accept(ClientConnection conn0, ByteArray array) {
        final PgClientConnection conn = (PgClientConnection) conn0;
        if (conn.currExtendedCacheNode == null) {
            writeParse(array);
            writeDescribe(array);
            writeBind(array);
            writeSync(array);
            return;
        }
        if (firstPrepare) {
            writeParse(array);
            writeDescribe(array);
            writeSync(array);
        } else {
            writeBind(array);
            writeSync(array);
        }
    }

}
