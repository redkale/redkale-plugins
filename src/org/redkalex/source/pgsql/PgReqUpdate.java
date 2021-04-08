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

/**
 *
 * @author zhangjx
 */
public class PgReqUpdate extends PgClientRequest {

    protected EntityInfo info;

    protected int fetchSize;

    protected Attribute[] attrs;

    protected String sql;

    protected Object[][] parameters;

    public <T> PgReqUpdate(final EntityInfo<T> info, String sql, int fetchSize, final Attribute<T, Serializable>[] attrs, final Object[]... parameters) {
        this.info = info;
        this.sql = sql;
        this.fetchSize = fetchSize;
        this.attrs = attrs;
        this.parameters = parameters;
    }

    @Override
    public int getType() {
        return REQ_TYPE_UPDATE;
    }

    @Override
    public String toString() {
        return "PgReqUpdate{sql=" + sql + ", parameters=" + JsonConvert.root().convertTo(parameters) + "}";
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        { // PARSE
            array.put((byte) 'P');
            int start = array.length();
            array.putInt(0);
            array.put((byte) 0); // unnamed prepared statement
            writeUTF8String(array, sql);
            array.putShort((short) 0); // no parameter types
            array.putInt(start, array.length() - start);
        }
        { // DESCRIBE
            array.put((byte) 'D');
            array.putInt(4 + 1 + 1);
            array.put((byte) 'S');
            array.put((byte) 0);
        }
        if (parameters != null && parameters.length > 0) {
            for (Object[] params : parameters) {
                { // BIND
                    array.put((byte) 'B');
                    int start = array.length();
                    array.putInt(0);
                    array.put((byte) 0); // portal
                    array.put((byte) 0); // prepared statement
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
                array.put((byte) 0); // prepared statement  
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
        { // SYNC
            array.put((byte) 'S');
            array.putInt(4);
        }
    }

}
