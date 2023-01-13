/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.*;
import java.util.logging.Level;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;
import static org.redkalex.source.pgsql.PgClientCodec.logger;

/**
 *
 * @author zhangjx
 */
public class PgReqBatch extends PgClientRequest {

    protected String[] sqls;

    @Override
    public int getType() {
        return REQ_TYPE_BATCH;
    }

    public <T> PgReqBatch prepare(String... sqls) {
        super.prepare();
        this.sqls = sqls;
        return this;
    }

    @Override
    public String toString() {
        return "PgReqBatch_" + Objects.hashCode(this) + "{sqls=" + Arrays.toString(sqls) + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        for (String sql : sqls) {
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
            { // BIND
                array.putByte('B');
                int start = array.length();
                array.putInt(0);
                array.putByte(0); // portal  
                array.putByte(0); // prepared statement  
                array.putShort((short) 0); // 后面跟着的参数格式代码的数目(在下面的 C 中说明)。这个数值可以是零，表示没有参数，或者是参数都使用缺省格式(文本)
                array.putShort((short) 0);  //number of format codes 参数格式代码。目前每个都必须是零(文本)或者一(二进制)。
                array.putShort((short) 0);// number of parameters 后面跟着的参数值的数目(可能为零)。这些必须和查询需要的参数个数匹配。
                array.putInt(start, array.length() - start);
            }
            writeExecute(array, 0); // EXECUTE
            writeSync(array); // SYNC
            if (PgsqlDataSource.debug) logger.log(Level.FINEST, "[" + Utility.nowMillis() + "] [" + Thread.currentThread().getName() + "]: " + conn + ", " + getClass().getSimpleName() + ".PARSE: " + sql + ", DESCRIBE, BIND, EXECUTE, SYNC");
        }
    }
}
