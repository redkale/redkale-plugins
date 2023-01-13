/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.Objects;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class PgReqQuery extends PgClientRequest {

    protected String sql;

    public <T> void prepare(String sql) {
        super.prepare();
        this.sql = sql;
    }

    @Override
    public int getType() {
        return REQ_TYPE_QUERY;
    }
    
    @Override
    public String toString() {
        return "PgReqQuery_" + Objects.hashCode(this) + "{sql=" + sql + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        array.putByte('Q');
        int start = array.length();
        array.putInt(0);
        writeUTF8String(array, sql);
        array.putInt(start, array.length() - start);
    }

}
