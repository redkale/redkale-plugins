/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.net.client.ClientConnection;
import org.redkale.source.EntityInfo;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class PgReqQuery extends PgClientRequest {

    protected EntityInfo info;

    protected String sql;

    public <T> PgReqQuery(EntityInfo<T> info, String sql) {
        this.info = info;
        this.sql = sql;
    }

    @Override
    public int getType() {
        return REQ_TYPE_QUERY;
    }

    @Override
    public String toString() {
        return "PgReqQuery{sql=" + sql + "}";
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        array.put((byte) 'Q');
        int start = array.length();
        array.putInt(0);
        writeUTF8String(array, sql);
        array.putInt(start, array.length() - start);
    }

}
