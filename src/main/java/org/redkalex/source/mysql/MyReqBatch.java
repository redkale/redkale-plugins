/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;
import static org.redkalex.source.mysql.Mysqls.COM_QUERY;

/**
 *
 * @author zhangjx
 */
public class MyReqBatch extends MyClientRequest {

    protected String[] sqls;

    @Override
    public int getType() {
        return REQ_TYPE_BATCH;
    }

    public <T> MyReqBatch prepare(String... sqls) {
        super.prepare();
        this.sqls = sqls;
        return this;
    }

    @Override
    public String toString() {
        return "MyReqBatch_" + Objects.hashCode(this) + "{sqls=" + Arrays.toString(sqls) + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        for (String sql : sqls) {
            byte[] sqlbytes = sql.getBytes(StandardCharsets.UTF_8);
            Mysqls.writeUB3(array, 1 + sqlbytes.length);
            array.put(packetIndex);
            array.put(COM_QUERY);
            array.put(sqlbytes);
        }
    }
}
