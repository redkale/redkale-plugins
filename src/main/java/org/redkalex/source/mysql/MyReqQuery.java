/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;
import static org.redkalex.source.mysql.Mysqls.*;

/**
 *
 * @author zhangjx
 */
public class MyReqQuery extends MyClientRequest {

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
        return "MyReqQuery_" + Objects.hashCode(this) + "{sql=" + sql + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        byte[] sqlbytes = sql.getBytes(StandardCharsets.UTF_8);
        Mysqls.writeUB3(array, 1 + sqlbytes.length);
        array.put(packetIndex);
        array.put(COM_QUERY);
        array.put(sqlbytes);
    }

}
