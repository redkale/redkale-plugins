/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;
import static org.redkalex.source.mysql.Mysqls.COM_QUERY;

/**
 *
 * @author zhangjx
 */
public class MyReqUpdate extends MyClientRequest {

    protected int fetchSize;

    protected Attribute[] attrs;

    protected String sql;

    protected Object[][] parameters;

    public <T> MyReqUpdate prepare(String sql) {
        prepare(sql, 0, null);
        return this;
    }

    public <T> MyReqUpdate prepare(String sql, int fetchSize, final Attribute<T, Serializable>[] attrs, final Object[]... parameters) {
        super.prepare();
        this.sql = sql;
        this.fetchSize = fetchSize;
        this.attrs = attrs;
        this.parameters = parameters;
        return this;
    }

    @Override
    public int getType() {
        return REQ_TYPE_UPDATE;
    }

    @Override
    public String toString() {
        return "MyReqUpdate_" + Objects.hashCode(this) + "{sql=" + sql + ", parameters=" + JsonConvert.root().convertTo(parameters) + "}";
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        byte[] sqlbytes = sql.getBytes(StandardCharsets.UTF_8);
        Mysqls.writeUB3(array, 1 + sqlbytes.length);
        array.put(packetIndex);
        array.put(COM_QUERY);
        array.put(sqlbytes);
    }

}
