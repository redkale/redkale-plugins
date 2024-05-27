/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class RedisCacheReqDB extends RedisCacheRequest {

    protected int db;

    public RedisCacheReqDB(int db) {
        this.db = db;
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray writer) {
        writer.put(mutliLengthBytes(2));
        writer.put(bulkLengthBytes(6));
        writer.put("SELECT\r\n".getBytes(StandardCharsets.UTF_8));

        byte[] dbs = String.valueOf(db).getBytes(StandardCharsets.UTF_8);
        writer.put(bulkLengthBytes(dbs.length));
        writer.put(dbs);
        writer.put(CRLF);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{SELECT " + db + "}";
    }
}
