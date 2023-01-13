/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class RedisCacheReqDB extends RedisCacheRequest {

    protected int db;

    public RedisCacheReqDB(int db) {
        this.db = db;
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray writer) {
        writer.put((byte) '*');
        writer.put((byte) '2');
        writer.put((byte) '\r', (byte) '\n');
        writer.put((byte) '$');
        writer.put((byte) '6');
        writer.put((byte) '\r', (byte) '\n');
        writer.put("SELECT".getBytes(StandardCharsets.UTF_8));
        writer.put((byte) '\r', (byte) '\n');

        byte[] dbs = String.valueOf(db).getBytes(StandardCharsets.UTF_8);
        writer.put((byte) '$');
        writer.put(String.valueOf(dbs.length).getBytes(StandardCharsets.UTF_8));
        writer.put((byte) '\r', (byte) '\n');
        writer.put(dbs);
        writer.put((byte) '\r', (byte) '\n');

    }
}
