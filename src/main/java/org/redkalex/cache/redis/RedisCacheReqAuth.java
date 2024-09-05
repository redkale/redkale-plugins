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
public class RedisCacheReqAuth extends RedisCacheRequest {

    private static final byte[] PS = "AUTH\r\n".getBytes(StandardCharsets.UTF_8);

    protected String password;

    public RedisCacheReqAuth(String password) {
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray writer) {
        byte[] pwd = password.getBytes();
        writer.put(mutliLengthBytes(2));
        writer.put(bulkLengthBytes(4));
        writer.put(PS);

        writer.put(bulkLengthBytes(pwd.length));
        writer.put(pwd);
        writer.put(CRLF);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{AUTH " + password + "}";
    }
}
