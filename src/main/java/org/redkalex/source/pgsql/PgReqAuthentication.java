/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class PgReqAuthentication extends PgClientRequest {

    protected String username;

    protected String password;

    protected String database;

    public PgReqAuthentication(String username, String password, String database) {
        this.username = username;
        this.password = password;
        this.database = database;
    }

    @Override
    public int getType() {
        return REQ_TYPE_AUTH;
    }

    @Override
    public String toString() {
        return "PgReqAuthentication{username=" + username + ", password=" + password + ", database=" + database + "}";
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        int start = array.length();
        array.putInt(0);
        array.putInt(196608);
        writeUTF8String(writeUTF8String(array, "user"), username);
        writeUTF8String(writeUTF8String(array, "database"), database);
        writeUTF8String(writeUTF8String(array, "client_encoding"), "UTF8");
        array.putByte(0);
        array.putInt(start, array.length() - start);
    }

}
