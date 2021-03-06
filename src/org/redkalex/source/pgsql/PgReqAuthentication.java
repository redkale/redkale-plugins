/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Arrays;
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
        array.put((byte) 0);
        array.putInt(start, array.length() - start);
    }

    public static class PgReqAuthPassword extends PgClientRequest {

        protected String username;

        protected String password;

        protected byte[] salt;

        public PgReqAuthPassword(String username, String password, byte[] salt) {
            this.username = username;
            this.password = password;
            this.salt = salt;
        }

        @Override
        public int getType() {
            return REQ_TYPE_AUTH;
        }

        @Override
        public String toString() {
            return "PgReqAuthPassword{username=" + username + ", password=" + password + ", salt=" + Arrays.toString(salt) + "}";
        }

        @Override
        public void accept(ClientConnection conn, ByteArray array) {
            array.put((byte) 'p');
            int start = array.length();
            array.putInt(0);
            if (salt == null) {
                array.put(password.getBytes(StandardCharsets.UTF_8));
            } else {
                MessageDigest md5;
                try {
                    md5 = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
                md5.update(password.getBytes(StandardCharsets.UTF_8));
                md5.update(username.getBytes(StandardCharsets.UTF_8));
                md5.update(Utility.binToHexString(md5.digest()).getBytes(StandardCharsets.UTF_8));
                md5.update(salt);
                array.put((byte) 'm', (byte) 'd', (byte) '5');
                array.put(Utility.binToHexString(md5.digest()).getBytes(StandardCharsets.UTF_8));
            }
            array.put((byte) 0);
            array.putInt(start, array.length() - start);
        }

    }
}
