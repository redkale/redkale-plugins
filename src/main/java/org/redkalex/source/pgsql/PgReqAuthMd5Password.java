/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import org.redkale.net.client.ClientConnection;
import org.redkale.source.SourceException;
import org.redkale.util.*;

/** @author zhangjx */
public class PgReqAuthMd5Password extends PgClientRequest {

    protected String username;

    protected String password;

    protected byte[] salt;

    public PgReqAuthMd5Password(String username, String password, byte[] salt) {
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
        return "PgReqAuthMd5Password_" + Objects.hashCode(this) + "{username=" + username + ", password=" + password
                + ", salt=" + Arrays.toString(salt) + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        array.putByte('p');
        int start = array.length();
        array.putInt(0);
        if (salt == null) {
            array.put(password.getBytes(StandardCharsets.UTF_8));
        } else {
            MessageDigest md5;
            try {
                md5 = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new SourceException(e);
            }
            md5.update(password.getBytes(StandardCharsets.UTF_8));
            md5.update(username.getBytes(StandardCharsets.UTF_8));
            md5.update(Utility.binToHexString(md5.digest()).getBytes(StandardCharsets.UTF_8));
            md5.update(salt);
            array.put((byte) 'm', (byte) 'd', (byte) '5');
            array.put(Utility.binToHexString(md5.digest()).getBytes(StandardCharsets.UTF_8));
        }
        array.putByte(0);
        array.putInt(start, array.length() - start);
    }
}
