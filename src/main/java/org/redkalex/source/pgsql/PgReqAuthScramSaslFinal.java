/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class PgReqAuthScramSaslFinal extends PgClientRequest {

    private static final byte[] CLIENT_KEY_HMAC_KEY = "Client Key".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SERVER_KEY_HMAC_KEY = "Server Key".getBytes(StandardCharsets.UTF_8);

    protected final PgReqAuthScramSaslContinueResult bean;

    protected String authMessage;

    protected byte[] serverKey;

    protected String clientFinalMessage;

    public PgReqAuthScramSaslFinal(PgReqAuthScramSaslContinueResult bean) {
        this.bean = bean;
        String fullNonce = bean.clientNonce + bean.serverNonce;
        byte[] saltedPassword = saltedPassword(bean.password, Base64.getDecoder().decode(bean.salt), bean.iteration);
        byte[] clientKey = hmac(saltedPassword, CLIENT_KEY_HMAC_KEY);
        this.serverKey = hmac(saltedPassword, SERVER_KEY_HMAC_KEY);
        byte[] storedKey = digest(clientKey);
        String gs2Header = "n=" + PgReqAuthScramPassword.toSaslName(bean.username) + ",r=" + bean.clientNonce;
        String cbind = "c=" + Base64.getEncoder().encodeToString("n,,".getBytes(StandardCharsets.UTF_8)) + ",r=" + fullNonce;
        this.authMessage = gs2Header + "," + bean.saslmsg + "," + cbind;
        byte[] proof = xor(clientKey, hmac(storedKey, authMessage.getBytes(StandardCharsets.UTF_8)));
        this.clientFinalMessage = cbind + ",p=" + Base64.getEncoder().encodeToString(proof);
    }

    @Override
    public int getType() {
        return REQ_TYPE_AUTH;
    }

    public void checkFinal(String serverFinalMessage, ByteBuffer buffer) {
        String[] msgs = serverFinalMessage.split(",");
        if (msgs[0].startsWith("v=")) {
            byte[] verifier = Base64.getDecoder().decode(msgs[0].substring(2));
            byte[] localsign = hmac(serverKey, authMessage.getBytes(StandardCharsets.UTF_8));
            if (!Arrays.equals(localsign, verifier)) throw new IllegalArgumentException("Invalid server SCRAM signature in " + msgs[0] + ", buffer.remain = " + buffer.remaining());
        } else if (msgs[0].startsWith("e=")) {
            throw new IllegalArgumentException(msgs[0].substring(2));
        } else {
            throw new IllegalArgumentException("Invalid server SCRAM message");
        }
    }

    @Override
    public String toString() {
        return "PgReqAuthScramSaslFinal{msg=" + clientFinalMessage + "}";
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        array.putByte('p');
        int start = array.length();
        array.putInt(0);
        array.put(clientFinalMessage.getBytes(StandardCharsets.UTF_8));
        array.putInt(start, array.length() - start);
    }

    protected static byte[] xor(byte[] value1, byte[] value2) {
        byte[] result = new byte[value1.length];
        for (int i = 0; i < value1.length; i++) {
            result[i] = (byte) (value1[i] ^ value2[i]);
        }
        return result;
    }

    protected static byte[] digest(byte[] message) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static byte[] hmac(byte[] saltedPassword, byte[] message) {
        try {
            SecretKeySpec spec = new SecretKeySpec(saltedPassword, "HmacSHA256");
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(spec);
            return mac.doFinal(message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static byte[] saltedPassword(String password, byte[] salt, int iterations) {
        char[] pwds = (password == null ? "" : password).toCharArray();
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            PBEKeySpec spec = new PBEKeySpec(pwds, salt, iterations, 256);
            SecretKey key = factory.generateSecret(spec);
            return key.getEncoded();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
