/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class MyReqAuthentication extends MyClientRequest {

    private static final byte[] FILLER = new byte[23];

    protected MyRespHandshakeResultSet handshake;

    protected String username;

    protected String password;

    protected String database;

    protected Properties attributes;

    public MyReqAuthentication(MyRespHandshakeResultSet handshake, String username, String password, String database, Properties attributes) {
        this.handshake = handshake;
        this.username = username;
        this.password = password;
        this.database = database;
        this.attributes = attributes;
        this.packetIndex = 1;
    }

    // 参考包: com.mysql.cj.protocol.a.authentication
    private byte[] formatPassword(final String password) {
        if (password == null || password.isEmpty()) return null;
        byte[] seed = Utility.append(handshake.seed, handshake.seed2);
        if ("caching_sha2_password".equals(handshake.authPluginName)) {
            return Mysqls.scrambleCachingSha2(password, seed);
        } else if ("mysql_native_password".equals(handshake.authPluginName)) {
            return Mysqls.scramble411(password, seed);
        } else {
            return null;
        }
    }

    @Override
    public int getType() {
        return REQ_TYPE_AUTH;
    }

    @Override
    public String toString() {
        return "MyReqAuthentication_" + Objects.hashCode(this) + "{username=" + username + ", password=" + password + ", database=" + database + "}";
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        final int startPos = array.length();
        Mysqls.writeUB3(array, 0);
        array.putByte(packetIndex);
        int capabilitiesFlags = getClientCapabilities();
        if (database != null && !database.isEmpty()) {
            capabilitiesFlags |= Mysqls.CLIENT_CONNECT_WITH_DB;
        }
        if (attributes != null && !attributes.isEmpty()) {
            capabilitiesFlags |= Mysqls.CLIENT_CONNECT_ATTRS;
        }
        Mysqls.writeUB4(array, capabilitiesFlags);
        Mysqls.writeUB4(array, Mysqls.MAX_PACKET_SIZE);
        array.putByte(45); //utf8mb4_general_ci("utf8mb4", "UTF-8", 45)
        array.put(FILLER);
        if (username == null || username.isEmpty()) {
            array.putByte(0);
        } else {
            Mysqls.writeWithNull(array, username.getBytes());
        }
        byte[] pwds = formatPassword(password);
        if (pwds == null) {
            array.putByte(0);
        } else {
            if ((capabilitiesFlags & Mysqls.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
                Mysqls.writeWithLength(array, pwds);
            } else if ((capabilitiesFlags & Mysqls.CLIENT_SECURE_CONNECTION) != 0) {
                array.putByte(pwds.length);
                array.put(pwds);
            } else {
                array.putByte(0);
            }
        }
        if ((capabilitiesFlags & Mysqls.CLIENT_CONNECT_WITH_DB) != 0) {
            Mysqls.writeWithNull(array, database.getBytes());
        }
        if ((capabilitiesFlags & Mysqls.CLIENT_PLUGIN_AUTH) != 0) {
            Mysqls.writeWithNull(array, handshake.authPluginName.getBytes());
        }
        if ((capabilitiesFlags & Mysqls.CLIENT_CONNECT_ATTRS) != 0) {
            attributes.put("_client_name", "redkale-mysql-client");
            ByteArray attrsArray = new ByteArray();
            attributes.forEach((k, v) -> {
                Mysqls.writeWithLength(attrsArray, k.toString().getBytes(StandardCharsets.UTF_8));
                Mysqls.writeWithLength(attrsArray, v.toString().getBytes(StandardCharsets.UTF_8));
            });
            Mysqls.writeWithLength(array, attrsArray.getBytes());
        }
        Mysqls.writeUB3(array, startPos, array.length() - startPos - 4);
        if (MysqlDataSource.debug) MyClientCodec.logger.log(Level.FINEST, Utility.nowMillis() + ": " + Thread.currentThread().getName() + ": " + conn + ", 发送 MyReqAuthentication length=" + array.length());
    }

    private int getClientCapabilities() {
        int flag = 0;
        flag |= Mysqls.CLIENT_DEPRECATE_EOF; //5.7.5以后，OK_Packet包含ERR_Packet格式, 必须包含属性
        flag |= Mysqls.CLIENT_PLUGIN_AUTH;
        flag |= Mysqls.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        flag |= Mysqls.CLIENT_LONG_PASSWORD;
        flag |= Mysqls.CLIENT_LONG_FLAG;
        flag |= Mysqls.CLIENT_SECURE_CONNECTION;
        flag |= Mysqls.CLIENT_PROTOCOL_41;
        flag |= Mysqls.CLIENT_TRANSACTIONS;
        flag |= Mysqls.CLIENT_MULTI_STATEMENTS;
        flag |= Mysqls.CLIENT_MULTI_RESULTS;
        flag |= Mysqls.CLIENT_PS_MULTI_RESULTS;
        flag |= Mysqls.CLIENT_IGNORE_SPACE;
        flag |= Mysqls.CLIENT_INTERACTIVE;
        flag |= Mysqls.CLIENT_IGNORE_SIGPIPE;
        flag |= Mysqls.CLIENT_FOUND_ROWS;
        flag &= handshake.serverCapabilities;
        return flag;
    }

    public static class MyReqAuthSwitch extends MyClientRequest {

        protected byte[] scrambledPassword;

        public MyReqAuthSwitch(String pluginName, byte[] seed, String password) {
            if ("caching_sha2_password".equals(pluginName)) {
                scrambledPassword = Mysqls.scrambleCachingSha2(password, seed);
            } else if ("mysql_native_password".equals(pluginName)) {
                scrambledPassword = Mysqls.scramble411(password, seed);
            } else if ("mysql_clear_password".equals(pluginName)) {
                scrambledPassword = password.getBytes(StandardCharsets.UTF_8);
            } else {
                throw new UnsupportedOperationException("Not supported password plugin: " + pluginName);
            }
        }

        @Override
        public int getType() {
            return REQ_TYPE_AUTH;
        }

        @Override
        public void accept(ClientConnection conn, ByteArray array) {
            Mysqls.writeUB3(array, scrambledPassword.length);
            array.putByte(3);
            array.put(scrambledPassword);
            if (MysqlDataSource.debug) MyClientCodec.logger.log(Level.FINEST, Utility.nowMillis() + ": " + conn + ", 发送 MyReqAuthSwitch length=" + array.length());
        }

    }
}
