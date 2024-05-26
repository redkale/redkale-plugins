/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class MyRespHandshakeDecoder extends MyRespDecoder<MyRespHandshakeResultSet> {

    public static final MyRespHandshakeDecoder instance = new MyRespHandshakeDecoder();

    private static final int NONCE_LENGTH = 20;

    private static final int AUTH_PLUGIN_DATA_PART1_LENGTH = 8;

    private static final byte[] FILLER_10 = new byte[10];

    @Override
    public MyRespHandshakeResultSet read(
            MyClientConnection conn,
            ByteBuffer buffer,
            int length,
            byte index,
            ByteArray array,
            MyClientRequest request,
            MyResultSet dataset)
            throws SQLException {
        MyRespHandshakeResultSet rs = new MyRespHandshakeResultSet();
        // protocol version
        rs.protocolVersion = buffer.get() & 0xff;
        if (rs.protocolVersion == 0xff) { // 0
            int vendorCode = Mysqls.readUB2(buffer);
            String desc = Mysqls.readASCIIString(buffer, array);
            throw new SQLException(desc + " (vendorCode=" + vendorCode + ")", null, vendorCode);
        }
        if (rs.protocolVersion < 10) {
            throw new SQLException("Not supported protocolVersion(" + rs.protocolVersion + "), must greaterthan 10");
        }
        // server version
        rs.serverVersion = Mysqls.readASCIIString(buffer, array);
        if (Integer.parseInt(rs.serverVersion.substring(0, rs.serverVersion.indexOf('.'))) < 5) {
            throw new SQLException("Not supported serverVersion(" + rs.serverVersion + "), must greaterthan 5.0");
        }

        rs.threadId = Mysqls.readUB4(buffer);
        rs.seed = Mysqls.readBytes(buffer, array, AUTH_PLUGIN_DATA_PART1_LENGTH);
        buffer.get(); // 读掉\0
        int lowerServerCapabilitiesFlags = Mysqls.readUB2(buffer);
        rs.serverCharsetIndex = buffer.get() & 0xff;
        rs.serverStatus = Mysqls.readUB2(buffer);
        int capabilityFlagsUpper = Mysqls.readUB2(buffer);

        final int serverCapabilitiesFlags = (lowerServerCapabilitiesFlags | (capabilityFlagsUpper << 16));
        rs.serverCapabilities = serverCapabilitiesFlags;
        // length of the combined auth_plugin_data (scramble)
        int lenOfAuthPluginData;
        boolean isClientPluginAuthSupported = (serverCapabilitiesFlags & Mysqls.CLIENT_PLUGIN_AUTH) != 0;
        if (isClientPluginAuthSupported) {
            lenOfAuthPluginData = buffer.get() & 0xff;
        } else {
            buffer.get();
            lenOfAuthPluginData = 0;
        }
        buffer.get(FILLER_10); // 预留字节
        rs.seed2 = Mysqls.readBytes(
                buffer, array, Math.max(NONCE_LENGTH - AUTH_PLUGIN_DATA_PART1_LENGTH, lenOfAuthPluginData - 9));
        buffer.get(); // 读掉\0
        rs.authPluginName = new String(Mysqls.readBytes(buffer, array), StandardCharsets.UTF_8);
        if (!"caching_sha2_password".equals(rs.authPluginName) && !"mysql_native_password".equals(rs.authPluginName)) {
            throw new SQLException("Not supported auth-plugin(" + rs.authPluginName + ")");
        }
        return rs;
    }
}
