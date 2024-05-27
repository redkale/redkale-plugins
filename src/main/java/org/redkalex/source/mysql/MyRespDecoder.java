/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import org.redkale.util.*;

/**
 * @author zhangjx
 * @param <T> 泛型
 */
public abstract class MyRespDecoder<T> {

    public abstract T read(
            MyClientConnection conn,
            ByteBuffer buffer,
            int length,
            byte index,
            ByteArray array,
            MyClientRequest request,
            MyResultSet dataset)
            throws SQLException;

    protected static SQLException readErrorPacket(
            MyClientConnection conn, ByteBuffer buffer, int length, byte index, ByteArray array) {
        int vendorCode = Mysqls.readUB2(buffer); // errorCode
        buffer.get(); // 固定为 # SQL state marker will always be #
        String sqlState = new String(Mysqls.readBytes(buffer, array, 5), StandardCharsets.UTF_8);
        // typeid=1, errorCode=2, #=1, sqlState=5
        String errorMessage =
                new String(Mysqls.readBytes(buffer, array, length - 1 - 2 - 1 - 5), StandardCharsets.UTF_8);
        return new SQLException(errorMessage, sqlState, vendorCode);
    }

    protected static MyRespOK readOKPacket(
            MyClientConnection conn, ByteBuffer buffer, int length, byte index, ByteArray array) {
        final MyRespOK rs = new MyRespOK();
        // com.mysql.cj.protocol.a.NativeProtocol
        // com.mysql.cj.protocol.a.result.OkPacket
        int pos = buffer.position() - 1; // typeid已经被读了
        rs.affectedRows = Mysqls.readLength(buffer);
        rs.lastInsertId = Mysqls.readLength(buffer);
        rs.serverStatusFlags = Mysqls.readUB2(buffer);
        rs.warningCount = Mysqls.readUB2(buffer);
        int limit = length + pos - buffer.position();
        rs.info = Mysqls.readUTF8StringWithTerm(buffer, array, limit);
        // read session state changes info
        if ((rs.serverStatusFlags & Mysqls.SERVER_SESSION_STATE_CHANGED) > 0) {
            Mysqls.readUB2(buffer); // totalLen
        }
        return rs;
    }
}
