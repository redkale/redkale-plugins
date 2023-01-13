/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.Objects;
import org.redkale.net.client.ClientConnection;
import org.redkale.source.AbstractDataSource.SourceUrlInfo;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class PgReqAuthentication extends PgClientRequest {

    protected final SourceUrlInfo info;

    public PgReqAuthentication(SourceUrlInfo info) {
        this.info = info;
    }

    @Override
    public int getType() {
        return REQ_TYPE_AUTH;
    }

    @Override
    public String toString() {
        return "PgReqAuthentication_" + Objects.hashCode(this) + "{username=" + info.username + ", password=" + info.password + ", database=" + info.database + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        int start = array.length();
        array.putInt(0);
        array.putInt(196608);
        writeUTF8String(writeUTF8String(array, "user"), info.username);
        writeUTF8String(writeUTF8String(array, "database"), info.database);
        writeUTF8String(writeUTF8String(array, "client_encoding"), "UTF8");
        array.putByte(0);
        array.putInt(start, array.length() - start);
    }

}
