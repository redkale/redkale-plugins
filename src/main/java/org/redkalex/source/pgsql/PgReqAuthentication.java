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

/** @author zhangjx */
public class PgReqAuthentication extends PgClientRequest {

    protected final SourceUrlInfo urlInfo;

    public PgReqAuthentication(SourceUrlInfo urlInfo) {
        this.urlInfo = urlInfo;
    }

    @Override
    public int getType() {
        return REQ_TYPE_AUTH;
    }

    @Override
    public String toString() {
        return "PgReqAuthentication_" + Objects.hashCode(this) + "{username=" + urlInfo.username + ", password="
                + urlInfo.password + ", database=" + urlInfo.database + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        int start = array.length();
        array.putInt(0);
        array.putInt(196608);
        writeUTF8String(writeUTF8String(array, "user"), urlInfo.username);
        writeUTF8String(writeUTF8String(array, "database"), urlInfo.database);
        writeUTF8String(writeUTF8String(array, "client_encoding"), "UTF8");
        writeUTF8String(writeUTF8String(array, "application_name"), "redkalex-pgsql-client");
        writeUTF8String(writeUTF8String(array, "DateStyle"), "ISO");
        writeUTF8String(writeUTF8String(array, "extra_float_digits"), "2");
        array.putByte(0);
        array.putInt(start, array.length() - start);
    }
}
