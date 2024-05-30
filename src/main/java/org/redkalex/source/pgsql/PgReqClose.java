/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import static org.redkalex.source.pgsql.PgClientRequest.REQ_TYPE_UPDATE;

import java.util.Objects;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class PgReqClose extends PgClientRequest {

    public PgReqClose() {}

    @Override
    public int getType() {
        return REQ_TYPE_UPDATE;
    }

    @Override
    public final boolean isCloseType() {
        return true;
    }

    @Override
    public String toString() {
        return "PgReqClose_" + Objects.hashCode(this) + "{type=" + getType() + "}";
    }

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {
        array.putByte('X');
        array.putInt(4);
    }
}
