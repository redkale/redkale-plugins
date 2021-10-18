/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;
import static org.redkalex.source.pgsql.PgClientRequest.*;

/**
 *
 * @author zhangjx
 */
public class PgReqClose extends PgClientRequest {

    public static final PgReqClose INSTANCE = new PgReqClose();

    public PgReqClose() {
    }

    @Override
    public int getType() {
        return REQ_TYPE_UPDATE;
    }

    @Override
    public String toString() {
        return "PgReqClose{}";
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        array.putByte('X');
        array.putInt(4);
    }

}
