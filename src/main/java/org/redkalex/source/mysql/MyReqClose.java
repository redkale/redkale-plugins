/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.util.Objects;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;
import static org.redkalex.source.mysql.MyClientRequest.REQ_TYPE_UPDATE;

/**
 *
 * @author zhangjx
 */
public class MyReqClose extends MyClientRequest {

    public MyReqClose() {
    }

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
        return "MyReqClose_" + Objects.hashCode(this) + "{type=" + getType() + "}";
    }

    @Override
    public void accept(ClientConnection conn, ByteArray array) {
        Mysqls.writeUB3(array, 1);
        array.put(packetIndex);
        array.put(Mysqls.COM_QUIT);
    }

}
