/*
 *
 */
package org.redkalex.source.mysql;

import java.util.Objects;
import org.redkale.net.client.ClientConnection;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class MyReqVirtual extends MyClientRequest {

    @Override
    public void writeTo(ClientConnection conn, ByteArray array) {}

    @Override
    public int getType() {
        return 0;
    }

    @Override
    public boolean isVirtualType() {
        return true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "_" + Objects.hash(this) + "{virtual}";
    }
}
