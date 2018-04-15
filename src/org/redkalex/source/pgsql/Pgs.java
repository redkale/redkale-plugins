/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *
 * @author zhangjx
 */
public abstract class Pgs {

    protected static String getCString(ByteBuffer buffer, byte[] store) {
        int i = 0;
        for (byte c = buffer.get(); c != 0; c = buffer.get()) {
            store[i++] = c;
        }
        return new String(store, 0, i, StandardCharsets.UTF_8);
    }

    protected static ByteBuffer putCString(ByteBuffer buffer, String string) {
        buffer.put(string.getBytes(StandardCharsets.UTF_8));
        buffer.put((byte) 0);
        return buffer;
    }
}
