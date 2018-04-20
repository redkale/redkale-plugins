/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;

/**
 *
 * @author zhangjx
 */
public class RespReadyDecoder implements RespDecoder<Boolean> {

    @Override
    public byte messageid() {
        return 'Z';
    }

    @Override
    public Boolean read(ByteBuffer buffer, int length, byte[] bytes) {
        if (length <= 4) return true;
        buffer.position(buffer.position() + length - 4);
        return true;
    }

}
