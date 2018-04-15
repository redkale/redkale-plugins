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
 * @param <T>
 */
public interface RespDecoder<T> {

    public byte messageid();

    public T read(ByteBuffer buffer);
}
