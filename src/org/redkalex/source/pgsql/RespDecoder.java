/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.util.ByteBufferReader;

/**
 *
 * @author zhangjx
 * @param <T> 泛型
 */
public interface RespDecoder<T> {

    public byte messageid();

    public T read(final ByteBufferReader buffer, final int length, final byte[] bytes);
}
