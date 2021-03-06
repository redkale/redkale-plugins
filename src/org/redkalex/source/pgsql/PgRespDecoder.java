/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 * @param <T> 泛型
 */
public interface PgRespDecoder<T> {

    public byte messageid();

    public T read(final ByteBuffer buffer, final int length, final ByteArray bytes);
}
