/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import org.redkale.util.*;

/**
 * @author zhangjx
 * @param <T> 泛型
 */
public abstract class PgRespDecoder<T> {

    public abstract byte messageid();

    public abstract T read(
            PgClientConnection conn,
            ByteBuffer buffer,
            int length,
            ByteArray array,
            PgClientRequest request,
            PgResultSet dataset);
}
