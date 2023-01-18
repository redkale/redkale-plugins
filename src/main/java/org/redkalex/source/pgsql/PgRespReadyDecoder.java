/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import org.redkale.util.ByteArray;
import static org.redkalex.source.pgsql.PgClientCodec.*;

/**
 *
 * @author zhangjx
 */
public class PgRespReadyDecoder extends PgRespDecoder<Boolean> {

    public static final PgRespReadyDecoder instance = new PgRespReadyDecoder();

    private PgRespReadyDecoder() {
    }

    @Override
    public byte messageid() {
        return MESSAGE_TYPE_READY_FOR_QUERY; // 'Z'
    }

    @Override
    public Boolean read(PgClientConnection conn, ByteBuffer buffer, int length, ByteArray array, PgClientRequest request, PgResultSet dataset) {
        if (length <= 4) {
            return true;
        }
        buffer.position(buffer.position() + length - 4);
        return true;
    }

}
