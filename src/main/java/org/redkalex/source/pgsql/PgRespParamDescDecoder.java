/*
 *
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import org.redkale.util.ByteArray;
import static org.redkalex.source.pgsql.PgClientCodec.*;

/**
 *
 * @author zhangjx
 */
public class PgRespParamDescDecoder extends PgRespDecoder<PgColumnFormat[]> {

    public static final PgRespParamDescDecoder instance = new PgRespParamDescDecoder();

    private PgRespParamDescDecoder() {
    }

    @Override
    public byte messageid() {
        return MESSAGE_TYPE_PARAMETER_DESCRIPTION; // 't'
    }

    @Override
    public PgColumnFormat[] read(PgClientConnection conn, ByteBuffer buffer, final int length, ByteArray array, PgClientRequest request, PgResultSet dataset) {
        if (length <= 4) {
            return null;
        }
        buffer.position(buffer.position() + length - 4);
        return null;
    }
}
