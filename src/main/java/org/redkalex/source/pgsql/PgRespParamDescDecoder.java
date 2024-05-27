/*
 *
 */
package org.redkalex.source.pgsql;

import static org.redkalex.source.pgsql.PgClientCodec.*;

import java.nio.ByteBuffer;
import org.redkale.source.SourceException;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class PgRespParamDescDecoder extends PgRespDecoder<PgColumnFormat[]> {

    public static final PgRespParamDescDecoder instance = new PgRespParamDescDecoder();

    private PgRespParamDescDecoder() {}

    @Override
    public byte messageid() {
        return MESSAGE_TYPE_PARAMETER_DESCRIPTION; // 't'
    }

    @Override
    public PgColumnFormat[] read(
            PgClientConnection conn,
            ByteBuffer buffer,
            final int length,
            ByteArray array,
            PgClientRequest request,
            PgResultSet dataset) {
        PgColumnFormat[] formats = new PgColumnFormat[buffer.getShort() & 0xFFFF];
        for (int i = 0; i < formats.length; i++) {
            int oid = buffer.getInt();
            formats[i] = PgColumnFormat.valueOf(oid);
            if (formats[i].encoder() == null) {
                throw new SourceException("Unsupported data encode ColumnFormat: " + formats[i]);
            }
        }
        return formats;
    }
}
