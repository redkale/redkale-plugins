/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class PgRespErrorDecoder extends PgRespDecoder<SQLException> {

    public static final PgRespErrorDecoder instance = new PgRespErrorDecoder();

    private PgRespErrorDecoder() {
    }

    @Override
    public byte messageid() {
        return 'E';
    }

    @Override
    public SQLException read(PgClientConnection conn, final ByteBuffer buffer, final int length, ByteArray array, PgClientRequest request, PgResultSet dataset) {
        String level = null, code = null, message = null, detail = null;
        for (byte type = buffer.get(); type != 0; type = buffer.get()) {
            String value = PgClientCodec.readUTF8String(buffer, array);
            if (type == (byte) 'S') {
                level = value;
            } else if (type == 'C') {
                code = value;
            } else if (type == 'M') {
                message = value;
            } else if (type == 'D') {
                detail = value;
            }
        }
        return new SQLException(detail == null ? message : (message + " (" + detail + ")"), code, "ERROR".equals(level) ? 1 : 0);
    }

}
