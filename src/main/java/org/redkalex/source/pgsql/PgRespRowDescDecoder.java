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
 */
public class PgRespRowDescDecoder extends PgRespDecoder<PgRowDesc> {

    public static final PgRespRowDescDecoder instance = new PgRespRowDescDecoder();

    private PgRespRowDescDecoder() {
    }

    @Override
    public byte messageid() {
        return 'T';
    }

    @Override
    public PgRowDesc read(PgClientConnection conn, ByteBuffer buffer, final int length, ByteArray array, PgClientRequest request, PgResultSet dataset) {
        PgRowColumn[] columns = new PgRowColumn[buffer.getShort()];
        for (int i = 0; i < columns.length; i++) {
            String name = PgClientCodec.readUTF8String(buffer, array);
            buffer.position(buffer.position() + 6);
            int oid = buffer.getInt();
            buffer.position(buffer.position() + 8);
            columns[i] = new PgRowColumn(name, oid);
        }
        return new PgRowDesc(columns);
    }
}
