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
public class PgRespRowDescDecoder implements PgRespDecoder<PgRowDesc> {

    @Override
    public byte messageid() {
        return 'T';
    }

    @Override
    public PgRowDesc read(final ByteBuffer buffer, final int length, final ByteArray bytes) {
        PgColumnDesc[] columns = new PgColumnDesc[buffer.getShort()];
        for (int i = 0; i < columns.length; i++) {
            String name = PgClientCodec.readUTF8String(buffer, bytes);
            buffer.position(buffer.position() + 6);
            PgOid type = PgOid.valueOfId(buffer.getInt());
            buffer.position(buffer.position() + 8);
            columns[i] = new PgColumnDesc(name, type);
        }
        return new PgRowDesc(columns);
    }
}
