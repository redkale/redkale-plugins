/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.util.ByteBufferReader;
import static org.redkalex.source.pgsql.PgsqlLDataSource.readUTF8String;
import static org.redkalex.source.pgsql.PgsqlLDataSource.readUTF8String;

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
    public PgRowDesc read(final ByteBufferReader buffer, final int length, final byte[] bytes) {
        PgColumnDesc[] columns = new PgColumnDesc[buffer.getShort()];
        for (int i = 0; i < columns.length; i++) {
            String name = readUTF8String(buffer, bytes);
            //buffer.position(buffer.position() + 6);
            buffer.skip(6);
            PgOid type = PgOid.valueOfId(buffer.getInt());
            //buffer.position(buffer.position() + 8);
            buffer.skip(8);
            columns[i] = new PgColumnDesc(name, type);
        }
        return new PgRowDesc(columns);
    }
}
