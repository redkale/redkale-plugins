/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.util.ByteBufferReader;
import static org.redkalex.source.pgsql.PgSQLDataSource.readUTF8String;
import static org.redkalex.source.pgsql.PgSQLDataSource.readUTF8String;

/**
 *
 * @author zhangjx
 */
public class RespRowDescDecoder implements RespDecoder<RowDesc> {

    @Override
    public byte messageid() {
        return 'T';
    }

    @Override
    public RowDesc read(final ByteBufferReader buffer, final int length, final byte[] bytes) {
        ColumnDesc[] columns = new ColumnDesc[buffer.getShort()];
        for (int i = 0; i < columns.length; i++) {
            String name = readUTF8String(buffer, bytes);
            //buffer.position(buffer.position() + 6);
            buffer.skip(6);
            Oid type = Oid.valueOfId(buffer.getInt());
            //buffer.position(buffer.position() + 8);
            buffer.skip(8);
            columns[i] = new ColumnDesc(name, type);
        }
        return new RowDesc(columns);
    }
}
