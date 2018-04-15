/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import static org.redkalex.source.pgsql.Pgs.getCString;

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
    public RowDesc read(ByteBuffer buffer) {
        byte[] bytes = new byte[255];
        ColumnDesc[] columns = new ColumnDesc[buffer.getShort()];

        for (int i = 0; i < columns.length; i++) {
            String name = getCString(buffer, bytes);
            buffer.position(buffer.position() + 6);
            Oid type = Oid.valueOfId(buffer.getInt());
            buffer.position(buffer.position() + 8);
            columns[i] = new ColumnDesc(name, type);
        }

        return new RowDesc(columns);
    }

}
