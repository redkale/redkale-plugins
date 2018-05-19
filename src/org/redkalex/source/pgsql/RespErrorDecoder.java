/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.sql.SQLException;
import org.redkale.util.ByteBufferReader;
import static org.redkalex.source.pgsql.PgSQLDataSource.readUTF8String;
import static org.redkalex.source.pgsql.PgSQLDataSource.readUTF8String;

/**
 *
 * @author zhangjx
 */
public class RespErrorDecoder implements RespDecoder<SQLException> {

    @Override
    public byte messageid() {
        return 'E';
    }

    @Override
    public SQLException read(final ByteBufferReader buffer, final int length, final byte[] bytes) {
        String level = null, code = null, message = null;
        for (byte type = buffer.get(); type != 0; type = buffer.get()) {
            String value = readUTF8String(buffer, bytes);
            if (type == (byte) 'S') {
                level = value;
            } else if (type == 'C') {
                code = value;
            } else if (type == 'M') {
                message = value;
            }
        }
        return new SQLException(message, code, "ERROR".equals(level) ? 1 : 0);
    }

}
