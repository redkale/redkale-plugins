/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import static org.redkalex.source.pgsql.PgClientCodec.*;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class PgRespErrorDecoder extends PgRespDecoder<SQLException> {

    public static final byte ERROR_OR_NOTICE_SEVERITY = 'S';

    public static final byte ERROR_OR_NOTICE_CODE = 'C';

    public static final byte ERROR_OR_NOTICE_MESSAGE = 'M';

    public static final byte ERROR_OR_NOTICE_DETAIL = 'D';

    public static final byte ERROR_OR_NOTICE_HINT = 'H';

    public static final byte ERROR_OR_NOTICE_POSITION = 'P';

    public static final byte ERROR_OR_NOTICE_INTERNAL_POSITION = 'p';

    public static final byte ERROR_OR_NOTICE_INTERNAL_QUERY = 'q';

    public static final byte ERROR_OR_NOTICE_WHERE = 'W';

    public static final byte ERROR_OR_NOTICE_FILE = 'F';

    public static final byte ERROR_OR_NOTICE_LINE = 'L';

    public static final byte ERROR_OR_NOTICE_ROUTINE = 'R';

    public static final byte ERROR_OR_NOTICE_SCHEMA = 's';

    public static final byte ERROR_OR_NOTICE_TABLE = 't';

    public static final byte ERROR_OR_NOTICE_COLUMN = 'c';

    public static final byte ERROR_OR_NOTICE_DATA_TYPE = 'd';

    public static final byte ERROR_OR_NOTICE_CONSTRAINT = 'n';

    public static final PgRespErrorDecoder instance = new PgRespErrorDecoder();

    private PgRespErrorDecoder() {}

    @Override
    public byte messageid() {
        return MESSAGE_TYPE_ERROR_RESPONSE; // 'E'
    }

    @Override
    public SQLException read(
            PgClientConnection conn,
            final ByteBuffer buffer,
            final int length,
            ByteArray array,
            PgClientRequest request,
            PgResultSet dataset) {
        String severity = null, code = null, message = null, detail = null, hint = null, line = null, table = null;
        for (byte type = buffer.get(); type != 0; type = buffer.get()) {
            String value = PgClientCodec.readUTF8String(buffer, array);
            if (type == ERROR_OR_NOTICE_SEVERITY) { // 'S'
                severity = value;
            } else if (type == ERROR_OR_NOTICE_CODE) { // 'C'
                code = value;
            } else if (type == ERROR_OR_NOTICE_MESSAGE) { // 'M'
                message = value;
            } else if (type == ERROR_OR_NOTICE_DETAIL) { // 'D'
                detail = value;
            } else if (type == ERROR_OR_NOTICE_HINT) { // 'H'
                hint = value;
            } else if (type == ERROR_OR_NOTICE_LINE) { // 'L'
                line = value;
            } else if (type == ERROR_OR_NOTICE_TABLE) { // 't'
                table = value;
            }
        }
        return new SQLException(
                detail == null ? message : (message + " (" + detail + ")"), code, "ERROR".equals(severity) ? 1 : 0);
    }
}
