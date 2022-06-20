/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class PgRespRowDataDecoder extends PgRespDecoder<PgRowData> {

    public static final PgRespRowDataDecoder instance = new PgRespRowDataDecoder();

    private PgRespRowDataDecoder() {
    }

    @Override
    public byte messageid() {
        return 'D';
    }

    @Override
    public PgRowData read(PgClientConnection conn, ByteBuffer buffer, final int length, ByteArray array, PgClientRequest request, PgResultSet dataset) {
        Attribute[] resultAttrs = request.getType() == PgClientRequest.REQ_TYPE_EXTEND_QUERY ? ((PgReqExtended) request).resultAttrs : null;
        if (resultAttrs == null) { //text
            byte[][] byteValues = new byte[buffer.getShort()][];
            for (int i = 0; i < byteValues.length; i++) {
                byteValues[i] = (byte[]) PgsqlFormatter.decodeRowColumnValue(buffer, array, null, buffer.getInt());
            }
            return new PgRowData(byteValues, null);
        }
        //binary
        Serializable[] realValues = new Serializable[buffer.getShort()];
        for (int i = 0; i < realValues.length; i++) {
            realValues[i] = PgsqlFormatter.decodeRowColumnValue(buffer, array, resultAttrs[i], buffer.getInt());
        }
        return new PgRowData(null, realValues);
    }

}
