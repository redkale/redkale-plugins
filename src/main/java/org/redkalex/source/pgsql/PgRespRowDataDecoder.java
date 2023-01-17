/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.redkale.util.ByteArray;

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

    @Override  //RowData 一行数据
    public PgRowData read(PgClientConnection conn, ByteBuffer buffer, final int length, ByteArray array, PgClientRequest request, PgResultSet dataset) {
        PgPrepareDesc prepareDesc = request.getType() == PgClientRequest.REQ_TYPE_EXTEND_QUERY ? conn.getPgPrepareDesc(((PgReqExtended) request).sql) : null;
        if (prepareDesc == null) { //text
            byte[][] byteValues = new byte[buffer.getShort()][];
            for (int i = 0; i < byteValues.length; i++) {
                byteValues[i] = (byte[]) PgsqlFormatter.decodeRowColumnValue(buffer, array, null, buffer.getInt());
            }
            return new PgRowData(byteValues, null);
        }
        //binary
        PgColumnFormat[] formats = prepareDesc.resultFormats();
        Serializable[] realValues = new Serializable[buffer.getShort()];
        for (int i = 0; i < realValues.length; i++) {
            realValues[i] = formats[i].decoder().decode(buffer, array, buffer.getInt());
        }
        return new PgRowData(null, realValues);
    }

}
