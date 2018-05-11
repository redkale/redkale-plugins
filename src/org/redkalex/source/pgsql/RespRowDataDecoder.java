/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.util.ByteBufferReader;

/**
 *
 * @author zhangjx
 */
public class RespRowDataDecoder implements RespDecoder<RowData> {

    @Override
    public byte messageid() {
        return 'D';
    }

    @Override
    public RowData read(final ByteBufferReader buffer, final int length, final byte[] bytes) {
        byte[][] values = new byte[buffer.getShort()][];
        for (int i = 0; i < values.length; i++) {
            int sublength = buffer.getInt();
            if (sublength != -1) {
                values[i] = new byte[sublength];
                buffer.get(values[i]);
            } else {
                values[i] = null;
            }
        }
        return new RowData(values);
    }
}
