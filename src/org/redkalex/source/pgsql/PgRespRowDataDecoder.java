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
public class PgRespRowDataDecoder implements PgRespDecoder<PgRowData> {

    @Override
    public byte messageid() {
        return 'D';
    }

    @Override
    public PgRowData read(final ByteBuffer buffer, final int length, final ByteArray bytes) {
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
        return new PgRowData(values);
    }

}
