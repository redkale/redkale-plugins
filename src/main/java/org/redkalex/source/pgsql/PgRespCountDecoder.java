/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.nio.ByteBuffer;
import org.redkale.util.ByteArray;

/**
 *
 * @author zhangjx
 */
public class PgRespCountDecoder extends PgRespDecoder<Integer> {

    private static final String END0 = " 0";

    private static final String END1 = " 1";

    public static final PgRespCountDecoder instance = new PgRespCountDecoder();

    private PgRespCountDecoder() {
    }

    @Override
    public byte messageid() {
        return 'C';
    }

    @Override
    public Integer read(PgClientConnection conn, ByteBuffer buffer, int length, ByteArray array, PgClientRequest request, PgResultSet dataset) {
        String val = PgClientCodec.readUTF8String(buffer, array);
        if (val.endsWith(END1)) return 1;
        if (val.endsWith(END0)) return 0;
        int pos = val.lastIndexOf(' ');
        if (pos > 0) {
            String numstr = val.substring(pos + 1);
            if (numstr.charAt(0) >= '0' && numstr.charAt(0) <= '9') {
                return Integer.parseInt(numstr);
            } else if (numstr.startsWith("CREATE TABLE")) {
                return 1;
            } else {
                return 0;
            }
        }
        return Integer.MIN_VALUE;
    }

}
