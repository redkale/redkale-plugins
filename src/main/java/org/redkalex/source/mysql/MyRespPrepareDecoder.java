/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import org.redkale.util.ByteArray;

/** @author zhangjx */
public class MyRespPrepareDecoder extends MyRespDecoder<MyRespPrepare> {

    public static final MyRespPrepareDecoder instance = new MyRespPrepareDecoder();

    @Override
    public MyRespPrepare read(
            MyClientConnection conn,
            ByteBuffer buffer,
            int length,
            byte index,
            ByteArray array,
            MyClientRequest request,
            MyResultSet dataset) {
        MyRespPrepare rs = new MyRespPrepare();
        rs.statementId = Mysqls.readUB4(buffer);
        rs.numberOfColumns = Mysqls.readUB2(buffer);
        rs.numberOfParameters = Mysqls.readUB2(buffer);
        buffer.get(); // [00] filler
        rs.numberOfWarnings = Mysqls.readUB2(buffer);
        if (rs.numberOfParameters > 0) {
            rs.paramDescs = new MyRowDesc(new MyRowColumn[rs.numberOfParameters]);
            rs.paramDecodeIndex = 0;
        }
        if (rs.numberOfColumns > 0) {
            rs.columnDescs = new MyRowDesc(new MyRowColumn[rs.numberOfColumns]);
            if (rs.paramDecodeIndex < 0) rs.columnDecodeIndex = 0;
        }
        return rs;
    }
}
