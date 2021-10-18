/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import org.redkale.util.ByteArray;
import static org.redkalex.source.mysql.MysqlType.ColumnFlags.UNSIGNED_FLAG;
import static org.redkalex.source.mysql.MysqlType.FIELD_FLAG_BINARY;

/**
 *
 * @author zhangjx
 */
public class MyRespRowColumnDecoder extends MyRespDecoder<MyRowColumn> {

    public static final MyRespRowColumnDecoder instance = new MyRespRowColumnDecoder();

    @Override
    public MyRowColumn read(MyClientConnection conn, ByteBuffer buffer, int length, byte index, ByteArray array, MyClientRequest request, MyResultSet dataset) {
        MyRowColumn column = new MyRowColumn();
        column.catalog = Mysqls.readBytesWithLength(buffer);
        column.schema = Mysqls.readBytesWithLength(buffer);
        column.tableLabel = Mysqls.readBytesWithLength(buffer);
        column.tableName = Mysqls.readBytesWithLength(buffer);
        column.columnLabel = new String(Mysqls.readBytesWithLength(buffer));
        column.columnName = new String(Mysqls.readBytesWithLength(buffer));
        buffer.get();  //nextLength : always 0x0c 12
        column.charsetSet = Mysqls.readUB2(buffer);
        column.length = Mysqls.readUB4(buffer);
        column.type = buffer.get() & 0xff;
        column.flags = Mysqls.readUB2(buffer);
        column.decimals = buffer.get();
        column.binary = (column.flags & FIELD_FLAG_BINARY) > 0;
        column.unsign = (column.flags & UNSIGNED_FLAG) != 0;
        return column;
    }

}
