/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.redkale.convert.json.JsonConvert;

/**
 *
 * @author zhangjx
 */
public class MySQLOKorErrorPacket extends MySQLPacket {

    private static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};

    private static final byte[] AC_OFF = new byte[]{7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

    private static final byte HEADER = 0x00;

    public byte header = HEADER;

    public long affectedRows;

    public long insertId;

    public String sqlState;

    public int serverStatus;

    public int warningCount;

    public byte[] message;

    public MySQLOKorErrorPacket(ByteBuffer buffer, byte[] array) {
        packetLength = MySQLs.readUB3(buffer);
        packetId = buffer.get();
        header = buffer.get();
        if (header != 0x00) {
            while (buffer.hasRemaining() && buffer.get() != '#');
            if (buffer.hasRemaining()) {
                this.sqlState = new String(MySQLs.readBytes(buffer, array, 5));
                this.message = MySQLs.readBytes(buffer, array);
            }
        } else {
            affectedRows = MySQLs.readLength(buffer);
            insertId = MySQLs.readLength(buffer);
            serverStatus = MySQLs.readUB2(buffer);
            warningCount = MySQLs.readUB2(buffer);
            if (buffer.hasRemaining()) {
                this.message = MySQLs.readBytesWithLength(buffer);
            }
        }
    }

    public String toMessageString(String defval) {
        if (message == null) return defval;
        return new String(message, StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        String json = JsonConvert.root().convertTo(this);
        return json.substring(0, json.length() - 1) + ", \"message\":" + (message == null ? "null" : ("\"" + new String(message) + "\"")) + "}";
    }
}
