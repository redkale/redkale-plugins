/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.util.ByteBufferReader;
import static org.redkalex.source.mysql.MySQLOKPacket.TYPE_ID_EOF;

/**
 *
 * @author zhangjx
 */
public class MySQLEOFPacket extends MySQLPacket {

    public int typeid;

    public int warningCount;

    public int statusFlags;

    public MySQLEOFPacket(int len, ByteBufferReader buffer, byte[] array) {
        this.packetLength = len < 1 ? MySQLs.readUB3(buffer) : len;
        this.packetIndex = buffer.get();
        this.typeid = buffer.get() & 0xff;
        if (this.typeid == TYPE_ID_EOF) {
            this.warningCount = MySQLs.readUB2(buffer);
            this.statusFlags = MySQLs.readUB2(buffer);
        }
    }

    public boolean isEOF() {
        return this.typeid == TYPE_ID_EOF && this.packetLength <= 5;
    }
}
