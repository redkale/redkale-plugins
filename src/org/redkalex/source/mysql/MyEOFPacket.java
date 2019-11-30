/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.util.ByteBufferReader;
import static org.redkalex.source.mysql.MyOKPacket.TYPE_ID_EOF;

/**
 *
 * @author zhangjx
 */
public class MyEOFPacket extends MyPacket {

    public int typeid;

    public int warningCount;

    public int statusFlags;

    public MyEOFPacket(int len, int index, ByteBufferReader reader, byte[] array) {
        this.packetLength = len < 1 ? Mysqls.readUB3(reader) : len;
        this.packetIndex = index < -999 ? reader.get() : (byte)index;
        this.typeid = reader.get() & 0xff;
        if (this.typeid == TYPE_ID_EOF) {
            this.warningCount = Mysqls.readUB2(reader);
            this.statusFlags = Mysqls.readUB2(reader);
        }
    }

    public boolean isEOF() {
        return this.typeid == TYPE_ID_EOF && this.packetLength <= 5;
    }
}
