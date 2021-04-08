/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.util.ByteBufferReader;

/**
 *
 * @author zhangjx
 */
public class MyColumnCountPacket extends MyPacket {

    public int columnCount;

    public MyColumnCountPacket(int len, ByteBufferReader reader) {
        this.packetLength = len < 1 ? Mysqls.readUB3(reader) : len;
        this.packetIndex = reader.get();
        this.columnCount = (int) Mysqls.readLength(reader);
    }
}
