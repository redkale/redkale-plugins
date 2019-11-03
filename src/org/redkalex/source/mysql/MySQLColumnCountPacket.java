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
public class MySQLColumnCountPacket extends MySQLPacket {

    public int columnCount;

    public MySQLColumnCountPacket(int len, ByteBufferReader buffer, byte[] array) {
        this.packetLength = len < 1 ? MySQLs.readUB3(buffer) : len;
        this.packetIndex = buffer.get();
        this.columnCount = (int) MySQLs.readLength(buffer);
    }
}
