/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.convert.json.JsonConvert;

/**
 *
 * @author zhangjx
 */
public abstract class MySQLPacket {

    public int packetLength;

    public byte packetId;

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
