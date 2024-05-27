/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.convert.json.JsonConvert;

/** @author zhangjx */
public class MyRespOK {

    public static final byte[] OK = new byte[] {7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};

    public static final byte[] AC_OFF = new byte[] {7, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0};

    public long affectedRows = -1;

    public long lastInsertId = -1;

    public int serverStatusFlags;

    public int warningCount;

    public String info;

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
