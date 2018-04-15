/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.convert.json.JsonConvert;

/**
 *
 * @author zhangjx
 */
public class ColumnDesc {

    final String name;

    final Oid type;

    public ColumnDesc(String name, Oid type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Oid getType() {
        return type;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
