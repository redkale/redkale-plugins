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
public class PgRowDesc {

    final PgColumnDesc[] columns;

    public PgRowDesc(PgColumnDesc[] columns) {
        this.columns = columns;
    }

    public PgColumnDesc[] getColumns() {
        return columns;
    }

    public PgColumnDesc getColumn(int i) {
        return columns[i];
    }

    public int length() {
        if (columns == null) return -1;
        return columns.length;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
