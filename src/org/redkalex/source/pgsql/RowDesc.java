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
public class RowDesc {

    final ColumnDesc[] columns;

    public RowDesc(ColumnDesc[] columns) {
        this.columns = columns;
    }

    public ColumnDesc[] getColumns() {
        return columns;
    }

    public ColumnDesc getColumn(int i) {
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
