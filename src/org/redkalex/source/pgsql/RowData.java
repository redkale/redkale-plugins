/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.util.Objects;

/**
 *
 * @author zhangjx
 */
public class RowData {

    final byte[][] values;

    public RowData(byte[][] values) {
        Objects.requireNonNull(values);
        this.values = values;
    }

    public byte[] getValue(int i) {
        return values[i];
    }

    public Serializable getObject(RowDesc rowDesc, int i) {
        byte[] bs = values[i];
        ColumnDesc colDesc = rowDesc.getColumn(i);
        if (bs == null) return null;
        return colDesc.getObject(bs);
    }

    public int length() {
        if (values == null) return -1;
        return values.length;
    }

    @Override
    public String toString() {
        return "{cols:" + (values == null ? -1 : values.length) + "}";
    }
}
