/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** @author zhangjx */
public class PgRowData {

    static final PgRowData NIL = new PgRowData(null, null);

    final byte[][] byteBalues;

    final Serializable[] realValues;

    public PgRowData(byte[][] byteBalues, Serializable[] realValues) {
        this.byteBalues = byteBalues;
        this.realValues = realValues;
    }

    public Serializable getObject(PgRowDesc rowDesc, int i) {
        if (realValues != null) {
            return realValues[i];
        }
        byte[] bs = byteBalues[i];
        PgRowColumn colDesc = rowDesc.getColumn(i);
        if (bs == null) {
            return null;
        }
        return colDesc.getObject(bs);
    }

    @Override
    public String toString() {
        if (realValues != null) {
            return Arrays.toString(realValues).replace('[', '{').replace(']', '}');
        }
        int size = byteBalues == null ? -1 : byteBalues.length;
        StringBuilder sb = new StringBuilder();
        sb.append(PgRowData.class.getSimpleName()).append('{');
        if (size < 1) {
            sb.append("[size]:").append(size);
        } else {
            StringBuilder d = new StringBuilder();
            for (int i = 0; i < size; i++) {
                if (d.length() > 0) {
                    d.append(',');
                }
                d.append(new String(byteBalues[0], StandardCharsets.UTF_8));
            }
            sb.append(d);
        }
        return sb.append('}').toString();
    }
}
