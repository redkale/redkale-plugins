/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.util.Arrays;

/** @author zhangjx */
public class MyRowDesc {

    final MyRowColumn[] columns;

    public MyRowDesc(MyRowColumn[] columns) {
        this.columns = columns;
    }

    public MyRowColumn[] getColumns() {
        return columns;
    }

    public MyRowColumn getColumn(int i) {
        return columns[i];
    }

    public int length() {
        if (columns == null) {
            return -1;
        }
        return columns.length;
    }

    @Override
    public String toString() {
        return Arrays.toString(columns);
    }
}
