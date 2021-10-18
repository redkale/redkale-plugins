/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

/**
 *
 * @author zhangjx
 */
public class MyPrepareDesc {

    public final int numberOfParameters;  //先param后column

    public final int numberOfColumns;

    public final MyRowDesc paramDescs;

    public final MyRowDesc columnDescs;

    public MyPrepareDesc(MyRespPrepare prepare) {
        this.numberOfParameters = prepare.numberOfParameters;
        this.numberOfColumns = prepare.numberOfColumns;
        this.paramDescs = prepare.paramDescs;
        this.columnDescs = prepare.columnDescs;
    }
}
