/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import org.redkale.convert.json.JsonConvert;

/** @author zhangjx */
public class MyRespPrepare {

    public Long statementId;

    public int numberOfParameters; // 先param后column

    public int numberOfColumns;

    public int numberOfWarnings;

    MyRowDesc paramDescs; // STATUS_PREPARE_PARAM时赋值

    int paramDecodeIndex = -1;

    MyRowDesc columnDescs; // STATUS_PREPARE_PARAM时赋值

    int columnDecodeIndex = -1;

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
