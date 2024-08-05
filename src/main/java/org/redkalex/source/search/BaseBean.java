/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.io.Serializable;
import org.redkale.convert.json.JsonConvert;
import org.redkale.annotation.Serial;

/** @author zhangjx */
@Serial
abstract class BaseBean implements Serializable {

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
