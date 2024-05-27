/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.Objects;
import org.redkale.convert.json.JsonConvert;

/** @author zhangjx */
public class PgReqInsert extends PgReqUpdate {

    @Override
    public int getType() {
        return REQ_TYPE_INSERT;
    }

    @Override
    public String toString() {
        return "PgReqInsert" + Objects.hashCode(this) + "{sql=" + sql + ", paramValues="
                + JsonConvert.root().convertTo(paramValues) + "}";
    }
}
