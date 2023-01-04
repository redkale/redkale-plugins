/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.Objects;

/**
 *
 * @author zhangjx
 */
public class PgReqPing extends PgReqQuery {

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public PgReqPing() {
        prepare("SELECT 1");
    }
    
    @Override
    public String toString() {
        return "PgReqPing_" + Objects.hashCode(this) + "{sql=" + sql + "}";
    }
}
