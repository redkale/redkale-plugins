/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import org.redkale.source.EntityInfo;
import org.redkale.util.Attribute;

/**
 *
 * @author zhangjx
 */
public class PgReqInsert extends PgReqUpdate {

    public <T extends Object> PgReqInsert(EntityInfo<T> info, String sql, int fetchSize, Attribute<T, Serializable>[] attrs, Object[]... parameters) {
        super(info, sql, fetchSize, attrs, parameters);
    }

    @Override
    public int getType() {
        return REQ_TYPE_INSERT;
    }
}
