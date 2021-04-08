/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

/**
 *
 * @author zhangjx
 */
public class PgReqPing extends PgReqQuery {

    public static final PgReqPing INSTANCE = new PgReqPing();

    public <T extends Object> PgReqPing() {
        super(null, "SELECT 1");
    }

}
