/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 *
 * @author zhangjx
 */
public class PgExtendedCommandNode implements Supplier<PgRowDesc> {

    public final AtomicBoolean currExtendedRowDescFlag = new AtomicBoolean();

    //预编译的sql的statmentid
    public long statementIndex;

    //DESCRIBE
    public PgRowDesc rowDesc;

    @Override
    public PgRowDesc get() {
        return rowDesc;
    }

}
