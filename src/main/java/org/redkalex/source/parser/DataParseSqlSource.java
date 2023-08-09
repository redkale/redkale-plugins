/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.statement.Statement;
import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
public abstract class DataParseSqlSource extends AbstractDataSqlSource {

    private static class NativeSqlInfo {

        //原始的sql
        protected String rowSql;

        //In或者NOT IN条件的参数名
        protected Set<String> inExprNamedSet;

        //所有参数名
        protected Set<String> fullNamedSet;

        //原始的Statement
        protected Statement fullStatement;

        //原始的where条件
        protected Expression fullWhere;
    }

}
