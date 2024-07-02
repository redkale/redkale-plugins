/* -
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2004 - 2019 JSQLParser
 * %%
 * Dual licensed under GNU LGPL 2.1 or Apache License 2.0
 * #L%
 */
package org.redkalex.source.parser;

import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

public class CustomStatementDeParser extends StatementDeParser {

    public CustomStatementDeParser(
            ExpressionDeParser expressionDeParser, SelectDeParser selectDeParser, StringBuilder buffer) {
        super(expressionDeParser, selectDeParser, buffer);
    }

    @Override
    public <S> StringBuilder visit(Delete delete, S context) {
        new CustomDeleteDeParser(getExpressionDeParser(), buffer).deParse(delete);
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(Update update, S context) {
        new CustomUpdateDeParser(getExpressionDeParser(), buffer).deParse(update);
        return buffer;
    }
}
