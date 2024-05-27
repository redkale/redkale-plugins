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

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.merge.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

public class CustomStatementDeParser extends StatementDeParser {

    protected final ExpressionDeParser expressionDeParser;

    protected final SelectDeParser selectDeParser;

    public CustomStatementDeParser(
            ExpressionDeParser expressionDeParser, SelectDeParser selectDeParser, StringBuilder buffer) {
        super(expressionDeParser, selectDeParser, buffer);
        this.expressionDeParser = expressionDeParser;
        this.selectDeParser = selectDeParser;
    }

    public void deParse(Statement statement) {
        statement.accept(this);
    }

    @Override
    public void visit(Delete delete) {
        CustomDeleteDeParser deleteDeParser = new CustomDeleteDeParser(expressionDeParser, buffer);
        deleteDeParser.deParse(delete);
    }

    @Override
    public void visit(Update update) {
        CustomUpdateDeParser updateDeParser = new CustomUpdateDeParser(expressionDeParser, buffer);
        updateDeParser.deParse(update);
    }

    @Override
    public void visit(Merge merge) {
        CustomMergeDeParser mergeDeParser = new CustomMergeDeParser(expressionDeParser, selectDeParser, buffer);
        mergeDeParser.deParse(merge);
    }
}
