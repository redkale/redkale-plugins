/* -
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2004 - 2019 JSQLParser
 * %%
 * Dual licensed under GNU LGPL 2.1 or Apache License 2.0
 * #L%
 *
 */
package org.redkalex.source.parser;

import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.UpdateDeParser;

public class CustomUpdateDeParser extends UpdateDeParser {

    public CustomUpdateDeParser(ExpressionVisitor expressionVisitor, StringBuilder builder) {
        super(expressionVisitor, builder);
    }

    @Override
    protected void deparseWhereClause(Update update) {
        if (update.getWhere() != null) {
            builder.append(" WHERE ");
            int len = builder.length();
            update.getWhere().accept(getExpressionVisitor());
            if (builder.length() == len) {
                builder.delete(len - " WHERE ".length(), len);
            }
        }
    }
}
