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
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.util.deparser.DeleteDeParser;

public class CustomDeleteDeParser extends DeleteDeParser {

    public CustomDeleteDeParser(ExpressionVisitor expressionVisitor, StringBuilder buffer) {
        super(expressionVisitor, buffer);
    }

    @Override
    protected void deparseWhereClause(Delete delete) {
        if (delete.getWhere() != null) {
            buffer.append(" WHERE ");
            int len = buffer.length();
            delete.getWhere().accept(getExpressionVisitor());
            if (buffer.length() == len) {
                buffer.delete(len - " WHERE ".length(), len);
            }
        }
    }
}
