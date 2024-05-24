/* -
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2004 - 2019 JSQLParser
 * %%
 * Dual licensed under GNU LGPL 2.1 or Apache License 2.0
 * #L%
 *
 * 复制过来增加acceptWhere、acceptUpdateSets方法
 */
package org.redkalex.source.parser;

import java.util.Iterator;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.LimitDeparser;
import net.sf.jsqlparser.util.deparser.OrderByDeParser;
import net.sf.jsqlparser.util.deparser.UpdateDeParser;

public class CustomUpdateDeParser extends UpdateDeParser {

    protected ExpressionVisitor expressionVisitor = new ExpressionVisitorAdapter();

    public CustomUpdateDeParser(ExpressionVisitor expressionVisitor, StringBuilder buffer) {
        super(expressionVisitor, buffer);
        this.expressionVisitor = expressionVisitor;
    }

    protected void acceptWhere(Update update) {
        if (update.getWhere() != null) {
            buffer.append(" WHERE ");
            int len = buffer.length();
            update.getWhere().accept(expressionVisitor);
            if (buffer.length() == len) {
                buffer.delete(len - " WHERE ".length(), len);
            }
        }
    }

    protected void acceptUpdateSets(Update update) {
        deparseUpdateSets(update.getUpdateSets(), buffer, expressionVisitor);
    }

    @Override
    public void deParse(Update update) {
        if (update.getWithItemsList() != null && !update.getWithItemsList().isEmpty()) {
            buffer.append("WITH ");
            for (Iterator<WithItem> iter = update.getWithItemsList().iterator(); iter.hasNext();) {
                WithItem withItem = iter.next();
                buffer.append(withItem);
                if (iter.hasNext()) {
                    buffer.append(",");
                }
                buffer.append(" ");
            }
        }
        buffer.append("UPDATE ");
        if (update.getOracleHint() != null) {
            buffer.append(update.getOracleHint()).append(" ");
        }
        if (update.getModifierPriority() != null) {
            buffer.append(update.getModifierPriority()).append(" ");
        }
        if (update.isModifierIgnore()) {
            buffer.append("IGNORE ");
        }
        buffer.append(update.getTable());
        if (update.getStartJoins() != null) {
            for (Join join : update.getStartJoins()) {
                if (join.isSimple()) {
                    buffer.append(", ").append(join);
                } else {
                    buffer.append(" ").append(join);
                }
            }
        }
        buffer.append(" SET ");

        acceptUpdateSets(update);

        if (update.getOutputClause() != null) {
            update.getOutputClause().appendTo(buffer);
        }

        if (update.getFromItem() != null) {
            buffer.append(" FROM ").append(update.getFromItem());
            if (update.getJoins() != null) {
                for (Join join : update.getJoins()) {
                    if (join.isSimple()) {
                        buffer.append(", ").append(join);
                    } else {
                        buffer.append(" ").append(join);
                    }
                }
            }
        }

        acceptWhere(update);

        if (update.getOrderByElements() != null) {
            new OrderByDeParser(expressionVisitor, buffer).deParse(update.getOrderByElements());
        }
        if (update.getLimit() != null) {
            new LimitDeparser(expressionVisitor, buffer).deParse(update.getLimit());
        }

        if (update.getReturningClause() != null) {
            update.getReturningClause().appendTo(buffer);
        }
    }

    @Override
    public void setExpressionVisitor(ExpressionVisitor visitor) {
        super.setExpressionVisitor(visitor);
        expressionVisitor = visitor;
    }

}
