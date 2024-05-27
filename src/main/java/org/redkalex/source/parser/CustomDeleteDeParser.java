/* -
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2004 - 2019 JSQLParser
 * %%
 * Dual licensed under GNU LGPL 2.1 or Apache License 2.0
 * #L%
 *
 * 复制过来增加deparseWhereClause方法
 */
package org.redkalex.source.parser;

import static java.util.stream.Collectors.joining;

import java.util.Iterator;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.DeleteDeParser;
import net.sf.jsqlparser.util.deparser.LimitDeparser;
import net.sf.jsqlparser.util.deparser.OrderByDeParser;

public class CustomDeleteDeParser extends DeleteDeParser {

	public CustomDeleteDeParser(ExpressionVisitor expressionVisitor, StringBuilder buffer) {
		super(expressionVisitor, buffer);
	}

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

	@Override
	public void deParse(Delete delete) {
		ExpressionVisitor expressionVisitor = getExpressionVisitor();
		if (delete.getWithItemsList() != null && !delete.getWithItemsList().isEmpty()) {
			buffer.append("WITH ");
			for (Iterator<WithItem> iter = delete.getWithItemsList().iterator(); iter.hasNext(); ) {
				WithItem withItem = iter.next();
				buffer.append(withItem);
				if (iter.hasNext()) {
					buffer.append(",");
				}
				buffer.append(" ");
			}
		}
		buffer.append("DELETE");
		if (delete.getOracleHint() != null) {
			buffer.append(delete.getOracleHint()).append(" ");
		}
		if (delete.getModifierPriority() != null) {
			buffer.append(" ").append(delete.getModifierPriority());
		}
		if (delete.isModifierQuick()) {
			buffer.append(" QUICK");
		}
		if (delete.isModifierIgnore()) {
			buffer.append(" IGNORE");
		}
		if (delete.getTables() != null && !delete.getTables().isEmpty()) {
			buffer.append(delete.getTables().stream()
					.map(Table::getFullyQualifiedName)
					.collect(joining(", ", " ", "")));
		}

		if (delete.getOutputClause() != null) {
			delete.getOutputClause().appendTo(buffer);
		}

		if (delete.isHasFrom()) {
			buffer.append(" FROM");
		}
		buffer.append(" ").append(delete.getTable().toString());

		if (delete.getUsingList() != null && !delete.getUsingList().isEmpty()) {
			buffer.append(" USING")
					.append(delete.getUsingList().stream().map(Table::toString).collect(joining(", ", " ", "")));
		}
		if (delete.getJoins() != null) {
			for (Join join : delete.getJoins()) {
				if (join.isSimple()) {
					buffer.append(", ").append(join);
				} else {
					buffer.append(" ").append(join);
				}
			}
		}

		deparseWhereClause(delete);

		if (delete.getOrderByElements() != null) {
			new OrderByDeParser(expressionVisitor, buffer).deParse(delete.getOrderByElements());
		}
		if (delete.getLimit() != null) {
			new LimitDeparser(expressionVisitor, buffer).deParse(delete.getLimit());
		}

		if (delete.getReturningClause() != null) {
			delete.getReturningClause().appendTo(buffer);
		}
	}
}
