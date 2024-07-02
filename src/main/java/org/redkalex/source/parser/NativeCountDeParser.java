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

import java.util.List;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class NativeCountDeParser extends CustomSelectDeParser {

    private PlainSelect countSelect;

    private List<SelectItem<?>> countRootSelectItems;

    public NativeCountDeParser(ExpressionVisitor expressionVisitor, StringBuilder buffer) {
        super(expressionVisitor, buffer);
    }

    public void initCountSelect(PlainSelect countSelect, List<SelectItem<?>> countSelectItems) {
        this.countSelect = countSelect;
        this.countRootSelectItems = countSelectItems;
    }

    @Override
    protected void deparseDistinctClause(PlainSelect plainSelect, Distinct distinct) {
        if (this.countSelect != plainSelect) {
            super.deparseDistinctClause(plainSelect, distinct);
        }
    }

    @Override
    protected void deparseSelectItemsClause(PlainSelect plainSelect, List<SelectItem<?>> selectItems) {
        if (this.countSelect != plainSelect) {
            super.deparseSelectItemsClause(plainSelect, selectItems);
        } else {
            super.deparseSelectItemsClause(plainSelect, this.countRootSelectItems);
        }
    }

    @Override
    protected void deparseOrderByElementsClause(PlainSelect plainSelect, List<OrderByElement> orderByElements) {
        if (this.countSelect != plainSelect) {
            super.deparseOrderByElementsClause(plainSelect, orderByElements);
        }
    }
}
