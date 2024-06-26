/*
 *
 */
package org.redkalex.source.parser;

import java.util.Iterator;
import java.util.List;
import static java.util.stream.Collectors.joining;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.WindowDefinition;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.First;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.LateralView;
import net.sf.jsqlparser.statement.select.OptimizeFor;
import net.sf.jsqlparser.statement.select.PlainSelect;
import static net.sf.jsqlparser.statement.select.PlainSelect.BigQuerySelectQualifier.AS_STRUCT;
import static net.sf.jsqlparser.statement.select.PlainSelect.BigQuerySelectQualifier.AS_VALUE;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Skip;
import net.sf.jsqlparser.statement.select.Top;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.GroupByDeParser;
import net.sf.jsqlparser.util.deparser.LimitDeparser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;

/* -
 * #%L
 * JSQLParser library
 * %%
 * Copyright (C) 2004 - 2019 JSQLParser
 * %%
 * Dual licensed under GNU LGPL 2.1 or Apache License 2.0
 * #L%
 *
 * 复制过来重载visit方法
 */
public class CustomSelectDeParser extends SelectDeParser {

    public CustomSelectDeParser(ExpressionVisitor expressionVisitor, StringBuilder buffer) {
        super(expressionVisitor, buffer);
    }

    @Override
    public <S> StringBuilder visit(PlainSelect plainSelect, S context) {
        List<WithItem> withItemsList = plainSelect.getWithItemsList();
        if (withItemsList != null && !withItemsList.isEmpty()) {
            buffer.append("WITH ");
            for (Iterator<WithItem> iter = withItemsList.iterator(); iter.hasNext(); ) {
                iter.next().accept((SelectVisitor<?>) this, context);
                if (iter.hasNext()) {
                    buffer.append(",");
                }
                buffer.append(" ");
            }
        }

        buffer.append("SELECT ");

        if (plainSelect.getMySqlHintStraightJoin()) {
            buffer.append("STRAIGHT_JOIN ");
        }

        OracleHint hint = plainSelect.getOracleHint();
        if (hint != null) {
            buffer.append(hint).append(" ");
        }

        Skip skip = plainSelect.getSkip();
        if (skip != null) {
            buffer.append(skip).append(" ");
        }

        First first = plainSelect.getFirst();
        if (first != null) {
            buffer.append(first).append(" ");
        }

        deparseDistinctClause(plainSelect, plainSelect.getDistinct());

        if (plainSelect.getBigQuerySelectQualifier() != null) {
            switch (plainSelect.getBigQuerySelectQualifier()) {
                case AS_STRUCT:
                    buffer.append("AS STRUCT ");
                    break;
                case AS_VALUE:
                    buffer.append("AS VALUE ");
                    break;
            }
        }

        Top top = plainSelect.getTop();
        if (top != null) {
            visit(top);
        }

        if (plainSelect.getMySqlSqlCacheFlag() != null) {
            buffer.append(plainSelect.getMySqlSqlCacheFlag().name()).append(" ");
        }

        if (plainSelect.getMySqlSqlCalcFoundRows()) {
            buffer.append("SQL_CALC_FOUND_ROWS").append(" ");
        }

        deparseSelectItemsClause(plainSelect, plainSelect.getSelectItems());

        if (plainSelect.getIntoTables() != null) {
            buffer.append(" INTO ");
            for (Iterator<Table> iter = plainSelect.getIntoTables().iterator(); iter.hasNext(); ) {
                visit(iter.next(), context);
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
        }

        if (plainSelect.getFromItem() != null) {
            buffer.append(" FROM ");
            if (plainSelect.isUsingOnly()) {
                buffer.append("ONLY ");
            }
            plainSelect.getFromItem().accept(this, context);

            if (plainSelect.getFromItem() instanceof Table) {
                Table table = (Table) plainSelect.getFromItem();
                if (table.getSampleClause() != null) {
                    table.getSampleClause().appendTo(buffer);
                }
            }
        }

        if (plainSelect.getLateralViews() != null) {
            for (LateralView lateralView : plainSelect.getLateralViews()) {
                deparseLateralView(lateralView);
            }
        }

        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                deparseJoin(join);
            }
        }

        if (plainSelect.isUsingFinal()) {
            buffer.append(" FINAL");
        }

        if (plainSelect.getKsqlWindow() != null) {
            buffer.append(" WINDOW ");
            buffer.append(plainSelect.getKsqlWindow().toString());
        }

        deparseWhereClause(plainSelect);

        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(getExpressionVisitor(), context);
        }

        if (plainSelect.getGroupBy() != null) {
            buffer.append(" ");
            new GroupByDeParser(getExpressionVisitor(), buffer).deParse(plainSelect.getGroupBy());
        }

        if (plainSelect.getHaving() != null) {
            buffer.append(" HAVING ");
            plainSelect.getHaving().accept(getExpressionVisitor(), context);
        }
        if (plainSelect.getQualify() != null) {
            buffer.append(" QUALIFY ");
            plainSelect.getQualify().accept(getExpressionVisitor(), context);
        }
        if (plainSelect.getWindowDefinitions() != null) {
            buffer.append(" WINDOW ");
            buffer.append(plainSelect.getWindowDefinitions().stream()
                    .map(WindowDefinition::toString)
                    .collect(joining(", ")));
        }
        if (plainSelect.getForClause() != null) {
            plainSelect.getForClause().appendTo(buffer);
        }

        deparseOrderByElementsClause(plainSelect, plainSelect.getOrderByElements());
        if (plainSelect.isEmitChanges()) {
            buffer.append(" EMIT CHANGES");
        }
        if (plainSelect.getLimitBy() != null) {
            new LimitDeparser(getExpressionVisitor(), buffer).deParse(plainSelect.getLimitBy());
        }
        if (plainSelect.getLimit() != null) {
            new LimitDeparser(getExpressionVisitor(), buffer).deParse(plainSelect.getLimit());
        }
        if (plainSelect.getOffset() != null) {
            visit(plainSelect.getOffset());
        }
        if (plainSelect.getFetch() != null) {
            visit(plainSelect.getFetch());
        }
        if (plainSelect.getIsolation() != null) {
            buffer.append(plainSelect.getIsolation().toString());
        }
        if (plainSelect.getForMode() != null) {
            buffer.append(" FOR ");
            buffer.append(plainSelect.getForMode().getValue());

            if (plainSelect.getForUpdateTable() != null) {
                buffer.append(" OF ").append(plainSelect.getForUpdateTable());
            }
            if (plainSelect.getWait() != null) {
                // wait's toString will do the formatting for us
                buffer.append(plainSelect.getWait());
            }
            if (plainSelect.isNoWait()) {
                buffer.append(" NOWAIT");
            } else if (plainSelect.isSkipLocked()) {
                buffer.append(" SKIP LOCKED");
            }
        }
        if (plainSelect.getOptimizeFor() != null) {
            deparseOptimizeFor(plainSelect.getOptimizeFor());
        }
        if (plainSelect.getForXmlPath() != null) {
            buffer.append(" FOR XML PATH(").append(plainSelect.getForXmlPath()).append(")");
        }
        if (plainSelect.getIntoTempTable() != null) {
            buffer.append(" INTO TEMP ").append(plainSelect.getIntoTempTable());
        }
        if (plainSelect.isUseWithNoLog()) {
            buffer.append(" WITH NO LOG");
        }
        return buffer;
    }

    private void deparseOptimizeFor(OptimizeFor optimizeFor) {
        buffer.append(" OPTIMIZE FOR ");
        buffer.append(optimizeFor.getRowCount());
        buffer.append(" ROWS");
    }

    @Override
    protected void deparseWhereClause(PlainSelect plainSelect) {
        if (plainSelect.getWhere() != null) {
            buffer.append(" WHERE ");
            int len = buffer.length();
            plainSelect.getWhere().accept(getExpressionVisitor());
            if (buffer.length() == len) {
                buffer.delete(len - " WHERE ".length(), len);
            }
        }
    }

    protected void deparseDistinctClause(PlainSelect plainSelect, Distinct distinct) {
        if (distinct != null) {
            if (distinct.isUseUnique()) {
                buffer.append("UNIQUE ");
            } else {
                buffer.append("DISTINCT ");
            }
            if (distinct.getOnSelectItems() != null) {
                buffer.append("ON (");
                for (Iterator<SelectItem<?>> iter = distinct.getOnSelectItems().iterator(); iter
                        .hasNext();) {
                    SelectItem<?> selectItem = iter.next();
                    selectItem.accept(this, null);
                    if (iter.hasNext()) {
                        buffer.append(", ");
                    }
                }
                buffer.append(") ");
            }
        }
    }

    protected void deparseSelectItemsClause(PlainSelect plainSelect, List<SelectItem<?>> selectItems) {
        if (selectItems != null) {
            for (Iterator<SelectItem<?>> iter = selectItems.iterator(); iter.hasNext();) {
                SelectItem<?> selectItem = iter.next();
                selectItem.accept(this, null);
                if (iter.hasNext()) {
                    buffer.append(", ");
                }
            }
        }
    }
}
