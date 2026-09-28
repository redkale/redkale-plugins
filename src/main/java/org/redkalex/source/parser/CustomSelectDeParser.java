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
        List<WithItem<?>> withItemsList = plainSelect.getWithItemsList();
        if (withItemsList != null && !withItemsList.isEmpty()) {
            builder.append("WITH ");
            for (Iterator<WithItem<?>> iter = withItemsList.iterator(); iter.hasNext(); ) {
                iter.next().accept((SelectVisitor<?>) this, context);
                if (iter.hasNext()) {
                    builder.append(",");
                }
                builder.append(" ");
            }
        }

        builder.append("SELECT ");

        if (plainSelect.getMySqlHintStraightJoin()) {
            builder.append("STRAIGHT_JOIN ");
        }

        OracleHint hint = plainSelect.getOracleHint();
        if (hint != null) {
            builder.append(hint).append(" ");
        }

        Skip skip = plainSelect.getSkip();
        if (skip != null) {
            builder.append(skip).append(" ");
        }

        First first = plainSelect.getFirst();
        if (first != null) {
            builder.append(first).append(" ");
        }

        deparseDistinctClause(plainSelect, plainSelect.getDistinct());

        if (plainSelect.getBigQuerySelectQualifier() != null) {
            switch (plainSelect.getBigQuerySelectQualifier()) {
                case AS_STRUCT:
                    builder.append("AS STRUCT ");
                    break;
                case AS_VALUE:
                    builder.append("AS VALUE ");
                    break;
            }
        }

        Top top = plainSelect.getTop();
        if (top != null) {
            visit(top);
        }

        if (plainSelect.getMySqlSqlCacheFlag() != null) {
            builder.append(plainSelect.getMySqlSqlCacheFlag().name()).append(" ");
        }

        if (plainSelect.getMySqlSqlCalcFoundRows()) {
            builder.append("SQL_CALC_FOUND_ROWS").append(" ");
        }

        deparseSelectItemsClause(plainSelect, plainSelect.getSelectItems());

        if (plainSelect.getIntoTables() != null) {
            builder.append(" INTO ");
            for (Iterator<Table> iter = plainSelect.getIntoTables().iterator(); iter.hasNext(); ) {
                visit(iter.next(), context);
                if (iter.hasNext()) {
                    builder.append(", ");
                }
            }
        }

        if (plainSelect.getFromItem() != null) {
            builder.append(" FROM ");
            if (plainSelect.isUsingOnly()) {
                builder.append("ONLY ");
            }
            plainSelect.getFromItem().accept(this, context);

            if (plainSelect.getFromItem() instanceof Table) {
                Table table = (Table) plainSelect.getFromItem();
                if (table.getSampleClause() != null) {
                    table.getSampleClause().appendTo(builder);
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
            builder.append(" FINAL");
        }

        if (plainSelect.getKsqlWindow() != null) {
            builder.append(" WINDOW ");
            builder.append(plainSelect.getKsqlWindow().toString());
        }

        deparseWhereClause(plainSelect);

        if (plainSelect.getOracleHierarchical() != null) {
            plainSelect.getOracleHierarchical().accept(getExpressionVisitor(), context);
        }

        if (plainSelect.getGroupBy() != null) {
            builder.append(" ");
            new GroupByDeParser(getExpressionVisitor(), builder).deParse(plainSelect.getGroupBy());
        }

        if (plainSelect.getHaving() != null) {
            builder.append(" HAVING ");
            plainSelect.getHaving().accept(getExpressionVisitor(), context);
        }
        if (plainSelect.getQualify() != null) {
            builder.append(" QUALIFY ");
            plainSelect.getQualify().accept(getExpressionVisitor(), context);
        }
        if (plainSelect.getWindowDefinitions() != null) {
            builder.append(" WINDOW ");
            builder.append(plainSelect.getWindowDefinitions().stream()
                    .map(WindowDefinition::toString)
                    .collect(joining(", ")));
        }
        if (plainSelect.getForClause() != null) {
            plainSelect.getForClause().appendTo(builder);
        }

        deparseOrderByElementsClause(plainSelect, plainSelect.getOrderByElements());
        if (plainSelect.isEmitChanges()) {
            builder.append(" EMIT CHANGES");
        }
        if (plainSelect.getLimitBy() != null) {
            new LimitDeparser(getExpressionVisitor(), builder).deParse(plainSelect.getLimitBy());
        }
        if (plainSelect.getLimit() != null) {
            new LimitDeparser(getExpressionVisitor(), builder).deParse(plainSelect.getLimit());
        }
        if (plainSelect.getOffset() != null) {
            visit(plainSelect.getOffset());
        }
        if (plainSelect.getFetch() != null) {
            visit(plainSelect.getFetch());
        }
        if (plainSelect.getIsolation() != null) {
            builder.append(plainSelect.getIsolation().toString());
        }
        if (plainSelect.getForMode() != null) {
            builder.append(" FOR ");
            builder.append(plainSelect.getForMode().getValue());

            if (plainSelect.getForUpdateTable() != null) {
                builder.append(" OF ").append(plainSelect.getForUpdateTable());
            }
            if (plainSelect.getWait() != null) {
                // wait's toString will do the formatting for us
                builder.append(plainSelect.getWait());
            }
            if (plainSelect.isNoWait()) {
                builder.append(" NOWAIT");
            } else if (plainSelect.isSkipLocked()) {
                builder.append(" SKIP LOCKED");
            }
        }
        if (plainSelect.getOptimizeFor() != null) {
            deparseOptimizeFor(plainSelect.getOptimizeFor());
        }
        if (plainSelect.getForXmlPath() != null) {
            builder.append(" FOR XML PATH(").append(plainSelect.getForXmlPath()).append(")");
        }
        if (plainSelect.getIntoTempTable() != null) {
            builder.append(" INTO TEMP ").append(plainSelect.getIntoTempTable());
        }
        if (plainSelect.isUseWithNoLog()) {
            builder.append(" WITH NO LOG");
        }
        return builder;
    }

    private void deparseOptimizeFor(OptimizeFor optimizeFor) {
        builder.append(" OPTIMIZE FOR ");
        builder.append(optimizeFor.getRowCount());
        builder.append(" ROWS");
    }

    @Override
    protected void deparseWhereClause(PlainSelect plainSelect) {
        if (plainSelect.getWhere() != null) {
            builder.append(" WHERE ");
            int len = builder.length();
            plainSelect.getWhere().accept(getExpressionVisitor());
            if (builder.length() == len) {
                builder.delete(len - " WHERE ".length(), len);
            }
        }
    }

    protected void deparseDistinctClause(PlainSelect plainSelect, Distinct distinct) {
        if (distinct != null) {
            if (distinct.isUseUnique()) {
                builder.append("UNIQUE ");
            } else {
                builder.append("DISTINCT ");
            }
            if (distinct.getOnSelectItems() != null) {
                builder.append("ON (");
                for (Iterator<SelectItem<?>> iter = distinct.getOnSelectItems().iterator(); iter
                        .hasNext();) {
                    SelectItem<?> selectItem = iter.next();
                    selectItem.accept(this, null);
                    if (iter.hasNext()) {
                        builder.append(", ");
                    }
                }
                builder.append(") ");
            }
        }
    }

    protected void deparseSelectItemsClause(PlainSelect plainSelect, List<SelectItem<?>> selectItems) {
        if (selectItems != null) {
            for (Iterator<SelectItem<?>> iter = selectItems.iterator(); iter.hasNext();) {
                SelectItem<?> selectItem = iter.next();
                selectItem.accept(this, null);
                if (iter.hasNext()) {
                    builder.append(", ");
                }
            }
        }
    }
}
