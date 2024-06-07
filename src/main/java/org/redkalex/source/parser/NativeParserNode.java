/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.redkale.annotation.Nullable;
import org.redkale.source.DataNativeSqlStatement;
import org.redkale.source.RowBound;
import org.redkale.source.SourceException;
import org.redkale.util.Utility;

/**
 * 每一个不带${xx}的sql对应一个此对象
 *
 * @author zhangjx
 */
public class NativeParserNode {

    // NativeParserInfo
    private final NativeParserInfo info;

    // 不带${xx}的sql模板
    private final String jdbcSql;

    // 是否需要countSql
    private final boolean countable;

    // Statement对象
    private final Statement originStmt;

    // COUNT语句的SelectItem
    @Nullable
    private final List<SelectItem<?>> countSelectItems;

    // 缓存
    private final ConcurrentHashMap<String, DataNativeSqlStatement> statements = new ConcurrentHashMap();

    public NativeParserNode(NativeParserInfo info, String jdbcSql, boolean countable, Statement originStmt) {
        this.info = info;
        this.jdbcSql = jdbcSql;
        this.countable = countable;
        this.originStmt = originStmt;
        this.countSelectItems = createCountSelectItems();
    }

    public DataNativeSqlStatement loadStatement(RowBound round, Map<String, Object> fullParams) {
        if (info.isDynamic()) { // 根据${xx}参数值动态生成的sql语句不缓存
            return createStatement(round, fullParams);
        }
        return statements.computeIfAbsent(cacheKey(round, fullParams), k -> createStatement(round, fullParams));
    }

    protected DataNativeSqlStatement createStatement(RowBound round, Map<String, Object> fullParams) {
        final NativeExprDeParser exprDeParser = new NativeExprDeParser(info.signFunc(), fullParams);
        String stmtSql = exprDeParser.deParseSql(originStmt);
        List<String> jdbcNames = exprDeParser.getJdbcNames();
        String pageSql = null;
        String countSql = null;
        List<String> paramNames = new ArrayList<>();
        for (String name : jdbcNames) { // jdbcNames在exprDeParser.deParseSql执行之后才有值
            paramNames.add(info.jdbcToNumsignMap == null ? name : info.jdbcToNumsignMap.getOrDefault(name, name));
        }
        if (countable) {
            String dbtype = info.getDbType();
            // 生成COUNT语句
            PlainSelect select = (PlainSelect) originStmt;
            exprDeParser.reset();
            StringBuilder buffer = exprDeParser.getBuffer();
            NativeCountDeParser countDeParser = new NativeCountDeParser(exprDeParser, buffer);
            exprDeParser.setSelectVisitor(countDeParser);
            countDeParser.initCountSelect(select, countSelectItems);
            select.accept(countDeParser);
            countSql = buffer.toString();
            if (RowBound.validLimit(round)) {
                if ("oracle".equals(dbtype)) {
                    paramNames.add("#start");
                    paramNames.add("#end");
                    fullParams.put("#start", round.getOffset());
                    fullParams.put("#end", round.getOffset() + round.getLimit());
                    String startParam = info.signFunc().apply(paramNames.size() - 1);
                    String endParam = info.signFunc().apply(paramNames.size());
                    pageSql = "SELECT * FROM (SELECT T_.*, ROWNUM RN_ FROM (" + stmtSql + ") T_) WHERE RN_ BETWEEN "
                            + startParam + " AND " + endParam;
                } else if ("sqlserver".equals(dbtype)) {
                    paramNames.add("#offset");
                    paramNames.add("#limit");
                    fullParams.put("#offset", round.getOffset());
                    fullParams.put("#limit", round.getLimit());
                    String offsetParam = info.signFunc().apply(paramNames.size() - 1);
                    String limitParam = info.signFunc().apply(paramNames.size());
                    pageSql = stmtSql + " OFFSET " + offsetParam + " ROWS FETCH NEXT " + limitParam + " ROWS ONLY";
                } else { // 按mysql、postgresql、mariadb、h2处理
                    paramNames.add("#limit");
                    paramNames.add("#offset");
                    fullParams.put("#limit", round.getLimit());
                    fullParams.put("#offset", round.getOffset());
                    String limitParam = info.signFunc().apply(paramNames.size() - 1);
                    String offsetParam = info.signFunc().apply(paramNames.size());
                    pageSql = stmtSql + " LIMIT " + limitParam + " OFFSET " + offsetParam;
                }
            }
        }
        DataNativeSqlStatement result = new DataNativeSqlStatement();
        result.setJdbcNames(jdbcNames);
        result.setParamNames(paramNames);
        result.setParamValues(fullParams);
        result.setNativeSql(stmtSql);
        result.setNativePageSql(pageSql);
        result.setNativeCountSql(countSql);
        return result;
    }

    private List<SelectItem<?>> createCountSelectItems() {
        if (!countable) {
            return null;
        }
        if (!(originStmt instanceof PlainSelect)) {
            throw new SourceException("Not support count-sql (" + jdbcSql + "), type: "
                    + originStmt.getClass().getName());
        }
        PlainSelect select = (PlainSelect) originStmt;
        if (select.getDistinct() == null) {
            Expression countFunc = new net.sf.jsqlparser.expression.Function()
                    .withName("COUNT")
                    .withParameters(new ExpressionList(new LongValue(1)));
            return Utility.ofList(new SelectItem(countFunc));
        } else {
            List<Expression> exprs = select.getSelectItems().stream()
                    .map(SelectItem::getExpression)
                    .collect(Collectors.toList());
            Expression countFunc = new net.sf.jsqlparser.expression.Function()
                    .withName("COUNT")
                    .withDistinct(true)
                    .withParameters(new ParenthesedExpressionList(exprs));
            return Utility.ofList(new SelectItem(countFunc));
        }
    }

    private String cacheKey(RowBound round, Map<String, Object> params) {
        List<String> list =
                info.fullJdbcNames.stream().filter(params::containsKey).collect(Collectors.toList());
        // fullJdbcNames是TreeSet, 已排序  //Collections.sort(list);
        return (RowBound.validLimit(round) ? "1:" : "0:")
                + (list.isEmpty() ? "" : list.stream().collect(Collectors.joining(",")));
    }
}
