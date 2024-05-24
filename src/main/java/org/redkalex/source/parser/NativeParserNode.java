/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.redkale.source.DataNativeSqlStatement;
import org.redkale.source.SourceException;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class NativeParserNode {

    //NativeParserInfo
    private final NativeParserInfo info;

    //是否需要countSql
    private final boolean countable;

    //Statement对象
    private final Statement originStmt;

    //where锁
    final ReentrantLock countLock = new ReentrantLock();

    //缓存
    private final ConcurrentHashMap<String, DataNativeSqlStatement> statements = new ConcurrentHashMap();

    public NativeParserNode(NativeParserInfo info, boolean countable, Statement originStmt) {
        this.info = info;
        this.originStmt = originStmt;
        this.countable = countable;
    }

    public DataNativeSqlStatement loadStatement(NativeSqlTemplet templet) {
        if (info.isDynamic()) { //根据参数值动态生成的sql语句不缓存
            return createStatement(templet);
        }
        return statements.computeIfAbsent(cacheKey(templet.getTempletParams()), k -> createStatement(templet));
    }

    protected DataNativeSqlStatement createStatement(NativeSqlTemplet templet) {
        Map<String, Object> fullParams = templet.getTempletParams();
        final NativeExprDeParser exprDeParser = new NativeExprDeParser(info.signFunc(), fullParams);
        String stmtSql = null;
        String countSql = null;
        List<String> jdbcNames = null;
        if (countable) {
            if (!(originStmt instanceof PlainSelect)) {
                throw new SourceException("Not support count-sql (" + templet.getJdbcSql() + "), type: " + originStmt.getClass().getName());
            }
            PlainSelect select = (PlainSelect) originStmt;
            Distinct distinct = select.getDistinct();
            List<SelectItem<?>> selectItems = select.getSelectItems();
            List<OrderByElement> orderByElements = select.getOrderByElements();
            countLock.lock();
            try {
                stmtSql = exprDeParser.deParseSql(originStmt);
                jdbcNames = exprDeParser.getJdbcNames();
                select.setOrderByElements(null);
                if (select.getDistinct() == null) {
                    Expression countFunc = new net.sf.jsqlparser.expression.Function().withName("COUNT")
                        .withParameters(new ExpressionList(new LongValue(1)));
                    select.setSelectItems(Utility.ofList(new SelectItem(countFunc)));
                } else {
                    List<Expression> exprs = selectItems.stream().map(SelectItem::getExpression).collect(Collectors.toList());
                    Expression countFunc = new net.sf.jsqlparser.expression.Function().withName("COUNT").withDistinct(true)
                        .withParameters(new ParenthesedExpressionList(exprs));
                    select.setSelectItems(Utility.ofList(new SelectItem(countFunc)));
                    select.setDistinct(null);
                }
                countSql = exprDeParser.reset().deParseSql(select);
            } finally {
                select.setDistinct(distinct);
                select.setSelectItems(selectItems);
                select.setOrderByElements(orderByElements);
                countLock.unlock();
            }
        } else {
            stmtSql = exprDeParser.deParseSql(originStmt);
            jdbcNames = exprDeParser.getJdbcNames();
        }
        DataNativeSqlStatement result = new DataNativeSqlStatement();
        result.setJdbcNames(jdbcNames);
        List<String> paramNames = new ArrayList<>();
        for (String name : jdbcNames) {
            paramNames.add(info.jdbcToNumsignMap == null ? name : info.jdbcToNumsignMap.getOrDefault(name, name));
        }
        result.setParamNames(paramNames);
        result.setParamValues(fullParams);
        result.setNativeSql(stmtSql);
        result.setNativeCountSql(countSql);
        return result;
    }

    private String cacheKey(Map<String, Object> params) {
        List<String> list = info.fullJdbcNames.stream().filter(params::containsKey).collect(Collectors.toList());
        //fullJdbcNames是TreeSet, 已排序  //Collections.sort(list);
        return list.isEmpty() ? "" : list.stream().collect(Collectors.joining(","));
    }

}
