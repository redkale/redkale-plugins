/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.redkale.source.DataNativeSqlStatement;
import org.redkale.source.SourceException;

/**
 *
 * @author zhangjx
 */
public class NativeParserNode {

    //sql是否根据参数值动态生成的，包含了IN或#{xx.xx}参数的
    private final boolean dynamic;

    //Statement对象
    private final Statement stmt;

    //原始sql的 COUNT(1)版
    private final PlainSelect countStmt;

    //where条件
    private final Expression fullWhere;

    //jdbc参数名:argxxx对应${xx.xx}参数名
    private final Map<String, String> jdbcDollarMap;

    //没有where的UPDATE语句
    private final String updateNoWhereSql;

    //修改项，只有UPDATE语句才有值
    private final List<String> updateJdbcNames;

    //必需的参数名, 包含了updateJdbcNames
    private final Set<String> requiredJdbcNames;

    //所有参数名，包含了requiredJdbcNames
    private final TreeSet<String> fullJdbcNames;

    private final ReentrantLock whereLock = new ReentrantLock();

    //缓存
    private final ConcurrentHashMap<String, DataNativeSqlStatement> statements = new ConcurrentHashMap();

    public NativeParserNode(Statement stmt, PlainSelect countStmt, Expression fullWhere, Map<String, String> jdbcDollarMap,
        Set<String> fullJdbcNames, Set<String> requiredJdbcNames, boolean dynamic, String updateNoWhereSql, List<String> updateJdbcNames) {
        this.stmt = stmt;
        this.dynamic = dynamic;
        this.countStmt = countStmt;
        this.fullWhere = fullWhere;
        this.jdbcDollarMap = jdbcDollarMap;
        this.updateNoWhereSql = updateNoWhereSql;
        this.updateJdbcNames = updateJdbcNames;
        this.fullJdbcNames = new TreeSet<>(fullJdbcNames);
        this.requiredJdbcNames = Collections.unmodifiableSet(requiredJdbcNames);
    }

    public DataNativeSqlStatement loadStatement(IntFunction<String> signFunc, Map<String, Object> params) {
        Set<String> miss = null;
        for (String mustName : requiredJdbcNames) {
            if (params.get(mustName) == null) {
                if (miss == null) {
                    miss = new LinkedHashSet<>();
                }
                miss.add(jdbcDollarMap.getOrDefault(mustName, mustName));
            }
        }
        if (miss != null) {
            throw new SourceException("Missing parameter " + miss);
        }
        if (dynamic) { //根据参数值动态生成的sql语句不缓存
            return createStatement(signFunc, params);
        }
        String key = cacheKey(params);
        return statements.computeIfAbsent(key, k -> createStatement(signFunc, params));
    }

    private DataNativeSqlStatement createStatement(IntFunction<String> signFunc, Map<String, Object> params) {
        final NativeExprDeParser exprDeParser = new NativeExprDeParser(signFunc, params);
        if (updateJdbcNames != null) {
            exprDeParser.getJdbcNames().addAll(updateJdbcNames);
        }
        String whereSql = exprDeParser.deParser(fullWhere);
        DataNativeSqlStatement statement = new DataNativeSqlStatement();
        statement.setJdbcNames(exprDeParser.getJdbcNames());
        List<String> paramNames = new ArrayList<>();
        for (String name : statement.getJdbcNames()) {
            paramNames.add(jdbcDollarMap == null ? name : jdbcDollarMap.getOrDefault(name, name));
        }
        statement.setParamNames(paramNames);
        statement.setParamValues(params);
        if (whereSql.isEmpty()) {
            statement.setNativeSql(updateNoWhereSql == null ? stmt.toString() : updateNoWhereSql);
            if (countStmt != null) {
                statement.setNativeCountSql(countStmt.toString());
            }
        } else if (countStmt != null) {  //SELECT
            String stmtSql;
            String countSql;
            PlainSelect queryStmt = (PlainSelect) stmt;
            whereLock.lock();
            Expression oldQueryWhere = queryStmt.getWhere();
            Expression oldCountWhere = countStmt.getWhere();
            try {
                queryStmt.setWhere(new NativeSqlExpression(whereSql));
                stmtSql = queryStmt.toString();
                countStmt.setWhere(new NativeSqlExpression(whereSql));
                countSql = countStmt.toString();
            } finally {
                queryStmt.setWhere(oldQueryWhere);
                countStmt.setWhere(oldCountWhere);
                whereLock.unlock();
            }
            statement.setNativeSql(stmtSql);
            statement.setNativeCountSql(countSql);
        } else { //not SELECT
            statement.setNativeSql((updateNoWhereSql == null ? stmt.toString() : updateNoWhereSql) + " WHERE " + whereSql);
        }
        return statement;
    }

    private String cacheKey(Map<String, Object> params) {
        List<String> list = fullJdbcNames.stream().filter(params::containsKey).collect(Collectors.toList());
        if (list.isEmpty()) {
            return "";
        }
        //TreeSet已是排序 Collections.sort(list);
        return list.stream().collect(Collectors.joining(","));
    }

    public static class NativeSqlExpression implements Expression {

        private final String sql;

        public NativeSqlExpression(String sql) {
            this.sql = Objects.requireNonNull(sql);
        }

        @Override
        public void accept(ExpressionVisitor expressionVisitor) {
            //do nothing
        }

        @Override
        public SimpleNode getASTNode() {
            return null;
        }

        @Override
        public void setASTNode(SimpleNode node) {
            //do nothing
        }

        @Override
        public String toString() {
            return sql;
        }

    }
}
