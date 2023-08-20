/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import org.redkale.source.DataNativeSqlParser.NativeSqlStatement;
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
    private final Statement countStmt;

    //where条件
    private final Expression fullWhere;

    //jdbc参数名:argxxx对应${xx.xx}参数名
    private final Map<String, String> jdbcDollarNames;

    //没有where的UPDATE语句
    private final String updateSql;

    //修改项，只有UPDATE语句才有值
    private final List<String> updateNamedSet;

    //必须要有的参数名, 包含了updateNamedSet
    private final Set<String> requiredNamedSet;

    //所有参数名，包含了requiredNamedSet
    private final Set<String> fullNamedSet;

    //缓存
    private final ConcurrentHashMap<String, NativeSqlStatement> statements = new ConcurrentHashMap();

    public NativeParserNode(Statement stmt, Statement countStmt, Expression fullWhere, Map<String, String> jdbcDollarNames,
        Set<String> fullNamedSet, Set<String> requiredNamedSet, boolean dynamic, String updateSql, List<String> updateNamedSet) {
        this.stmt = stmt;
        this.dynamic = dynamic;
        this.countStmt = countStmt;
        this.fullWhere = fullWhere;
        this.jdbcDollarNames = jdbcDollarNames;
        this.updateSql = updateSql;
        this.updateNamedSet = updateNamedSet;
        this.fullNamedSet = Collections.unmodifiableSet(fullNamedSet);
        this.requiredNamedSet = Collections.unmodifiableSet(requiredNamedSet);
    }

    public NativeSqlStatement loadStatement(java.util.function.Function<Integer, String> signFunc, Map<String, Object> params) {
        Set<String> miss = null;
        for (String mustName : requiredNamedSet) {
            if (params.get(mustName) == null) {
                if (miss == null) {
                    miss = new LinkedHashSet<>();
                }
                miss.add(jdbcDollarNames.getOrDefault(mustName, mustName));
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

    private NativeSqlStatement createStatement(java.util.function.Function<Integer, String> signFunc, Map<String, Object> params) {
        final NativeExprDeParser exprDeParser = new NativeExprDeParser(jdbcDollarNames, signFunc, params);
        if (updateNamedSet != null) {
            exprDeParser.getParamNames().addAll(updateNamedSet);
        }
        String whereSql = exprDeParser.deParser(fullWhere);
        NativeSqlStatement statement = new NativeSqlStatement();
        statement.setParamNames(exprDeParser.getParamNames());
        statement.setParamValues(params);
        if (whereSql.isEmpty()) {
            statement.setNativeSql(updateSql == null ? stmt.toString() : updateSql);
            if (countStmt != null) {
                statement.setNativeCountSql(countStmt.toString());
            }
        } else {
            statement.setNativeSql((updateSql == null ? stmt.toString() : updateSql) + " WHERE " + whereSql);
            if (countStmt != null) {
                statement.setNativeCountSql(countStmt.toString() + " WHERE " + whereSql);
            }
        }
        return statement;
    }

    private String cacheKey(Map<String, Object> params) {
        List<String> list = fullNamedSet.stream().filter(v -> params.containsKey(v)).collect(Collectors.toList());
        if (list.isEmpty()) {
            return "";
        }
        Collections.sort(list);
        return list.stream().collect(Collectors.joining(","));
    }
}
