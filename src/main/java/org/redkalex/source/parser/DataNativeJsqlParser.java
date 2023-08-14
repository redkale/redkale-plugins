/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
public class DataNativeJsqlParser implements DataNativeSqlParser {

    private final ConcurrentHashMap<String, NativeParserInfo> parserInfo = new ConcurrentHashMap();

    @Override
    public NativeSqlStatement parse(java.util.function.Function<Integer, String> signFunc, String nativeSql, Map<String, Object> params) {
        return loadParser(nativeSql).loadStatement(signFunc, params);
    }

    private NativeParserInfo loadParser(String nativeSql) {
        return parserInfo.computeIfAbsent(nativeSql, sql -> {
            try {
                CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
                Select stmt = (Select) sqlParser.Statement();
                PlainSelect selectBody = (PlainSelect) stmt.getSelectBody();
                Expression where = selectBody.getWhere();
                final Set<String> fullNames = new HashSet<String>();
                final AtomicBoolean containsInName = new AtomicBoolean();
                if (where != null) {
                    where.accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(JdbcNamedParameter expr) {
                            super.visit(expr);
                            fullNames.add(expr.getName());
                        }

                        @Override
                        public void visit(InExpression expr) {
                            int size = fullNames.size();
                            super.visit(expr);
                            if (fullNames.size() > size) {
                                containsInName.set(true);
                            }
                        }

                        @Override
                        public void visit(JdbcParameter jdbcParameter) {
                            throw new SourceException("Cannot contains ? JdbcParameter");
                        }
                    });
                }
                return new NativeParserInfo(sql, containsInName.get(), stmt, fullNames);
            } catch (ParseException e) {
                throw new SourceException("parse error, sql: " + sql, e);
            }
        });
    }

    private static class NativeParserInfo {

        //原始sql
        protected final String nativeSql;

        //是否包含InExpression参数名
        protected final boolean existInNamed;

        //Statement对象
        protected final Select select;

        protected final Expression fullWhere;

        //所有参数名
        protected final Set<String> fullNamedSet;

        private final ReentrantLock selectLock = new ReentrantLock();

        private final ConcurrentHashMap<String, NativeSqlStatement> statements = new ConcurrentHashMap();

        public NativeParserInfo(String nativeSql, boolean containsInNamed, Select select, Set<String> fullNamedSet) {
            this.nativeSql = nativeSql;
            this.existInNamed = containsInNamed;
            this.select = select;
            this.fullNamedSet = Collections.unmodifiableSet(fullNamedSet);
            PlainSelect ps = (PlainSelect) select.getSelectBody();
            this.fullWhere = ps.getWhere();
            ps.setWhere(null);
        }

        public NativeSqlStatement loadStatement(java.util.function.Function<Integer, String> signFunc, Map<String, Object> params) {
            if (existInNamed) { //包含In参数名的不缓存
                return createStatement(signFunc, params);
            }
            String key = cacheKey(params);
            return statements.computeIfAbsent(key, k -> createStatement(signFunc, params));
        }

        private NativeSqlStatement createStatement(java.util.function.Function<Integer, String> signFunc, Map<String, Object> params) {
            final DataExpressionDeParser exprDeParser = new DataExpressionDeParser(signFunc, params);
            final SelectDeParser selectParser = new SelectDeParser(exprDeParser, exprDeParser.getBuffer());
            exprDeParser.setSelectVisitor(selectParser);
            PlainSelect selectBody = (PlainSelect) select.getSelectBody();
            String whereSql = exprDeParser.deParser(fullWhere);
            NativeSqlStatement statement = new NativeSqlStatement();
            statement.setExistInNamed(existInNamed);
            statement.setParamNames(exprDeParser.getParamNames());
            statement.setParamValues(params);
            if (whereSql.isEmpty()) {
                statement.setNativeSql(select.toString());
            } else {
                statement.setNativeSql(select.toString() + " WHERE " + whereSql);
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
}
