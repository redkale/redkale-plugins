/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.*;
import java.util.stream.Collectors;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.util.deparser.*;
import org.redkale.annotation.ResourceType;
import org.redkale.source.*;

/**
 *
 * @author zhangjx
 */
@ResourceType(DataNativeSqlParser.class)
public class DataNativeJsqlParser implements DataNativeSqlParser {

    protected final Logger logger = Logger.getLogger(DataNativeJsqlParser.class.getSimpleName());

    private final ConcurrentHashMap<String, NativeParserInfo> parserInfo = new ConcurrentHashMap();

    @Override
    public NativeSqlStatement parse(java.util.function.Function<Integer, String> signFunc, String nativeSql, Map<String, Object> params) {
        NativeSqlStatement statement = loadParser(signFunc, nativeSql).loadStatement(signFunc, params);
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, "sql = " + nativeSql + ", nativeSql = " + statement.getNativeSql() + ", paramNames = " + statement.getParamNames());
        }
        return statement;
    }

    private NativeParserInfo loadParser(java.util.function.Function<Integer, String> signFunc, String nativeSql) {
        return parserInfo.computeIfAbsent(nativeSql, sql -> {
            try {
                CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
                Statement stmt = sqlParser.Statement();
                Expression where = null;
                Runnable clearWhere = null;
                List<UpdateSet> updateSets = null;
                if (stmt instanceof Select) {
                    PlainSelect selectBody = (PlainSelect) ((Select) stmt).getSelectBody();
                    where = selectBody.getWhere();
                    clearWhere = () -> selectBody.setWhere(null);
                } else if (stmt instanceof Insert) {
                    Select select = ((Insert) stmt).getSelect();
                    SelectBody selectBody = select.getSelectBody();
                    if (selectBody instanceof PlainSelect) {
                        where = ((PlainSelect) selectBody).getWhere();
                        clearWhere = () -> ((PlainSelect) selectBody).setWhere(null);
                    }
                } else if (stmt instanceof Delete) {
                    where = ((Delete) stmt).getWhere();
                    clearWhere = () -> ((Delete) stmt).setWhere(null);
                } else if (stmt instanceof Update) {
                    updateSets = ((Update) stmt).getUpdateSets();
                    where = ((Update) stmt).getWhere();
                    clearWhere = () -> ((Update) stmt).setWhere(null);
                } else {
                    throw new SourceException("Not support sql (" + sql + ") ");
                }
                final Set<String> fullNames = new HashSet<String>();
                final Set<String> mustNames = new HashSet<String>();
                final AtomicBoolean containsInName = new AtomicBoolean();
                ExpressionVisitorAdapter exprAdapter = new ExpressionVisitorAdapter() {

                    @Override
                    public void visit(JdbcNamedParameter expr) {
                        super.visit(expr);
                        mustNames.add(expr.getName());
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
                };
                SelectDeParser selectAdapter = new SelectDeParser();
                selectAdapter.setExpressionVisitor(exprAdapter);
                exprAdapter.setSelectVisitor(selectAdapter);
                if (where != null) {
                    where.accept(exprAdapter);
                    mustNames.clear(); //where不存在必需的参数名
                }
                if (updateSets != null) {
                    for (UpdateSet set : updateSets) {
                        for (Expression expr : set.getExpressions()) {
                            expr.accept(exprAdapter);
                        }
                    }
                }
                if (clearWhere != null) {
                    clearWhere.run(); //必须清空where条件
                }

                String updateSql = null;
                List<String> updateNamedSet = new ArrayList<>();
                if (updateSets != null) {
                    Map<String, Object> params = new HashMap<>();
                    Object val = List.of(1); //虚构一个参数值，IN需要Collection
                    for (String name : mustNames) {
                        params.put(name, val);
                    }
                    final DataExpressionDeParser exprDeParser = new DataExpressionDeParser(signFunc, params);
                    UpdateDeParser deParser = new UpdateDeParser(exprDeParser, exprDeParser.getBuffer());
                    deParser.deParse((Update) stmt);
                    updateSql = exprDeParser.getBuffer().toString();
                    updateNamedSet = exprDeParser.getParamNames();
                }
                return new NativeParserInfo(sql, containsInName.get(), stmt, where, fullNames, mustNames, updateSql, updateNamedSet);
            } catch (ParseException e) {
                throw new SourceException("Parse error, sql: " + sql, e);
            }
        });
    }

    protected static class NativeParserInfo {

        //原始sql
        protected final String nativeSql;

        //是否包含InExpression参数名
        protected final boolean existInNamed;

        //Statement对象
        protected final Statement stmt;

        //where条件
        protected final Expression fullWhere;

        //没有where的UPDATE语句
        protected final String updateSql;

        //修改项，只有UPDATE语句才有值
        protected final List<String> updateNamedSet;

        //必须要有的参数名
        protected final Set<String> mustNamedSet;

        //所有参数名，包含了mustNamedSet
        protected final Set<String> fullNamedSet;

        //缓存
        private final ConcurrentHashMap<String, NativeSqlStatement> statements = new ConcurrentHashMap();

        public NativeParserInfo(String nativeSql, boolean containsInNamed, Statement stmt, Expression fullWhere,
            Set<String> fullNamedSet, Set<String> mustNamedSet, String updateSql, List<String> updateNamedSet) {
            this.nativeSql = nativeSql;
            this.existInNamed = containsInNamed;
            this.stmt = stmt;
            this.fullWhere = fullWhere;
            this.updateSql = updateSql;
            this.updateNamedSet = updateNamedSet;
            this.fullNamedSet = Collections.unmodifiableSet(fullNamedSet);
            this.mustNamedSet = Collections.unmodifiableSet(mustNamedSet);
        }

        public NativeSqlStatement loadStatement(java.util.function.Function<Integer, String> signFunc, Map<String, Object> params) {
            Set<String> miss = null;
            for (String mustName : mustNamedSet) {
                if (!params.containsKey(mustName)) {
                    if (miss == null) {
                        miss = new LinkedHashSet<>();
                    }
                    miss.add(mustName);
                }
            }
            if (miss != null) {
                throw new SourceException("Missing parameter " + miss);
            }
            if (existInNamed) { //包含In参数名的不缓存
                return createStatement(signFunc, params);
            }
            String key = cacheKey(params);
            return statements.computeIfAbsent(key, k -> createStatement(signFunc, params));
        }

        private NativeSqlStatement createStatement(java.util.function.Function<Integer, String> signFunc, Map<String, Object> params) {
            final DataExpressionDeParser exprDeParser = new DataExpressionDeParser(signFunc, params);
            if (updateNamedSet != null) {
                exprDeParser.getParamNames().addAll(updateNamedSet);
            }
            String whereSql = exprDeParser.deParser(fullWhere);
            NativeSqlStatement statement = new NativeSqlStatement();
            statement.setExistInNamed(existInNamed);
            statement.setParamNames(exprDeParser.getParamNames());
            statement.setParamValues(params);
            if (whereSql.isEmpty()) {
                statement.setNativeSql(updateSql == null ? stmt.toString() : updateSql);
            } else {
                statement.setNativeSql((updateSql == null ? stmt.toString() : updateSql) + " WHERE " + whereSql);
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
