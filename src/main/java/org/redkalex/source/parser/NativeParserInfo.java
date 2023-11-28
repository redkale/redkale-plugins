/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.util.deparser.*;
import org.redkale.source.SourceException;
import org.redkale.util.*;

/**
 * jsqlparser只能识别:xxx的参数变量形式的sql，而DataNativeSqlParser定义的参数变量形式是: #{xxx}、${xxx}、$${xxx}
 * 此类作用是将原始sql先转换成:name形式的sql再解析出变量参数
 *
 * @author zhangjx
 */
@SuppressWarnings("unchecked")
public class NativeParserInfo {

    //原始sql语句
    private final String rawSql;

    //jdbc版的sql语句, 只有numberSignNames为空时才有值
    private final String jdbcSql;

    //#{xx.xx}的参数名
    private final Map<String, NativeSqlParameter> numberSignNames = new HashMap<>();

    //${xx.xx}参数名对应jdbc参数名:argxxx, 包含了requiredDollarNames
    private final Map<String, NativeSqlParameter> dollarJdbcNames = new HashMap<>();

    //必需的$${xx.xx}参数名
    private final Map<String, NativeSqlParameter> requiredDollarNames = new HashMap<>();

    //jdbc参数名:argxxx对应${xx.xx}参数名
    private final Map<String, String> jdbcDollarMap = new HashMap<>();

    //根据#{xx.xx}分解并将${xx.xx}替换成:argxxx的sql片段
    private final List<NativeSqlFragment> fragments = new ArrayList<>();

    private final List<NativeSqlParameter> allNamedParameters = new ArrayList<>();

    private final ConcurrentHashMap<String, NativeParserNode> parserNodes = new ConcurrentHashMap();

    public NativeParserInfo(final String rawSql) {
        this.rawSql = rawSql;
        //解析sql
        boolean paraming = false;
        StringBuilder sb = new StringBuilder();
        final char[] chars = Utility.charArray(rawSql);
        char last = 0;
        boolean dyn = false;
        int type = 0; //1:#{xx.xx}, 2:${xx.xx}, 3:$${xx.xx}
        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];
            if (ch == '{') {
                if (paraming || i < 2) {
                    throw new SourceException("Parse error, sql: " + rawSql);
                }
                if (last == '#') {
                    fragments.add(new NativeSqlFragment(false, sb.substring(0, sb.length() - 1)));
                    sb.delete(0, sb.length());
                    type = 1;
                    paraming = true;
                } else if (last == '$') {
                    type = chars[i - 2] == '$' ? 3 : 2;
                    fragments.add(new NativeSqlFragment(false, sb.substring(0, sb.length() + 1 - type)));
                    sb.delete(0, sb.length());
                    paraming = true;
                }
            } else if (ch == '}') {
                if (!paraming) {
                    throw new SourceException("Parse error, sql: " + rawSql);
                }
                String name = sb.toString();
                sb.delete(0, sb.length());
                if (type == 1) { //#{xx.xx}
                    numberSignNames.put(name, new NativeSqlParameter(name, name, true));
                    fragments.add(new NativeSqlFragment(true, name));
                    dyn = true;
                } else if (type >= 2) { //${xx.xx}、$${xx.xx}
                    NativeSqlParameter old = dollarJdbcNames.get(name);
                    String jdbc = old == null ? null : old.getJdbcName();
                    if (jdbc == null) {
                        int seqno = dollarJdbcNames.size() + 1;
                        jdbc = "arg" + (seqno >= 10 ? seqno : ("0" + seqno));
                        NativeSqlParameter p = new NativeSqlParameter(name, jdbc, type == 3);
                        dollarJdbcNames.put(name, p);
                        jdbcDollarMap.put(jdbc, name);
                        if (p.isRequired()) {
                            requiredDollarNames.put(name, p);
                        }
                    }
                    fragments.add(new NativeSqlFragment(false, ":" + jdbc));
                }
                paraming = false;
            } else {
                sb.append(ch);
            }
            last = ch;
        }
        if (paraming) {
            throw new SourceException("Parse error, sql: " + rawSql);
        }
        if (sb.length() > 0) {
            fragments.add(new NativeSqlFragment(false, sb.toString()));
        }
        if (numberSignNames.isEmpty()) {
            StringBuilder ss = new StringBuilder();
            for (NativeSqlFragment fragment : fragments) {
                ss.append(fragment.getText());
            }
            this.jdbcSql = ss.toString();
        } else {
            this.jdbcSql = null;
        }
        this.allNamedParameters.addAll(numberSignNames.values());
        this.allNamedParameters.addAll(dollarJdbcNames.values());
    }

    public boolean isDynamic() {
        return jdbcSql == null;
    }

    public Map<String, Object> createNamedParams(ObjectRef<String> newSql, Map<String, Object> params) {
        Map<String, Object> newParams = new HashMap<>(params);
        for (NativeSqlParameter p : allNamedParameters) {
            Object val = p.getParamValue(params);
            if (p.isRequired() && val == null) {
                throw new SourceException("Missing parameter " + p.getDollarName());
            }
            if (val != null) {
                newParams.put(p.getDollarName(), val);
                newParams.put(p.getJdbcName(), val);
            }
        }
        if (jdbcSql == null) {
            StringBuilder sb = new StringBuilder();
            for (NativeSqlFragment fragment : fragments) {
                if (fragment.isSignable()) {
                    sb.append(newParams.get(fragment.getText()));
                } else {
                    sb.append(fragment.getText());
                }
            }
            newSql.set(sb.toString());
        } else {
            newSql.set(jdbcSql);
        }
        return newParams;
    }

    public NativeParserNode loadParserNode(java.util.function.IntFunction<String> signFunc, String dbtype, final String nativeSql) {
        if (isDynamic()) {
            return createParserNode(signFunc, dbtype, nativeSql);
        }
        return parserNodes.computeIfAbsent(nativeSql, sql -> createParserNode(signFunc, dbtype, nativeSql));
    }

    private NativeParserNode createParserNode(java.util.function.IntFunction<String> signFunc, String dbtype, final String nativeSql) {
        try {
            CCJSqlParser sqlParser = new CCJSqlParser(nativeSql).withAllowComplexParsing(true);
            Statement stmt = sqlParser.Statement();
            final Set<String> fullNames = new HashSet<String>();
            final Set<String> requiredNamedSet = new HashSet<String>();
            final AtomicBoolean containsInName = new AtomicBoolean();
            ExpressionVisitorAdapter exprAdapter = new ExpressionVisitorAdapter() {

                @Override
                public void visit(JdbcNamedParameter expr) {
                    super.visit(expr);
                    requiredNamedSet.add(expr.getName());
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

            Statement countStmt = null;
            Expression where = null;
            Runnable clearWhere = null;
            List<UpdateSet> updateSets = null;
            List<SelectBody> insertSets = null;
            if (stmt instanceof Select) {
                PlainSelect selectBody = (PlainSelect) ((Select) stmt).getSelectBody();
                where = selectBody.getWhere();
                clearWhere = () -> selectBody.setWhere(null);
                { //创建COUNT总数sql
                    CCJSqlParser countParser = new CCJSqlParser(nativeSql).withAllowComplexParsing(true);
                    Select countSelect = (Select) countParser.Statement();
                    PlainSelect countBody = (PlainSelect) countSelect.getSelectBody();
                    if (countBody.getDistinct() == null) {
                        Expression countFunc = new net.sf.jsqlparser.expression.Function().withName("COUNT").withParameters(new ExpressionList(new LongValue(1)));
                        countBody.setSelectItems(Utility.ofList(new SelectExpressionItem(countFunc)));
                    } else {
                        Expression countFunc = new net.sf.jsqlparser.expression.Function().withName("COUNT").withDistinct(true)
                            .withParameters(new ExpressionList((List) countBody.getSelectItems()));
                        countBody.setSelectItems(Utility.ofList(new SelectExpressionItem(countFunc)));
                        countBody.setDistinct(null);
                    }
                    countBody.setWhere(null);
                    countStmt = countSelect;
                }
            } else if (stmt instanceof Insert) {
                Select select = ((Insert) stmt).getSelect();
                SelectBody selectBody = select.getSelectBody();
                if (selectBody instanceof PlainSelect) {
                    where = ((PlainSelect) selectBody).getWhere();
                    clearWhere = () -> ((PlainSelect) selectBody).setWhere(null);
                } else if (selectBody instanceof SetOperationList) {
                    insertSets = ((SetOperationList) selectBody).getSelects();
                } else {
                    throw new SourceException("Not support sql (" + rawSql + ") ");
                }
            } else if (stmt instanceof Delete) {
                where = ((Delete) stmt).getWhere();
                clearWhere = () -> ((Delete) stmt).setWhere(null);
            } else if (stmt instanceof Update) {
                updateSets = ((Update) stmt).getUpdateSets();
                where = ((Update) stmt).getWhere();
                clearWhere = () -> ((Update) stmt).setWhere(null);
            } else {
                throw new SourceException("Not support sql (" + rawSql + ") ");
            }

            SelectDeParser selectAdapter = new SelectDeParser();
            selectAdapter.setExpressionVisitor(exprAdapter);
            exprAdapter.setSelectVisitor(selectAdapter);
            if (where != null) {
                where.accept(exprAdapter);
                requiredNamedSet.clear(); //where不存在必需的参数名
            }
            if (insertSets != null) {
                for (SelectBody body : insertSets) {
                    body.accept(selectAdapter);
                }
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
                for (String name : requiredNamedSet) {
                    params.put(name, val);
                }
                final NativeExprDeParser exprDeParser = new NativeExprDeParser(signFunc, params);
                UpdateDeParser deParser = new UpdateDeParser(exprDeParser, exprDeParser.getBuffer());
                deParser.deParse((Update) stmt);
                updateSql = exprDeParser.getBuffer().toString();
                updateNamedSet = exprDeParser.getJdbcNames();
            }
            return new NativeParserNode(stmt, countStmt, where, jdbcDollarMap,
                fullNames, requiredNamedSet, isDynamic() || containsInName.get(), updateSql, updateNamedSet);
        } catch (ParseException e) {
            throw new SourceException("Parse error, sql: " + rawSql, e);
        }
    }

    @Override
    public String toString() {
        return NativeParserInfo.class.getSimpleName() + "{"
            + "rawSql: \"" + rawSql + "\""
            + ", jdbcSql: \"" + jdbcSql + "\""
            + ", numberSignNames: " + numberSignNames
            + ", dollarJdbcNames: " + dollarJdbcNames
            + ", requiredDollarNames: " + requiredDollarNames
            + ", jdbcDollarMap: " + jdbcDollarMap
            + "}";
    }

}
