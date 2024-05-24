/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.grant.Grant;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.upsert.Upsert;
import net.sf.jsqlparser.util.deparser.*;
import org.redkale.source.DataNativeSqlInfo;
import org.redkale.source.SourceException;
import org.redkale.util.*;

/**
 * jsqlparser只能识别:xxx的参数变量形式的sql，而DataNativeSqlParser定义的参数变量形式是: ${xxx}、#{xxx}、##{xxx}
 * 此类作用是将原始sql先转换成:name形式的sql再解析出变量参数
 * 注意: 目前不支持union sql
 *
 * @author zhangjx
 */
@SuppressWarnings("unchecked")
public class NativeParserInfo extends DataNativeSqlInfo {

    //${xx.xx}的拼接参数名
    private final Map<String, NativeSqlParameter> dollarNames = new HashMap<>();

    //#{xx.xx}参数名对应jdbc参数名:argxxx, 包含了requiredNumsignNames ##{xx.xx}
    //key: xx.xx
    private final Map<String, NativeSqlParameter> numsignJdbcNames = new HashMap<>();

    //必需的##{xx.xx}参数名
    //key: xx.xx
    private final Map<String, NativeSqlParameter> requiredNumsignNames = new HashMap<>();

    //jdbc参数名:argxxx对应#{xx.xx}参数名
    //key: arg0x, value: xx.xx
    private final Map<String, String> jdbcToNumsignMap = new HashMap<>();

    //根据${xx.xx}分解并将xx.xx替换成:argxxx的sql片段
    private final List<NativeSqlFragment> fragments = new ArrayList<>();

    //包含${xx.xx}、#{xx.xx}、##{xx.xx}所有参数名
    private final List<NativeSqlParameter> allNamedParameters = new ArrayList<>();

    //非动态sql的NativeParserNode对象缓存
    private final ConcurrentHashMap<String, NativeParserNode> parserNodes = new ConcurrentHashMap();

    public NativeParserInfo(final String rawSql) {
        this.rawSql = rawSql;
        Set<String> rootParams = parseSql();
        if (dollarNames.isEmpty()) {
            StringBuilder ss = new StringBuilder();
            for (NativeSqlFragment fragment : fragments) {
                ss.append(fragment.getText());
            }
            this.templetSql = ss.toString();
        } else {
            this.templetSql = null;
        }
        this.allNamedParameters.addAll(dollarNames.values());
        this.allNamedParameters.addAll(numsignJdbcNames.values());
        this.rootParamNames.addAll(rootParams);
        this.sqlMode = getSqlMode(Utility.orElse(this.templetSql, this.rawSql), this.rawSql);
    }

    /**
     * 解析sql，将sql中的${xx.xx}, #{xx}转化成 :arg :xxx形式
     *
     * @return rootParams
     */
    private Set<String> parseSql() {
        boolean paraming = false;
        StringBuilder sb = new StringBuilder();
        final char[] chars = Utility.charArray(rawSql);
        char last = 0;
        Set<String> rootParams = new LinkedHashSet<>();
        int type = 0; //1:${xx.xx}, 2:#{xx.xx}, 3:##{xx.xx}
        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];
            if (ch == '{') {
                if (paraming || i < 2) {
                    throw new SourceException("Parse error, sql: " + rawSql);
                }
                if (last == '$') {
                    fragments.add(new NativeSqlFragment(false, sb.substring(0, sb.length() - 1)));
                    sb.delete(0, sb.length());
                    type = 1;
                    paraming = true;
                } else if (last == '#') {
                    type = chars[i - 2] == '#' ? 3 : 2;
                    fragments.add(new NativeSqlFragment(false, sb.substring(0, sb.length() + 1 - type)));
                    sb.delete(0, sb.length());
                    paraming = true;
                } else if (last == '\\') {
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append(ch);
                } else {
                    sb.append(ch);
                }
            } else if (ch == '}' && last == '\\') {
                sb.deleteCharAt(sb.length() - 1);
                sb.append(ch);
            } else if (ch == '}') {
                if (!paraming) {
                    throw new SourceException("Parse error, sql: " + rawSql);
                }
                String name = sb.toString().trim();
                sb.delete(0, sb.length());
                if (type == 1) { //${xx.xx}
                    dollarNames.put(name, new NativeSqlParameter(name, name, true));
                    fragments.add(new NativeSqlFragment(true, name));
                } else if (type >= 2) { //#{xx.xx}、##{xx.xx}
                    NativeSqlParameter old = numsignJdbcNames.get(name);
                    String jdbc = old == null ? null : old.getJdbcName();
                    if (jdbc == null) {
                        int seqno = numsignJdbcNames.size() + 1;
                        jdbc = "arg" + (seqno >= 10 ? seqno : ("0" + seqno));
                        NativeSqlParameter p = new NativeSqlParameter(name, jdbc, type == 3);
                        numsignJdbcNames.put(name, p);
                        jdbcToNumsignMap.put(jdbc, name);
                        if (p.isRequired()) {
                            requiredNumsignNames.put(name, p);
                        }
                    }
                    fragments.add(new NativeSqlFragment(false, ":" + jdbc));
                }
                paraming = false;
                int p1 = name.indexOf('.');
                int p2 = name.indexOf('[');
                if (p1 < 0 && p2 < 0) {
                    rootParams.add(name);
                } else {
                    int p = p1 > 0 ? (p2 > 0 ? Math.min(p1, p2) : p1) : p2;
                    rootParams.add(name.substring(0, p));
                }
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
        return rootParams;
    }

    public NativeSqlTemplet createTemplet(Map<String, Object> params) {
        Map<String, Object> newParams = params == null ? new HashMap<>() : new HashMap<>(params);
        for (NativeSqlParameter p : allNamedParameters) {
            Object val = p.getParamValue(params);
            if (p.isRequired() && val == null) {
                throw new SourceException("Missing parameter " + p.getNumsignName());
            }
            if (val != null) {
                newParams.put(p.getNumsignName(), val);
                newParams.put(p.getJdbcName(), val);
            }
        }
        if (templetSql == null) { //需要根据${xx.xx}参数动态构建sql
            StringBuilder sb = new StringBuilder();
            for (NativeSqlFragment fragment : fragments) {
                if (fragment.isDollarable()) {
                    sb.append(newParams.get(fragment.getText())); //不能用JsonConvert，比如 FROM user_${uid}
                } else {
                    sb.append(fragment.getText());
                }
            }
            return new NativeSqlTemplet(sb.toString(), newParams);
        } else {
            return new NativeSqlTemplet(templetSql, newParams);
        }
    }

    public NativeParserNode loadParserNode(IntFunction<String> signFunc, String dbtype, final String nativeSql, boolean countable) {
        if (isDynamic()) {
            return createParserNode(signFunc, nativeSql, countable);
        }
        return parserNodes.computeIfAbsent(nativeSql, sql -> createParserNode(signFunc, nativeSql, countable));
    }

    private NativeParserNode createParserNode(IntFunction<String> signFunc, final String nativeSql, boolean countable) {
        try {
            CCJSqlParser sqlParser = new CCJSqlParser(nativeSql).withAllowComplexParsing(true);
            Statement originStmt = sqlParser.Statement();
            final Set<String> fullNames = new HashSet<>();
            final Set<String> requiredNamedSet = new HashSet<>();
            //包含IN参数的sql必须走动态拼接sql模式
            final AtomicBoolean containsInExpr = new AtomicBoolean();
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
                    //rightExpression maybe JdbcNamedParameter/ParenthesedExpressionList/ParenthesedSelect
                    if (fullNames.size() > size && !(expr.getRightExpression() instanceof Select)) {
                        containsInExpr.set(true);
                    }
                }

                @Override
                public void visit(JdbcParameter jdbcParameter) {
                    throw new SourceException("Cannot contains ? JdbcParameter");
                }
            };

            PlainSelect countStmt = null;
            Expression fullWhere = null;
            Runnable clearWhere = null;
            List<UpdateSet> updateSets = null;
            List<Select> insertSets = null;
            if (originStmt instanceof Select) {
                if (!(originStmt instanceof PlainSelect)) {
                    throw new SourceException("Not support sql (" + rawSql + "), type: " + originStmt.getClass().getName());
                }
                PlainSelect selectBody = (PlainSelect) originStmt;
                fullWhere = selectBody.getWhere();
                clearWhere = () -> selectBody.setWhere(null);
                //创建COUNT总数sql
                CCJSqlParser countParser = new CCJSqlParser(nativeSql).withAllowComplexParsing(true);
                PlainSelect countBody = (PlainSelect) countParser.Statement();
                if (countBody.getDistinct() == null) {
                    Expression countFunc = new net.sf.jsqlparser.expression.Function().withName("COUNT")
                        .withParameters(new ExpressionList(new LongValue(1)));
                    countBody.setSelectItems(Utility.ofList(new SelectItem(countFunc)));
                } else {
                    Expression countFunc = new net.sf.jsqlparser.expression.Function().withName("COUNT").withDistinct(true)
                        .withParameters(new ParenthesedExpressionList((List) countBody.getSelectItems()));
                    countBody.setSelectItems(Utility.ofList(new SelectItem(countFunc)));
                    countBody.setDistinct(null);
                }
                countBody.setWhere(null);
                countBody.setOrderByElements(null);
                countStmt = countBody;
            } else if (originStmt instanceof Insert) {
                Insert insert = (Insert) originStmt;
                Select selectBody = insert.getSelect();
                if (selectBody instanceof PlainSelect) {
                    fullWhere = ((PlainSelect) selectBody).getWhere();
                    clearWhere = () -> ((PlainSelect) selectBody).setWhere(null);
                } else if (selectBody instanceof SetOperationList) {
                    insertSets = ((SetOperationList) selectBody).getSelects();
                } else if (selectBody instanceof Values) {
                    //do nothing
                } else {
                    throw new SourceException("Not support sql (" + rawSql + "), type: " + selectBody.getClass().getName());
                }
            } else if (originStmt instanceof Delete) {
                Delete delete = (Delete) originStmt;
                fullWhere = delete.getWhere();
                clearWhere = () -> delete.setWhere(null);
            } else if (originStmt instanceof Update) {
                Update update = (Update) originStmt;
                updateSets = update.getUpdateSets();
                fullWhere = update.getWhere();
                clearWhere = () -> update.setWhere(null);
            } else if (originStmt instanceof Upsert) {
                Upsert upsert = (Upsert) originStmt;
                updateSets = upsert.getUpdateSets();
                PlainSelect select = upsert.getPlainSelect();
                if (select != null) {
                    fullWhere = select.getWhere();
                    clearWhere = () -> select.setWhere(null);
                }
            } else if (originStmt instanceof Truncate) {
                //do nothing
            } else if (originStmt instanceof Alter) {
                //do nothing
            } else if (originStmt instanceof Grant) {
                //do nothing
            } else if (originStmt instanceof Drop) {
                //do nothing
            } else if (originStmt instanceof Execute) {
                //do nothing
            } else if (originStmt instanceof Merge) {
                //do nothing
            } else {
                throw new SourceException("Not support sql (" + rawSql + "), type: " + originStmt.getClass().getName());
            }

            SelectDeParser selectAdapter = new SelectDeParser();
            selectAdapter.setExpressionVisitor(exprAdapter);
            exprAdapter.setSelectVisitor(selectAdapter);
            if (fullWhere != null) {
                fullWhere.accept(exprAdapter);
                requiredNamedSet.clear(); //where不存在必需的参数名
            }
            if (insertSets != null) {
                for (Select body : insertSets) {
                    body.accept(selectAdapter);
                }
            }
            if (updateSets != null) {
                for (UpdateSet set : updateSets) {
                    for (Expression expr : set.getColumns()) {
                        expr.accept(exprAdapter);
                    }
                    for (Expression expr : set.getValues()) {
                        expr.accept(exprAdapter);
                    }
                }
            }
            if (clearWhere != null) {
                clearWhere.run(); //必须清空where条件
            }

            String updateNoWhereSql = null;
            List<String> updateNamedSet = new ArrayList<>();
            if (updateSets != null) { //UPDATE语句
                Map<String, Object> params = new HashMap<>();
                Object val = List.of(1); //虚构一个参数值，IN需要Collection
                for (String name : requiredNamedSet) {
                    params.put(name, val);
                }
                final NativeExprDeParser exprDeParser = new NativeExprDeParser(signFunc, params);
                UpdateDeParser deParser = new UpdateDeParser(exprDeParser, exprDeParser.getBuffer());
                deParser.deParse((Update) originStmt);
                updateNoWhereSql = exprDeParser.getBuffer().toString();
                updateNamedSet = exprDeParser.getJdbcNames();
            }
            return new NativeParserNode(originStmt, countStmt, fullWhere, jdbcToNumsignMap,
                fullNames, requiredNamedSet, isDynamic() || containsInExpr.get(), updateNoWhereSql, updateNamedSet);
        } catch (ParseException e) {
            throw new SourceException("Parse error, sql: " + nativeSql, e);
        }
    }

    private SqlMode getSqlMode(String parserSql, String rawSql) {
        try {
            CCJSqlParser sqlParser = new CCJSqlParser(parserSql).withAllowComplexParsing(true);
            Statement stmt = sqlParser.SingleStatement();
            if (stmt instanceof Select) {
                return SqlMode.SELECT;
            } else if (stmt instanceof Insert) {
                return SqlMode.INSERT;
            } else if (stmt instanceof Delete) {
                return SqlMode.DELETE;
            } else if (stmt instanceof Update) {
                return SqlMode.UPDATE;
            } else {
                return SqlMode.OTHERS;
            }
        } catch (ParseException e) {
            String upperSql = rawSql.trim().toUpperCase();
            SqlMode mode = SqlMode.OTHERS;
            if (upperSql.startsWith("SELECT")) {
                mode = SqlMode.SELECT;
            } else if (upperSql.startsWith("INSERT")) {
                mode = SqlMode.INSERT;
            } else if (upperSql.startsWith("UPDATE")) {
                mode = SqlMode.UPDATE;
            } else if (upperSql.startsWith("DELETE")) {
                mode = SqlMode.DELETE;
            }
            return mode;
        }
    }

    @Override
    public String toString() {
        return NativeParserInfo.class.getSimpleName() + "{"
            + "rawSql: \"" + rawSql + "\""
            + ", templetSql: \"" + templetSql + "\""
            + ", dollarNames: " + dollarNames
            + ", numsignJdbcNames: " + numsignJdbcNames
            + ", requiredNumsignNames: " + requiredNumsignNames
            + ", jdbcToNumsignMap: " + jdbcToNumsignMap
            + "}";
    }

}
