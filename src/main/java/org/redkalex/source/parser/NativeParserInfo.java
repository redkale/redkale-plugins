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
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;
import org.redkale.source.DataNativeSqlInfo;
import org.redkale.source.SourceException;
import org.redkale.util.*;

/**
 * jsqlparser只能识别:xxx的参数变量形式的sql，而DataNativeSqlParser定义的参数变量形式是: ${xxx}、#{xxx}、##{xxx}
 * 此类作用是将原始sql先转换成:name形式的sql再解析出变量参数 注意: 目前不支持union sql
 *
 * @author zhangjx
 */
@SuppressWarnings("unchecked")
public class NativeParserInfo extends DataNativeSqlInfo {

    // ${xxx}、#{xxx}参数生成jdbc参数函数
    private final IntFunction<String> signFunc;

    // db类型
    private final String dbType;

    // 所有参数名 arg01/xx
    final TreeSet<String> fullJdbcNames = new TreeSet<>();

    // ${xx.xx}的拼接参数名
    private final Map<String, NativeSqlParameter> dollarNames = new HashMap<>();

    // 必需的##{xx.xx}参数名
    // key: xx.xx
    private final Map<String, NativeSqlParameter> requiredNumsignNames = new HashMap<>();

    // jdbc参数名:argxxx对应#{xx.xx}参数名
    // key: xx_xx, value: xx.xx
    final Map<String, String> jdbcToNumsignMap = new HashMap<>();

    // #{xx.xx}参数名对应jdbc参数名:xx_xx, 包含了requiredNumsignNames ##{xx.xx}
    // key: xx.xx
    private final Map<String, NativeSqlParameter> numsignParameters = new HashMap<>();

    // 根据${xx.xx}分解并将xx.xx替换成:xx_xx的sql片段
    private final List<NativeSqlFragment> fragments = new ArrayList<>();

    // 包含${xx.xx}、#{xx.xx}、##{xx.xx}所有参数名
    private final List<NativeSqlParameter> allNamedParameters = new ArrayList<>();

    // 非动态sql的NativeParserNode对象缓存
    private final ConcurrentHashMap<String, NativeParserNode> parserNodes = new ConcurrentHashMap();

    public NativeParserInfo(String rawSql, String dbType, IntFunction<String> signFunc) {
        this.rawSql = rawSql;
        this.dbType = dbType;
        this.signFunc = signFunc;
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
        this.allNamedParameters.addAll(numsignParameters.values());
        this.rootParamNames.addAll(rootParams);
        parseInfo(Utility.orElse(this.templetSql, this.rawSql), this.rawSql);
    }

    public NativeSqlTemplet createTemplet(Map<String, Object> params) {
        Map<String, Object> newParams = params == null ? new HashMap<>() : new HashMap<>(params);
        for (NativeSqlParameter p : allNamedParameters) {
            Object val = p.getParamValue(params);
            if (p.isRequired() && val == null) {
                throw MissingParamException.of(p.getNumsignName());
            }
            if (val != null) {
                newParams.put(p.getNumsignName(), val);
                newParams.put(p.getJdbcName(), val);
            }
        }
        if (templetSql == null) { // 需要根据${xx.xx}参数动态构建sql
            StringBuilder sb = new StringBuilder();
            for (NativeSqlFragment fragment : fragments) {
                if (fragment.isDollarable()) {
                    sb.append(newParams.get(fragment.getText())); // 不能用JsonConvert，比如 FROM user_${uid}
                } else {
                    sb.append(fragment.getText());
                }
            }
            return new NativeSqlTemplet(sb.toString(), newParams);
        } else {
            return new NativeSqlTemplet(templetSql, newParams);
        }
    }

    public NativeParserNode loadParserNode(String jdbcSql, boolean countable) {
        if (isDynamic()) {
            return createParserNode(jdbcSql, countable);
        }
        return parserNodes.computeIfAbsent(jdbcSql, sql -> createParserNode(sql, countable));
    }

    public String getDbType() {
        return dbType;
    }

    public IntFunction<String> signFunc() {
        return signFunc;
    }

    protected NativeParserNode createParserNode(final String jdbcSql, boolean countable) {
        try {
            CCJSqlParser sqlParser = new CCJSqlParser(jdbcSql).withAllowComplexParsing(true);
            return new NativeParserNode(this, jdbcSql, countable, sqlParser.Statement());
        } catch (ParseException e) {
            throw new SourceException("Parse error, sql: " + jdbcSql, e);
        }
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
        int type = 0; // 1:${xx.xx}, 2:#{xx.xx}, 3:##{xx.xx}
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
                if (type == 1) { // ${xx.xx}
                    dollarNames.put(name, new NativeSqlParameter(name, name, true));
                    fragments.add(new NativeSqlFragment(true, name));
                } else if (type >= 2) { // #{xx.xx}、##{xx.xx}
                    NativeSqlParameter old = numsignParameters.get(name);
                    String jdbc = old == null ? null : old.getJdbcName();
                    if (jdbc == null) {
                        jdbc = formatNumsignToJdbcName(name);
                        NativeSqlParameter p = new NativeSqlParameter(name, jdbc, type == 3);
                        numsignParameters.put(name, p);
                        jdbcToNumsignMap.put(jdbc, name);
                        if (p.isRequired()) {
                            requiredNumsignNames.put(name, p);
                        }
                    } else if (!old.isRequired() && type == 3) { // 参数先非必需，后必需，需要更改required属性
                        old.required(true);
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

    private void parseInfo(String parserSql, String rawSql) {
        try {
            CCJSqlParser sqlParser = new CCJSqlParser(parserSql).withAllowComplexParsing(true);
            Statement stmt = sqlParser.SingleStatement();
            // 包含IN参数的sql必须走动态拼接sql模式
            final AtomicBoolean containsInExprFlag = new AtomicBoolean();
            // 参数解析器
            ExpressionDeParser exprDeParser = new ExpressionDeParser() {

                @Override
                public void visit(JdbcNamedParameter expr) {
                    super.visit(expr);
                    fullJdbcNames.add(expr.getName());
                }

                @Override
                public void visit(InExpression expr) {
                    int size = fullJdbcNames.size();
                    super.visit(expr);
                    // rightExpression maybe JdbcNamedParameter/ParenthesedExpressionList/ParenthesedSelect
                    if (fullJdbcNames.size() > size && !(expr.getRightExpression() instanceof Select)) {
                        containsInExprFlag.set(true);
                    }
                }

                @Override
                public void visit(JdbcParameter jdbcParameter) {
                    throw new SourceException("Cannot contains ? JdbcParameter");
                }
            };
            stmt.accept(new StatementDeParser(exprDeParser, new SelectDeParser(), new StringBuilder()));
            this.containsInExpr = containsInExprFlag.get();

            SqlMode mode = SqlMode.OTHERS;
            if (stmt instanceof Select) {
                mode = SqlMode.SELECT;
            } else if (stmt instanceof Insert) {
                mode = SqlMode.INSERT;
            } else if (stmt instanceof Delete) {
                mode = SqlMode.DELETE;
            } else if (stmt instanceof Update) {
                mode = SqlMode.UPDATE;
                final Set<String> updateNames = new HashSet<>();
                ExpressionDeParser updateDeParser = new ExpressionDeParser() {

                    @Override
                    public void visit(JdbcNamedParameter expr) {
                        super.visit(expr);
                        updateNames.add(expr.getName());
                    }
                };
                updateDeParser.setSelectVisitor(new SelectDeParser(updateDeParser, updateDeParser.getBuffer()));
                List<UpdateSet> updateSets = ((Update) stmt).getUpdateSets();
                if (updateSets != null) {
                    for (UpdateSet updateSet : updateSets) {
                        for (Expression expr : updateSet.getValues()) {
                            // 跳过SELECT子语句WHERE中的参数
                            // UPDATE dayrecord SET money = (SELECT SUM(m) FROM order WHERE flag = #{flag})
                            if (!(expr instanceof ParenthesedSelect)) {
                                expr.accept(updateDeParser);
                            }
                        }
                    }
                    updateNames.forEach(jdbcName -> {
                        for (NativeSqlParameter p : allNamedParameters) {
                            if (Objects.equals(p.getJdbcName(), jdbcName)) {
                                if (!p.isRequired()) { // UPDATE SET中的参数都是必需的
                                    numsignParameters.put(p.getNumsignName(), p.required(true));
                                }
                                return;
                            }
                        }
                    });
                }
            }
            this.sqlMode = mode;
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
            this.sqlMode = mode;
        }
    }

    private String formatNumsignToJdbcName(String name) {
        StringBuilder sb = new StringBuilder(name.length());
        for (char ch : name.toCharArray()) {
            if ((ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '_'
                    || ch == '$') {
                sb.append(ch);
            } else {
                sb.append('_');
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return NativeParserInfo.class.getSimpleName() + "{"
                + "rawSql: \"" + rawSql + "\""
                + ", templetSql: \"" + templetSql + "\""
                + ", dollarNames: " + dollarNames
                + ", numsignJdbcNames: " + numsignParameters
                + ", requiredNumsignNames: " + requiredNumsignNames
                + ", jdbcToNumsignMap: " + jdbcToNumsignMap
                + "}";
    }
}
