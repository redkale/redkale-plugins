/*
 *
 */
package org.redkalex.source.parser;

import java.lang.reflect.Array;
import java.math.*;
import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.*;
import org.redkale.source.*;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class DataExpressionDeParser extends ExpressionDeParser {

    private static final Package relationalPkg = Between.class.getPackage();

    private static final Package conditionalPkg = AndExpression.class.getPackage();

    //只存AndExpression、OrExpression、XorExpression
    private final Deque<BinaryExpression> conditions = new ArrayDeque<>();

    private final Deque<Expression> relations = new ArrayDeque<>();

    //需要预编译的参数名, 数量与sql中的?数量一致
    protected List<String> paramNames = new ArrayList<>();

    protected java.util.function.Function<Integer, String> signFunc;

    //参数
    protected Map<String, Object> paramValues;

    //当前BinaryExpression缺失参数
    protected boolean paramLosing;

    public static void main(String[] args) throws Throwable {
        final java.util.function.Function<Integer, String> signFunc = index -> "?";
        {
            String sql = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table T "
                + "WHERE col1 = 10 AND (col2 = :c2 OR col3 = MAX(:c3)) AND name LIKE '%'"
                + " AND seqid IS NULL AND (gameid IN :gameids OR gameName IN ('%', 'zzz'))"
                + " AND time BETWEEN :min AND :range_max AND col2 >= :c2"
                + " AND id IN (SELECT id FROM table2 WHERE name LIKE :name AND time > 1)";
            Map<String, Object> params = Utility.ofMap("min2", 1, "c2", 3, "range_max", 100, "gameids", List.of(2, 3));
            final DataExpressionDeParser exprDeParser = new DataExpressionDeParser(index -> "?", params);
            final SelectDeParser selectParser = new SelectDeParser(exprDeParser, exprDeParser.getBuffer());
            exprDeParser.setSelectVisitor(selectParser);
            CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
            Select stmt = (Select) sqlParser.Statement();
            PlainSelect selectBody = (PlainSelect) stmt.getSelectBody();
            //System.out.println(stmt.toString());

            System.out.println(selectBody.getWhere());
            System.out.println(exprDeParser.deParser(selectBody.getWhere()));
            System.out.println("应该是有两个： [c2, c2]");
            System.out.println("paramNames = " + exprDeParser.getParamNames());

            DataNativeJsqlParser parser = new DataNativeJsqlParser();
            DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, sql, params);
            System.out.println("新sql = " + statement.getNativeSql());
            System.out.println("paramNames = " + statement.getParamNames());
        }
        {
            String sql = "SELECT 1";
            Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

            DataNativeJsqlParser parser = new DataNativeJsqlParser();
            DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, sql, params);
            System.out.println("新sql = " + statement.getNativeSql());
            System.out.println("paramNames = " + statement.getParamNames());
        }
        {
            String sql = "INSERT INTO dayrecord (recordid, content, createTime) VALUES (1, 2, 3)";
            Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

            DataNativeJsqlParser parser = new DataNativeJsqlParser();
            DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, sql, params);
            System.out.println("新sql = " + statement.getNativeSql());
            System.out.println("paramNames = " + statement.getParamNames());
        }
        {
            String sql = "INSERT INTO dayrecord (recordid, content, createTime) SELECT recordid, content, NOW() FROM hourrecord WHERE createTime BETWEEN :startTime AND :endTime AND id > 0";
            Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

            DataNativeJsqlParser parser = new DataNativeJsqlParser();
            DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, sql, params);
            System.out.println("新sql = " + statement.getNativeSql());
            System.out.println("paramNames = " + statement.getParamNames());
        }
        {
            String sql = "UPDATE dayrecord SET id = MAX(:id), remark = :remark, name = CASE WHEN type = 1 THEN :v1 WHEN type = 2 THEN :v2 ELSE :v3 END WHERE createTime BETWEEN :startTime AND :endTime AND id IN :ids";
            Map<String, Object> params = Utility.ofMap("startTime", 1, "ids", List.of(2, 3));
            
            CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
            System.out.println(sqlParser.Statement());

            DataNativeJsqlParser parser = new DataNativeJsqlParser();
            DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, sql, params);
            System.out.println("新sql = " + statement.getNativeSql());
            System.out.println("paramNames = " + statement.getParamNames());
        }
    }

    public DataExpressionDeParser(java.util.function.Function<Integer, String> signFunc, Map<String, Object> params) {
        Objects.requireNonNull(signFunc);
        Objects.requireNonNull(params);
        this.signFunc = signFunc;
        this.paramValues = params;
    }

    public String deParser(Expression where) {
        if (where != null) {
            where.accept(this);
        }
        return this.buffer.toString();
    }

    public List<String> getParamNames() {
        return paramNames;
    }

    public Map<String, Object> getParamValues() {
        return paramValues;
    }

    @Override
    public void visit(JdbcNamedParameter expr) {
        Object val = paramValues.get(expr.getName());
        if (val == null) { //没有参数值            
            paramLosing = true;
            return;
        }
        paramNames.add(expr.getName());
        //使用JdbcParameter代替JdbcNamedParameter
        buffer.append(signFunc.apply(paramNames.size()));
    }

    @Override
    public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expr, String operator) {
        if (expr.getClass().getPackage() != relationalPkg) {
            throw new SourceException("Not support expression (" + expr + ") ");
        }
        paramLosing = false;
        relations.push(expr);

        int size1 = paramNames.size();
        final int start1 = buffer.length();
        expr.getLeftExpression().accept(this);
        int end1 = buffer.length();
        int size2 = paramNames.size();
        if (!paramLosing) {
            if (expr.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_RIGHT) {
                buffer.append("(+)");
            }
            end1 = buffer.length();
        }
        if (!paramLosing) {
            buffer.append(operator);
        } else {
            for (int i = size1; i < size2; i++) {
                paramNames.remove(paramNames.size() - 1);
            }
        }

        size1 = paramNames.size();
        expr.getRightExpression().accept(this);
        int end2 = buffer.length();
        size2 = paramNames.size();
        if (paramLosing) { //没有right
            buffer.delete(start1, end2);
            //多个paramNames里中一个不存在，需要删除另外几个
            for (int i = size1; i < size2; i++) {
                paramNames.remove(paramNames.size() - 1);
            }
        } else {
            if (expr.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_LEFT) {
                buffer.append("(+)");
            }
        }

        relations.pop();
        paramLosing = false;
    }

    @Override
    protected void visitBinaryExpression(BinaryExpression expr, String operator) {
        if (expr.getClass().getPackage() == conditionalPkg) {
            paramLosing = false;
            conditions.push(expr);

            int size1 = paramNames.size();
            final int start1 = buffer.length();
            expr.getLeftExpression().accept(this);
            final int end1 = buffer.length();
            int size2 = paramNames.size();
            if (end1 > start1) {
                buffer.append(operator);
            } else {
                for (int i = size1; i < size2; i++) {
                    paramNames.remove(paramNames.size() - 1);
                }
            }

            size1 = paramNames.size();
            final int start2 = buffer.length();
            expr.getRightExpression().accept(this);
            final int end2 = buffer.length();
            size2 = paramNames.size();
            if (end2 == start2) { //没有right
                buffer.delete(end1, end2);
                for (int i = size1; i < size2; i++) {
                    paramNames.remove(paramNames.size() - 1);
                }
            }

            conditions.pop();
            paramLosing = false;
        } else if (expr.getClass().getPackage() == relationalPkg) {
            paramLosing = false;
            relations.push(expr);

            int size1 = paramNames.size();
            final int start1 = buffer.length();
            expr.getLeftExpression().accept(this);
            if (!paramLosing) {
                buffer.append(operator);

                expr.getRightExpression().accept(this);
                final int end1 = buffer.length();
                if (paramLosing) { //没有right
                    buffer.delete(start1, end1);
                    int size2 = paramNames.size();
                    for (int i = size1; i < size2; i++) {
                        paramNames.remove(paramNames.size() - 1);
                    }
                }
            } else {
                int size2 = paramNames.size();
                for (int i = size1; i < size2; i++) {
                    paramNames.remove(paramNames.size() - 1);
                }
            }

            relations.pop();
            paramLosing = false;
        } else {
            throw new SourceException("Not support expression (" + expr + ") ");
        }

    }

    @Override
    public void visit(AndExpression expr) {
        visitBinaryExpression(expr, expr.isUseOperator() ? " && " : " AND ");
    }

    @Override
    public void visit(OrExpression expr) {
        visitBinaryExpression(expr, " OR ");
    }

    @Override
    public void visit(XorExpression expr) {
        visitBinaryExpression(expr, " XOR ");
    }

    @Override
    public void visit(Parenthesis parenthesis) {
        buffer.append("(");
        int start = buffer.length();
        parenthesis.getExpression().accept(this);
        int end = buffer.length();
        if (end > start) {
            buffer.append(")");
        } else {
            buffer.delete(start - 1, end);
        }
    }

    //--------------------------------------------------
    @Override
    public void visit(Between expr) {
        paramLosing = false;
        relations.push(expr);

        final int size = paramNames.size();
        final int start = buffer.length();
        expr.getLeftExpression().accept(this);
        int end = buffer.length();
        if (!paramLosing) {
            if (expr.isNot()) {
                buffer.append(" NOT");
            }
            buffer.append(" BETWEEN ");
            int start2 = buffer.length();
            expr.getBetweenExpressionStart().accept(this);
            int end2 = buffer.length();
            if (!paramLosing) {
                buffer.append(" AND ");
                start2 = buffer.length();
                expr.getBetweenExpressionEnd().accept(this);
                end2 = buffer.length();
                if (paramLosing) {
                    buffer.delete(start, end2);
                    final int size2 = paramNames.size();
                    for (int i = size; i < size2; i++) {
                        paramNames.remove(paramNames.size() - 1);
                    }
                }
            } else {
                buffer.delete(start, end2);
                final int size2 = paramNames.size();
                for (int i = size; i < size2; i++) {
                    paramNames.remove(paramNames.size() - 1);
                }
            }
        }

        relations.pop();
        paramLosing = false;
    }

    @Override
    public void visit(InExpression expr) {
        paramLosing = false;
        relations.push(expr);
        final int size1 = paramNames.size();
        final int start = buffer.length();
        expr.getLeftExpression().accept(this);
        int end = buffer.length();
        if (!paramLosing) {
            if (expr.getOldOracleJoinSyntax() == SupportsOldOracleJoinSyntax.ORACLE_JOIN_RIGHT) {
                buffer.append("(+)");
            }
            if (expr.isNot()) {
                buffer.append(" NOT");
            }
            buffer.append(" IN ");
            if (expr.getRightExpression() != null) {
                if (expr.getRightExpression() instanceof SubSelect) {
                    expr.getRightExpression().accept(this);
                } else if (expr.getRightExpression() instanceof JdbcNamedParameter) {
                    String name = ((JdbcNamedParameter) expr.getRightExpression()).getName();
                    Object val = paramValues.get(name);
                    if (val == null) { //没有参数值            
                        throw new SourceException("Not found parameter (name=" + name + ") ");
                    }
                    if (val instanceof Collection) {
                        if (((Collection) val).isEmpty()) {
                            throw new SourceException("Parameter (name=" + name + ") is empty");
                        }
                    } else if (val.getClass().isArray()) {
                        int len = Array.getLength(val);
                        if (len < 1) {
                            throw new SourceException("Parameter (name=" + name + ") is empty");
                        }
                        Collection list = new ArrayList();
                        for (int i = 0; i < len; i++) {
                            list.add(Array.get(val, i));
                        }
                        val = list;
                    } else {
                        throw new SourceException("Parameter (name=" + name + ") is not Collection or Array");
                    }
                    List<Expression> itemList = new ArrayList();
                    for (Object item : (Collection) val) {
                        if (item == null) {
                            itemList.add(new NullValue());
                        } else if (item instanceof String) {
                            itemList.add(new StringValue(item.toString()));
                        } else if (item instanceof Short || item instanceof Integer || item instanceof Long) {
                            itemList.add(new LongValue(item.toString()));
                        } else if (item instanceof Float || item instanceof Double) {
                            itemList.add(new DoubleValue(item.toString()));
                        } else if (item instanceof BigInteger || item instanceof BigDecimal) {
                            itemList.add(new StringValue(item.toString()));
                        } else if (item instanceof java.sql.Date) {
                            itemList.add(new DateValue((java.sql.Date) item));
                        } else if (item instanceof java.sql.Time) {
                            itemList.add(new TimeValue(item.toString()));
                        } else {
                            throw new SourceException("Not support parameter: " + val);
                        }
                    }
                    new ExpressionList(itemList).accept(this);
                } else {
                    throw new SourceException("Not support expression (" + expr.getRightExpression() + ") ");
                }
            } else {
                expr.getRightItemsList().accept(this);
            }
        } else {
            buffer.delete(start, end);
            final int size2 = paramNames.size();
            for (int i = size1; i < size2; i++) {
                paramNames.remove(paramNames.size() - 1);
            }
        }
        relations.pop();
        paramLosing = false;
    }

    @Override
    public void visit(IsNullExpression expr) {
        paramLosing = false;
        relations.push(expr);
        super.visit(expr);
        relations.pop();
        paramLosing = false;
    }

    @Override
    public void visit(IsBooleanExpression expr) {
        paramLosing = false;
        relations.push(expr);
        super.visit(expr);
        relations.pop();
        paramLosing = false;
    }

    @Override
    public void visit(ExistsExpression expr) {
        paramLosing = false;
        relations.push(expr);
        super.visit(expr);
        relations.pop();
        paramLosing = false;
    }
}
