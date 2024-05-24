/*
 *
 */
package org.redkalex.source.parser;

import java.lang.reflect.Array;
import java.math.*;
import java.util.*;
import java.util.function.IntFunction;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.deparser.*;
import org.redkale.source.SourceException;

/**
 *
 * @author zhangjx
 */
public class NativeExprDeParser extends ExpressionDeParser {

    private static final Package relationalPkg = Between.class.getPackage();

    private static final Package conditionalPkg = AndExpression.class.getPackage();

    //只存AndExpression、OrExpression、XorExpression
    private final Deque<BinaryExpression> conditions = new ArrayDeque<>();

    private final Deque<Expression> relations = new ArrayDeque<>();

    private final IntFunction<String> signFunc;

    //需要预编译的jdbc参数名:argxxx, 数量与sql中的?数量一致
    private List<String> jdbcNames = new ArrayList<>();

    //参数
    private Map<String, Object> paramValues;

    //当前BinaryExpression缺失参数
    private boolean paramLosing;

    public NativeExprDeParser(IntFunction<String> signFunc, Map<String, Object> params) {
        Objects.requireNonNull(signFunc);
        Objects.requireNonNull(params);
        this.signFunc = signFunc;
        this.paramValues = params;
        setSelectVisitor(new CustomSelectDeParser(this, buffer));
    }

    public String deParseSql(Statement stmt) {
        CustomStatementDeParser deParser = new CustomStatementDeParser(this, (SelectDeParser) getSelectVisitor(), buffer);
        stmt.accept(deParser);
        return buffer.toString();
    }

    public NativeExprDeParser reset() {
        conditions.clear();
        relations.clear();
        jdbcNames.clear();
        paramLosing = false;
        buffer.delete(0, buffer.length());
        return this;
    }

    public List<String> getJdbcNames() {
        return jdbcNames;
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
        jdbcNames.add(expr.getName());
        //使用JdbcParameter代替JdbcNamedParameter
        buffer.append(signFunc.apply(jdbcNames.size()));
    }

    @Override
    public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expr, String operator) {
        if (expr.getClass().getPackage() != relationalPkg) {
            throw new SourceException("Not support expression (" + expr + ") ");
        }
        paramLosing = false;
        relations.push(expr);

        int size1 = jdbcNames.size();
        final int start1 = buffer.length();
        expr.getLeftExpression().accept(this);
        int size2 = jdbcNames.size();
        if (!paramLosing) {
            if (expr.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_RIGHT) {
                buffer.append("(+)");
            }
        }
        if (!paramLosing) {
            buffer.append(operator);
        } else {
            trimJdbcNames(size1, size2);
        }

        size1 = jdbcNames.size();
        expr.getRightExpression().accept(this);
        int end2 = buffer.length();
        size2 = jdbcNames.size();
        if (paramLosing) { //没有right
            buffer.delete(start1, end2);
            //多个paramNames里中一个不存在，需要删除另外几个
            trimJdbcNames(size1, size2);
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

            int size1 = jdbcNames.size();
            final int start1 = buffer.length();
            expr.getLeftExpression().accept(this);
            final int end1 = buffer.length();
            int size2 = jdbcNames.size();
            if (end1 > start1) {
                buffer.append(operator);
            } else {
                trimJdbcNames(size1, size2);
            }

            size1 = jdbcNames.size();
            final int start2 = buffer.length();
            expr.getRightExpression().accept(this);
            final int end2 = buffer.length();
            size2 = jdbcNames.size();
            if (end2 == start2) { //没有right
                buffer.delete(end1, end2);
                trimJdbcNames(size1, size2);
            }

            conditions.pop();
            paramLosing = false;
        } else if (expr.getClass().getPackage() == relationalPkg) {
            paramLosing = false;
            relations.push(expr);

            int size1 = jdbcNames.size();
            final int start1 = buffer.length();
            expr.getLeftExpression().accept(this);
            if (!paramLosing) {
                buffer.append(operator);

                expr.getRightExpression().accept(this);
                final int end1 = buffer.length();
                if (paramLosing) { //没有right
                    buffer.delete(start1, end1);
                    trimJdbcNames(size1, jdbcNames.size());
                }
            } else {
                trimJdbcNames(size1, jdbcNames.size());
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

        final int size = jdbcNames.size();
        final int start = buffer.length();
        expr.getLeftExpression().accept(this);
        if (!paramLosing) {
            if (expr.isNot()) {
                buffer.append(" NOT");
            }
            buffer.append(" BETWEEN ");
            expr.getBetweenExpressionStart().accept(this);
            int end = buffer.length();
            if (!paramLosing) {
                buffer.append(" AND ");
                expr.getBetweenExpressionEnd().accept(this);
                end = buffer.length();
                if (paramLosing) {
                    buffer.delete(start, end);
                    trimJdbcNames(size, jdbcNames.size());
                }
            } else {
                buffer.delete(start, end);
                trimJdbcNames(size, jdbcNames.size());
            }
        }

        relations.pop();
        paramLosing = false;
    }

    @Override
    public void visit(InExpression expr) {
        paramLosing = false;
        relations.push(expr);
        final int size1 = jdbcNames.size();
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
            Expression rightExpr = expr.getRightExpression();
            if (rightExpr instanceof Select) {
                rightExpr.accept(this);
            } else if (rightExpr instanceof ExpressionList) {
                List<Expression> newList = new ArrayList<>((ExpressionList) rightExpr);
                for (int i = newList.size() - 1; i >= 0; i--) {
                    Expression item = newList.get(i);
                    if (item instanceof JdbcNamedParameter) {
                        List<Expression> es = createInParamItemList((JdbcNamedParameter) item);
                        newList.remove(i);
                        if (es != null) {
                            newList.addAll(i, es);
                        }
                    }
                }
                new ParenthesedExpressionList(newList).accept(this);
            } else if (rightExpr instanceof JdbcNamedParameter) {
                List<Expression> itemList = createInParamItemList((JdbcNamedParameter) rightExpr);
                if (itemList == null) {
                    buffer.delete(start, end);
                    buffer.append(expr.isNot() ? "1=1" : "1=2");
                } else {
                    new ParenthesedExpressionList(itemList).accept(this);
                }
            } else {
                throw new SourceException("Not support expression (" + rightExpr
                    + "), type: " + (rightExpr == null ? null : rightExpr.getClass().getName()));
            }
        } else {
            buffer.delete(start, end);
            trimJdbcNames(size1, jdbcNames.size());
        }
        relations.pop();
        paramLosing = false;
    }

    private void trimJdbcNames(int size1, int size2) {
        for (int i = size1; i < size2; i++) {
            jdbcNames.remove(jdbcNames.size() - 1);
        }
    }

    private List<Expression> createInParamItemList(JdbcNamedParameter namedParam) {
        String name = namedParam.getName();
        Object val = paramValues.get(name);
        if (val == null) { //没有参数值            
            throw new SourceException("Not found parameter (name=" + name + ") ");
        }
        if (val instanceof Collection) {
            if (((Collection) val).isEmpty()) {
                //throw new SourceException("Parameter (name=" + name + ") is empty");
                return null;
            }
        } else if (val.getClass().isArray()) {
            int len = Array.getLength(val);
            if (len < 1) {
                //throw new SourceException("Parameter (name=" + name + ") is empty");
                return null;
            }
            Collection list = new ArrayList();
            for (int i = 0; i < len; i++) {
                list.add(Array.get(val, i));
            }
            val = list;
        } else {
            throw new SourceException("Parameter (name=" + name + ") is not Collection or Array, value = " + val);
        }
        List<Expression> itemList = new ArrayList();
        for (Object item : (Collection) val) {
            if (item == null) {
                itemList.add(new NullValue());
            } else if (item instanceof String) {
                itemList.add(new StringValue(item.toString().replace("'", "\\'")));
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
        return itemList;
    }

    @Override
    public void visit(FullTextSearch expr) {
        //@TODO 
        paramLosing = false;
        relations.push(expr);
        super.visit(expr);
        relations.pop();
        paramLosing = false;
    }

    @Override
    public void visit(LikeExpression expr) {
        final int start = buffer.length();
        visitBinaryExpression(expr, (expr.isNot() ? " NOT" : "") + (expr.isCaseInsensitive() ? " ILIKE " : " LIKE "));
        final int end = buffer.length();
        Expression escape = expr.getEscape();
        if (end > start && escape != null) {
            buffer.append(" ESCAPE ");
            expr.getEscape().accept(this);
        }
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
