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

/** @author zhangjx */
public class NativeExprDeParser extends ExpressionDeParser {

    private static final Package relationalPkg = Between.class.getPackage();

    private static final Package conditionalPkg = AndExpression.class.getPackage();

    // 只存AndExpression、OrExpression、XorExpression
    private final Deque<BinaryExpression> conditions = new ArrayDeque<>();

    private final Deque<Expression> relations = new ArrayDeque<>();

    private final IntFunction<String> signFunc;

    // 需要预编译的jdbc参数名:argxxx, 数量与sql中的?数量一致
    private final List<String> jdbcNames = new ArrayList<>();

    // 参数
    private final Map<String, Object> paramValues;

    // 当前BinaryExpression缺失参数
    private boolean paramLosing;

    public NativeExprDeParser(IntFunction<String> signFunc, Map<String, Object> params) {
        Objects.requireNonNull(signFunc);
        Objects.requireNonNull(params);
        this.signFunc = signFunc;
        this.paramValues = params;
        setSelectVisitor(new CustomSelectDeParser(this, buffer));
    }

    public String deParseSql(Statement stmt) {
        SelectDeParser parser = (SelectDeParser) getSelectVisitor();
        CustomStatementDeParser deParser = new CustomStatementDeParser(this, parser, buffer);
        stmt.accept(deParser, null);
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
    protected <S> void deparse(BinaryExpression expr, String operator, S context) {
        deparse(expr, () -> buffer.append(operator), null, context);
    }

    private <S> void deparse(BinaryExpression expr, Runnable afterLeftRunner, Runnable afterRightRunner, S context) {
        if (expr.getClass().getPackage() == conditionalPkg) {
            paramLosing = false;
            conditions.push(expr);

            int size1 = jdbcNames.size();
            final int start1 = buffer.length();
            expr.getLeftExpression().accept(this, context);
            final int end1 = buffer.length();
            int size2 = jdbcNames.size();
            if (end1 > start1) { // 不能用!paramLosing
                if (afterLeftRunner != null) {
                    afterLeftRunner.run();
                }
            } else {
                trimJdbcNames(size1, size2);
            }

            size1 = jdbcNames.size();
            final int start2 = buffer.length();
            expr.getRightExpression().accept(this, context);
            final int end2 = buffer.length();
            size2 = jdbcNames.size();
            if (end2 == start2) { // 没有right
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
            expr.getLeftExpression().accept(this, context);
            if (!paramLosing) {
                if (afterLeftRunner != null) {
                    afterLeftRunner.run();
                }
                expr.getRightExpression().accept(this, context);
                final int end1 = buffer.length();
                if (paramLosing) { // 没有right
                    buffer.delete(start1, end1);
                    trimJdbcNames(size1, jdbcNames.size());
                } else if (afterRightRunner != null) {
                    afterRightRunner.run();
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
    public <S> StringBuilder visit(JdbcNamedParameter expr, S context) {
        Object val = paramValues.get(expr.getName());
        if (val == null) { // 没有参数值
            paramLosing = true;
            return buffer;
        }
        jdbcNames.add(expr.getName());
        // 使用JdbcParameter ? 代替JdbcNamedParameter xx.xx
        buffer.append(signFunc.apply(jdbcNames.size()));
        return buffer;
    }

    @Override
    public <S> StringBuilder deparse(OldOracleJoinBinaryExpression expr, String operator, S context) {
        if (expr.getClass().getPackage() != relationalPkg) {
            throw new SourceException("Not support expression (" + expr + ") ");
        }
        paramLosing = false;
        relations.push(expr);

        int size1 = jdbcNames.size();
        final int start1 = buffer.length();
        expr.getLeftExpression().accept(this, context);
        int size2 = jdbcNames.size();
        if (!paramLosing) {
            if (expr.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_RIGHT) {
                buffer.append("(+)");
            }
            buffer.append(operator);
        } else {
            trimJdbcNames(size1, size2);
        }

        size1 = jdbcNames.size();
        expr.getRightExpression().accept(this, context);
        int end2 = buffer.length();
        size2 = jdbcNames.size();
        if (paramLosing) { // 没有right
            buffer.delete(start1, end2);
            // 多个paramNames里中一个不存在，需要删除另外几个
            trimJdbcNames(size1, size2);
        } else {
            if (expr.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_LEFT) {
                buffer.append("(+)");
            }
        }
        relations.pop();
        paramLosing = false;
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(TranscodingFunction transcodingFunction, S context) {
        return super.visit(transcodingFunction, context);
    }

    @Override
    public <S> StringBuilder visit(TrimFunction trimFunction, S context) {
        return super.visit(trimFunction, context);
    }

    @Override
    public <S> StringBuilder visit(Function function, S context) {
        return super.visit(function, context);
    }

    @Override
    public <S> StringBuilder visit(RangeExpression expr, S context) {
        paramLosing = false;
        relations.push(expr);

        int size1 = jdbcNames.size();
        final int start1 = buffer.length();
        expr.getStartExpression().accept(this, context);
        if (!paramLosing) {
            buffer.append(":");
            expr.getEndExpression().accept(this, context);
            final int end1 = buffer.length();
            if (paramLosing) { // 没有right
                buffer.delete(start1, end1);
                trimJdbcNames(size1, jdbcNames.size());
            }
        } else {
            trimJdbcNames(size1, jdbcNames.size());
        }

        relations.pop();
        paramLosing = false;
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(ExpressionList<? extends Expression> expressionList, S context) {
        int start = buffer.length();
        super.visit(expressionList, context);
        int end = buffer.length();
        if (end == (start + 2) && buffer.charAt(start) == '(') {
            buffer.delete(start - 1, end);
        }
        return buffer;
    }

    // --------------------------------------------------
    @Override
    public <S> StringBuilder visit(Between expr, S context) {
        paramLosing = false;
        relations.push(expr);

        final int size = jdbcNames.size();
        final int start = buffer.length();
        expr.getLeftExpression().accept(this, context);
        if (!paramLosing) {
            if (expr.isNot()) {
                buffer.append(" NOT");
            }
            buffer.append(" BETWEEN ");
            expr.getBetweenExpressionStart().accept(this, context);
            int end = buffer.length();
            if (!paramLosing) {
                buffer.append(" AND ");
                expr.getBetweenExpressionEnd().accept(this, context);
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
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(InExpression expr, S context) {
        paramLosing = false;
        relations.push(expr);

        final int size1 = jdbcNames.size();
        final int start = buffer.length();
        expr.getLeftExpression().accept(this, context);
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
                rightExpr.accept(this, context);
            } else if (rightExpr instanceof ExpressionList) {
                List<Expression> newList = new ArrayList<>((ExpressionList) rightExpr);
                for (int i = newList.size() - 1; i >= 0; i--) {
                    Expression item = newList.get(i);
                    if (item instanceof JdbcNamedParameter) {
                        Object val = createInParamItemList(true, (JdbcNamedParameter) item);
                        if (val instanceof String) {
                            buffer.append(val);
                        } else {
                            List<Expression> es = (List<Expression>) val;
                            newList.remove(i);
                            if (es != null) {
                                newList.addAll(i, es);
                            }
                        }
                    }
                }
                new ParenthesedExpressionList(newList).accept(this, context);
            } else if (rightExpr instanceof JdbcNamedParameter) {
                Object val = createInParamItemList(false, (JdbcNamedParameter) rightExpr);
                if (val instanceof String) {
                    buffer.append(val);
                } else {
                    List<Expression> itemList = (List<Expression>) val;
                    if (itemList == null) {
                        buffer.delete(start, end);
                        buffer.append(expr.isNot() ? "1=1" : "1=2");
                    } else {
                        new ParenthesedExpressionList(itemList).accept(this, context);
                    }
                }
            } else {
                throw new SourceException("Not support expression (" + rightExpr + "), type: "
                        + (rightExpr == null ? null : rightExpr.getClass().getName()));
            }
        } else {
            buffer.delete(start, end);
            trimJdbcNames(size1, jdbcNames.size());
        }

        relations.pop();
        paramLosing = false;
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(LikeExpression expr, S context) {
        paramLosing = false;
        relations.push(expr);

        int size1 = jdbcNames.size();
        final int start1 = buffer.length();
        expr.getLeftExpression().accept(this, context);
        if (!paramLosing) {
            buffer.append(" ");
            if (expr.isNot()) {
                buffer.append("NOT ");
            }
            String keywordStr = expr.getLikeKeyWord() == LikeExpression.KeyWord.SIMILAR_TO
                    ? " SIMILAR TO"
                    : expr.getLikeKeyWord().toString();
            buffer.append(keywordStr).append(" ");
            if (expr.isUseBinary()) {
                buffer.append("BINARY ");
            }
            expr.getRightExpression().accept(this, context);
            final int end1 = buffer.length();
            if (paramLosing) { // 没有right
                buffer.delete(start1, end1);
                trimJdbcNames(size1, jdbcNames.size());
            } else {
                Expression escape = expr.getEscape();
                if (escape != null) {
                    buffer.append(" ESCAPE ");
                    expr.getEscape().accept(this);
                }
            }
        } else {
            trimJdbcNames(size1, jdbcNames.size());
        }

        relations.pop();
        paramLosing = false;
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(FullTextSearch expr, S context) {
        paramLosing = false;
        relations.push(expr);
        super.visit(expr, context);
        relations.pop();
        paramLosing = false;
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(IsNullExpression expr, S context) {
        paramLosing = false;
        relations.push(expr);
        super.visit(expr, context);
        relations.pop();
        paramLosing = false;
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(IsBooleanExpression expr, S context) {
        paramLosing = false;
        relations.push(expr);
        super.visit(expr, context);
        relations.pop();
        paramLosing = false;
        return buffer;
    }

    @Override
    public <S> StringBuilder visit(ExistsExpression expr, S context) {
        paramLosing = false;
        relations.push(expr);
        super.visit(expr, context);
        relations.pop();
        paramLosing = false;
        return buffer;
    }

    private void trimJdbcNames(int size1, int size2) {
        for (int i = size1; i < size2; i++) {
            jdbcNames.remove(jdbcNames.size() - 1);
        }
    }

    // 返回类型只能是List<Expression>、String
    private Object createInParamItemList(boolean subIn, JdbcNamedParameter namedParam) {
        String name = namedParam.getName();
        Object val = paramValues.get(name);
        if (val == null) { // 没有参数值
            throw new SourceException("Not found parameter (name=" + name + ") ");
        }
        if (val instanceof Collection) {
            if (((Collection) val).isEmpty()) {
                // throw new SourceException("Parameter (name=" + name + ") is empty");
                return null;
            }
        } else if (val.getClass().isArray()) {
            int len = Array.getLength(val);
            if (len < 1) {
                // throw new SourceException("Parameter (name=" + name + ") is empty");
                return null;
            }
            Collection list = new ArrayList();
            for (int i = 0; i < len; i++) {
                list.add(Array.get(val, i));
            }
            val = list;
        } else if (subIn) {
            return List.of(createItemExpression(val, name));
        } else if (val instanceof String) { // 默认完整参数值
            return val;
        } else {
            throw new SourceException("Parameter (name=" + name + ") is not Collection or Array, value = " + val);
        }
        List<Expression> itemList = new ArrayList();
        for (Object item : (Collection) val) {
            itemList.add(createItemExpression(item, name));
        }
        return itemList;
    }

    private Expression createItemExpression(Object item, String name) {
        if (item == null) {
            return new NullValue();
        } else if (item instanceof String) {
            return new StringValue(item.toString().replace("'", "\\'"));
        } else if (item instanceof Short || item instanceof Integer || item instanceof Long) {
            return new LongValue().withValue(((Number) item).longValue());
        } else if (item instanceof Float || item instanceof Double) {
            return new DoubleValue().withValue(((Number) item).doubleValue());
        } else if (item instanceof BigInteger || item instanceof BigDecimal) {
            return new StringValue(item.toString());
        } else if (item instanceof java.util.Date) {
            return new DateValue().withValue(new java.sql.Date(((java.util.Date) item).getTime()));
        } else if (item instanceof java.sql.Date) {
            return new DateValue().withValue((java.sql.Date) item);
        } else if (item instanceof java.sql.Time) {
            return new TimeValue().withValue((java.sql.Time) item);
        } else if (item instanceof java.sql.Timestamp) {
            return new TimestampValue().withValue((java.sql.Timestamp) item);
        } else if (item instanceof java.time.LocalDate) {
            return new DateValue().withValue(java.sql.Date.valueOf((java.time.LocalDate) item));
        } else if (item instanceof java.time.LocalTime) {
            return new TimeValue().withValue(java.sql.Time.valueOf((java.time.LocalTime) item));
        } else if (item instanceof java.time.LocalDateTime) {
            return new TimestampValue().withValue(java.sql.Timestamp.valueOf((java.time.LocalDateTime) item));
        } else {
            throw new SourceException("Not support parameter: " + name);
        }
    }
}
