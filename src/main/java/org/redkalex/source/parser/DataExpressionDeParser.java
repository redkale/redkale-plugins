/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.*;
import org.redkale.annotation.Nonnull;
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
    protected List<String> paramNames;

    //参数
    protected Map<String, Object> paramValues;

    public static void main(String[] args) throws Throwable {
        String sql = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table T WHERE col1 = 10 AND (col2 = :c2 OR col3 = MAX(:c3)) AND name LIKE '%' AND seqid IS NULL AND (gameid IN :gameids OR gameName IN ('%', 'zzz')) AND time BETWEEN :min AND :range_max AND id IN (SELECT id FROM table2 WHERE name LIKE :name AND time > 1)";
        Map<String, Object> params = Utility.ofMap("min2", 1, "c2", 3, "range_max", 100, "gameids", List.of(2, 3));
        final ExpressionDeParser exprDeParser = new DataExpressionDeParser();
        final SelectDeParser selectParser = new SelectDeParser(exprDeParser, exprDeParser.getBuffer());
        exprDeParser.setSelectVisitor(selectParser);
        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        Select stmt = (Select) sqlParser.Statement();
        PlainSelect selectBody = (PlainSelect) stmt.getSelectBody();
        selectBody.accept(selectParser);
        System.out.println(stmt.toString());
        System.out.println(selectParser.getBuffer());

        {
            StringBuilder buffer = new StringBuilder();
            int start1 = buffer.length();
            buffer.append("1=2");
            int end1 = buffer.length();
            if (end1 > start1) {
                buffer.append(" AND ");
            }
            int start2 = buffer.length();
            buffer.append("3=4");
            int end2 = buffer.length();
            if (start2 == end2) { //没有right
                buffer.delete(end1, end2);
            }
            System.out.println("buffer = " + buffer);
        }

    }

    public String deParser(Expression where, @Nonnull List<String> paramNames, @Nonnull Map<String, Object> params) {
        Objects.requireNonNull(paramNames);
        Objects.requireNonNull(params);
        this.buffer.delete(0, this.buffer.length());
        this.paramNames = paramNames;
        this.paramValues = params;
        if (where != null) {
            where.accept(this);
        }
        this.paramNames = null;
        this.paramValues = null;
        return this.buffer.toString();
    }

    @Override
    public void visit(JdbcNamedParameter expr) {
        Object val = paramValues.get(expr.getName());
        if (val == null) { //没有参数值
            return;
        }
        paramNames.add(expr.getName());
        //使用JdbcParameter代替JdbcNamedParameter
        buffer.append("?");
    }

    @Override
    public void visitOldOracleJoinBinaryExpression(OldOracleJoinBinaryExpression expr, String operator) {
        boolean relate = expr.getClass().getPackage() == relationalPkg;
        if (relate) {
            relations.push(expr);
        }

        int start1 = buffer.length();
        expr.getLeftExpression().accept(this);
        int end1 = buffer.length();
        if (end1 > start1) {
            if (expr.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_RIGHT) {
                buffer.append("(+)");
            }
            end1 = buffer.length();
        }
        if (end1 > start1) {
            buffer.append(operator);
        }
        int start2 = buffer.length();
        expr.getRightExpression().accept(this);
        int end2 = buffer.length();
        if (start2 == end2) { //没有right
            buffer.delete(end1, end2);
        } else {
            if (expr.getOldOracleJoinSyntax() == EqualsTo.ORACLE_JOIN_LEFT) {
                buffer.append("(+)");
            }
        }

        if (relate) {
            relations.pop();
        }
    }

    @Override
    protected void visitBinaryExpression(BinaryExpression expr, String operator) {
        boolean condit = expr.getClass().getPackage() == conditionalPkg;
        boolean relate = !condit && expr.getClass().getPackage() == relationalPkg;
        if (condit) {
            conditions.push(expr);
        } else if (relate) {
            relations.push(expr);
        }

        int start1 = buffer.length();
        expr.getLeftExpression().accept(this);
        int end1 = buffer.length();
        if (end1 > start1) {
            buffer.append(operator);
        }
        int start2 = buffer.length();
        expr.getRightExpression().accept(this);
        int end2 = buffer.length();
        if (start2 == end2) { //没有right
            buffer.delete(end1, end2);
        }

        if (condit) {
            conditions.pop();
        } else if (relate) {
            relations.pop();
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
        relations.push(expr);

        final int start = buffer.length();
        expr.getLeftExpression().accept(this);
        int end = buffer.length();
        if (end > start) {
            if (expr.isNot()) {
                buffer.append(" NOT");
            }
            buffer.append(" BETWEEN ");
            int start2 = buffer.length();
            expr.getBetweenExpressionStart().accept(this);
            int end2 = buffer.length();
            if (end2 > start2) {
                buffer.append(" AND ");
                start2 = buffer.length();
                expr.getBetweenExpressionEnd().accept(this);
                end2 = buffer.length();
                if (end2 == start2) {
                    if (expr.getBetweenExpressionStart() instanceof JdbcNamedParameter) {
                        paramNames.remove(paramNames.size() - 1);
                    }
                    buffer.delete(start, end2);
                }
            } else {
                buffer.delete(start, end2);
            }
        }

        relations.pop();
    }

    @Override
    public void visit(InExpression expr) {
        relations.push(expr);
        final int start = buffer.length();
        expr.getLeftExpression().accept(this);
        int end = buffer.length();
        if (end > start) {
            if (expr.getOldOracleJoinSyntax() == SupportsOldOracleJoinSyntax.ORACLE_JOIN_RIGHT) {
                buffer.append("(+)");
            }
            if (expr.isNot()) {
                buffer.append(" NOT");
            }
            buffer.append(" IN ");
            if (expr.getRightExpression() != null) {
                int start2 = buffer.length();
                expr.getRightExpression().accept(this);
                int ends2 = buffer.length();
                
            } else {
                expr.getRightItemsList().accept(this);
            }
        }
        relations.pop();
    }

    @Override
    public void visit(IsNullExpression expr) {
        relations.push(expr);
        super.visit(expr);
        relations.pop();
    }

    @Override
    public void visit(IsBooleanExpression expr) {
        relations.push(expr);
        super.visit(expr);
        relations.pop();
    }

    @Override
    public void visit(ExistsExpression expr) {
        relations.push(expr);
        super.visit(expr);
        relations.pop();
    }
}
