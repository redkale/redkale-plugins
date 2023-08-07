/*
 *
 */
package org.redkalex.source.mysql;

import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.select.*;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class SqlParserTest {

    private static final Package relationalPkg = Between.class.getPackage();

    private static final Package conditionalPkg = AndExpression.class.getPackage();

    private static Expression trimExpr(Expression expr) {
        if (expr instanceof Parenthesis) {
            Parenthesis hesis = (Parenthesis) expr;
            Expression sub = trimExpr(hesis.getExpression());
            if (sub == null) {
                return null;
            } else {
                if (sub.getClass().getPackage() != conditionalPkg) {
                    return sub;
                }
                hesis.setExpression(sub);
                return expr;
            }
        }
        if (!(expr instanceof BinaryExpression)) {
            return expr;
        }
        BinaryExpression bin = (BinaryExpression) expr;
        if (bin.getLeftExpression() == null) {
            return bin.getRightExpression();
        } else if (bin.getRightExpression() == null) {
            return bin.getLeftExpression();
        } else {
            return bin;
        }
    }

    private static void trimSelectBody(SelectBody body, Set<String> namedSet, Map<String, Object> params) {
        if (!(body instanceof PlainSelect)) {
            return;
        }
        Deque<BinaryExpression> conditions = new ArrayDeque<>(); //只存放AndExpression、OrExpression、XorExpression
        Deque<Expression> relations = new ArrayDeque<>();
        PlainSelect selectBody = (PlainSelect) body;
        Expression where = selectBody.getWhere();
        ExpressionVisitorAdapter adapter = new ExpressionVisitorAdapter() {

            @Override
            public void visit(AndExpression expr) {
                visitConditionExpression(expr);
            }

            @Override
            public void visit(OrExpression expr) {
                visitConditionExpression(expr);
            }

            @Override
            public void visit(XorExpression expr) {
                visitConditionExpression(expr);
            }

            private void visitConditionExpression(final BinaryExpression expr) {
                conditions.push(expr);
                super.visitBinaryExpression(expr);
                conditions.pop();
                expr.setLeftExpression(trimExpr(expr.getLeftExpression()));
                expr.setRightExpression(trimExpr(expr.getRightExpression()));
            }

            @Override
            public void visit(SubSelect subSelect) {
                trimSelectBody(subSelect.getSelectBody(), namedSet, params);
                super.visit(subSelect);
            }

            @Override
            public void visit(JdbcNamedParameter expr) {
                super.visit(expr);
                if (!params.containsKey(expr.getName())) { //没有参数值
                    Expression relation = relations.peek();
                    BinaryExpression condition = conditions.peek();
                    if (condition.getLeftExpression() == relation) {
                        condition.setLeftExpression(null);
                    } else {
                        condition.setRightExpression(null);
                    }
                    //类似Between这样可能存在多个:name参数的
                    //不能排除其他条件包含，或者前一个参数存在, 所以不能在此处写进namedSet
                }
            }

            //--------------------------------------------------
            @Override
            protected void visitBinaryExpression(BinaryExpression expr) {
                boolean relate = expr.getClass().getPackage() == relationalPkg;
                if (relate) {
                    relations.push(expr);
                }
                super.visitBinaryExpression(expr);
                if (relate) {
                    relations.pop();
                }
            }

            //--------------------------------------------------
            @Override
            public void visit(Between expr) {
                relations.push(expr);
                super.visit(expr);
                relations.pop();
            }

            @Override
            public void visit(InExpression expr) {
                relations.push(expr);
                super.visit(expr);
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

        };
        where.accept(adapter);
        where = trimExpr(where);
        if (where != null) {
            where.accept(new ExpressionVisitorAdapter() {

                @Override
                public void visit(JdbcNamedParameter expr) {
                    super.visit(expr);
                    namedSet.add(expr.getName());
                }
            });
        }
        selectBody.setWhere(where);
    }

    public static void main(String[] args) throws Throwable {
        GreaterThanEquals s;
        AndExpression a;
        LikeExpression ac;
        EqualsTo e;
        Parenthesis z;

        String sql = "SELECT distinct col1 AS a, col2 AS b, col3 AS c FROM table T \n"
            + "WHERE col1 = 10 AND (col2 = :c2 OR col3 = MAX(:c3)) AND name like '%' AND seqid is null \n"
            + "AND time between :min and :range_max AND  id in (SELECT id from table2 where name like :name AND time > 1)";
        {
            Map<String, Object> params = Utility.ofMap("min2", 1, "c2", 3, "range_max", 100);
            CCJSqlParser parser = new CCJSqlParser(sql).withAllowComplexParsing(true);
            Select stmt = (Select) parser.Statement();
            Set<String> namedParams = new LinkedHashSet<>();
            trimSelectBody(stmt.getSelectBody(), namedParams, params);
            System.out.println("需要传的参数: " + namedParams);
            System.out.println(stmt.toString());
        }

    }
}
