/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.deparser.*;
import org.redkale.annotation.*;
import org.redkale.source.SourceException;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class DataExpressionVisitor extends ExpressionVisitorAdapter {

    private static final Package relationalPkg = Between.class.getPackage();

    private static final Package conditionalPkg = AndExpression.class.getPackage();

    //只存AndExpression、OrExpression、XorExpression
    private final Deque<BinaryExpression> conditions = new ArrayDeque<>();

    private final Deque<Expression> relations = new ArrayDeque<>();

    //所有参数名
    protected Set<String> fullNamedSet;

    //In或者NOT IN条件的参数名
    protected Map<String, List<InExpression>> fullInNamedMap;

    //参数
    protected Map<String, Object> paramValues;

    public static void main(String[] args) throws Throwable {
        {
            String sql = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table T WHERE col1 = 10 AND (col2 = :c2 OR col3 = MAX(:c3)) AND name LIKE '%' AND seqid IS NULL AND (gameid IN :gameids OR gameName IN ('%', 'zzz')) AND time BETWEEN :min AND :range_max AND id IN (SELECT id FROM table2 WHERE name LIKE :name AND time > 1)";
            Set<String> fullNamedSet = new LinkedHashSet<>();
            Map<String, List<InExpression>> fullInNamedMap = new LinkedHashMap<>();
            Map<String, Object> params = Utility.ofMap("min2", 1, "c2", 3, "range_max", 100, "gameids", List.of(2, 3));
            Select stmt = (Select) CCJSqlParserUtil.parse(sql, p -> p.withAllowComplexParsing(true));
            PlainSelect selectBody = (PlainSelect) stmt.getSelectBody();
            Expression where = selectBody.getWhere();
            if (where != null) {
                List<String> paramNames = new ArrayList<>();
                new DataExpressionVisitor(fullNamedSet, fullInNamedMap).parse(where, paramNames, params);
                selectBody.setWhere(trimExpr(where));
            }
            System.out.println(sql);
            System.out.println(stmt.toString());
        }
        {
            String sql = "UPDATE user SET id = :id, name = '' WHERE time > 0 AND remark IS NULL OR remark = ''";
            CCJSqlParser parser = new CCJSqlParser(sql).withAllowComplexParsing(true);
            Statement stmt = parser.Statement();
            Update up = (Update) stmt;
            System.out.println(stmt.toString());
        }
        {
            String sql = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table T WHERE col1 = 10 AND (col2 = :c2 OR col3 = MAX(:c3)) AND name LIKE '%' AND seqid IS NULL AND (gameid IN :gameids OR gameName IN ('%', 'zzz')) AND time BETWEEN :min AND :range_max AND id IN (SELECT id FROM table2 WHERE name LIKE :name AND time > 1)";
            Map<String, Object> params = Utility.ofMap("min2", 1, "c2", 3, "range_max", 100, "gameids", List.of(2, 3));
            final ExpressionDeParser exprDeParser = new ExpressionDeParser() {
                @Override
                public void visit(EqualsTo equalsTo) {
                    super.visit(equalsTo);
                }
            };
            final SelectDeParser selectParser = new SelectDeParser(exprDeParser, exprDeParser.getBuffer());
            exprDeParser.setSelectVisitor(selectParser);
            CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
            Select stmt = (Select) sqlParser.Statement();
            PlainSelect selectBody = (PlainSelect) stmt.getSelectBody();
            selectBody.accept(selectParser);
            System.out.println(stmt.toString());
            System.out.println(selectParser.getBuffer());

        }

    }

    //构造函数
    public DataExpressionVisitor(@Nonnull Set<String> fullNamedSet, @Nonnull Map<String, List<InExpression>> fullInNamedMap) {
        Objects.requireNonNull(fullNamedSet);
        Objects.requireNonNull(fullInNamedMap);
        this.fullNamedSet = fullNamedSet;
        this.fullInNamedMap = fullInNamedMap;
    }

    public Expression parse(Expression where, @Nullable List<String> paramNames, @Nullable Map<String, Object> params) {
        this.paramValues = params;
        if (where != null) {
            where.accept(this);
            if (params != null) {
                where = trimExpr(where);
            }
            if (where != null && paramNames != null) {
                //类似Between这种条件可能存在多个:name参数的
                //不能排除其他条件包含，或者前一个参数存在, 所以不能在此处写进namedSet
                where.accept(new ExpressionVisitorAdapter() {

                    @Override
                    public void visit(JdbcNamedParameter expr) {
                        super.visit(expr);
                        paramNames.add(expr.getName());
                    }
                });
            }
        }
        this.paramValues = null;
        return where;
    }

    //--------------------------------------------------
    @Override
    public void visit(JdbcNamedParameter expr) {
        super.visit(expr);
        fullNamedSet.add(expr.getName());
        Expression relation = relations.peek();
        if (relation instanceof InExpression) {
            this.fullInNamedMap.computeIfAbsent(expr.getName(), t -> new ArrayList<>()).add((InExpression) relation);
        }
        if (paramValues != null) {
            Object val = paramValues.get(expr.getName());
            if (val == null) { //没有参数值
                BinaryExpression condition = conditions.peek();
                if (condition.getLeftExpression() == relation) {
                    condition.setLeftExpression(null);
                } else {
                    condition.setRightExpression(null);
                }
            }
        }
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new SourceException("Cannot contains ? JdbcParameter");
    }

    private void visitConditionExpression(final BinaryExpression expr) {
        conditions.push(expr);
        super.visitBinaryExpression(expr);
        conditions.pop();
        if (paramValues != null) {
            expr.setLeftExpression(trimExpr(expr.getLeftExpression()));
            expr.setRightExpression(trimExpr(expr.getRightExpression()));
        }
    }

    @Override
    public void visit(SubSelect subSelect) {
        super.visit(subSelect);
        SelectBody body = subSelect.getSelectBody();
        if (body instanceof PlainSelect) {
            PlainSelect selectBody = (PlainSelect) body;
            Expression where = selectBody.getWhere();
            if (where != null) {
                where.accept(this);
                if (paramValues != null) {
                    selectBody.setWhere(trimExpr(where));
                }
            }
        }
    }

    //--------------------------------------------------  
    @Override
    protected void visitBinaryExpression(BinaryExpression expr) {
        if (expr.getClass().getPackage() == relationalPkg) {
            relations.push(expr);
            super.visitBinaryExpression(expr);
            relations.pop();
        } else {
            super.visitBinaryExpression(expr);
        }
    }

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
//
//    //--------------------------------------------------
//    public static Expression cloneConditionExpression(Expression expr) {
//        if (expr instanceof AndExpression) {
//            AndExpression old = (AndExpression) expr;
//            return new AndExpression()
//                .withUseOperator(old.isUseOperator())
//                .withLeftExpression(cloneConditionExpression(old.getLeftExpression()))
//                .withRightExpression(cloneConditionExpression(old.getRightExpression()));
//        } else if (expr instanceof OrExpression) {
//            OrExpression old = (OrExpression) expr;
//            return new OrExpression()
//                .withLeftExpression(cloneConditionExpression(old.getLeftExpression()))
//                .withRightExpression(cloneConditionExpression(old.getRightExpression()));
//        } else if (expr instanceof XorExpression) {
//            XorExpression old = (XorExpression) expr;
//            return new XorExpression()
//                .withLeftExpression(cloneConditionExpression(old.getLeftExpression()))
//                .withRightExpression(cloneConditionExpression(old.getRightExpression()));
//        } else {
//            return expr;
//        }
//    }
//

    public static Expression trimExpr(Expression expr) {
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
}
