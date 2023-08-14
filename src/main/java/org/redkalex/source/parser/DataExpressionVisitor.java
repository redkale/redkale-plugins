/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.select.*;
import org.redkale.source.SourceException;

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

    protected Select select;

    //所有参数名
    protected Set<String> fullNamedSet;

    public static void main(String[] args) throws Throwable {
        final String sql = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table T "
            + "WHERE col1 = 10 AND (col2 = :c2 OR col3 = MAX(:c3)) AND name LIKE '%'"
            + " AND seqid IS NULL AND (gameid IN :gameids OR gameName IN ('%', 'zzz'))"
            + " AND time BETWEEN :min AND :range_max AND col2 >= :c2"
            + " AND id IN (SELECT id FROM table2 WHERE name LIKE :name AND time > 1)";
        final DataExpressionVisitor visitor = new DataExpressionVisitor();
        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        Select stmt = (Select) sqlParser.Statement();
        PlainSelect selectBody = (PlainSelect) stmt.getSelectBody();
        selectBody.getWhere().accept(visitor);
        System.out.println(stmt.toString());
        System.out.println(visitor.fullNamedSet);
    }

    //构造函数
    public DataExpressionVisitor() {
        this.fullNamedSet = new LinkedHashSet<>();
    }

    public Expression parse(String sql) {
        try {
            CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
            Select stmt = (Select) sqlParser.Statement();
            PlainSelect selectBody = (PlainSelect) stmt.getSelectBody();
            Expression where = selectBody.getWhere();
            if (where != null) {
                where.accept(this);
            }
            return where;
        } catch (ParseException e) {
            throw new SourceException("parse error, sql: " + sql, e);
        }
    }

    //--------------------------------------------------
    @Override
    public void visit(JdbcNamedParameter expr) {
        super.visit(expr);
        fullNamedSet.add(expr.getName());
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {
        throw new SourceException("Cannot contains ? JdbcParameter");
    }

    private void visitConditionExpression(final BinaryExpression expr) {
        conditions.push(expr);
        super.visitBinaryExpression(expr);
        conditions.pop();
    }

    @Override
    public void visit(SubSelect subSelect) {
        super.visit(subSelect);
    }
 
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

}
