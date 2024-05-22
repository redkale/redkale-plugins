/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.function.IntFunction;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.junit.jupiter.api.Test;
import org.redkale.source.DataNativeSqlStatement;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class JsqlJdbcParserTest {

    private static final IntFunction<String> signFunc = index -> "?";

    public static void main(String[] args) throws Throwable {
        JsqlJdbcParserTest test = new JsqlJdbcParserTest();
        test.run1();
        test.run2();
        test.run3();
        test.run4();
        test.run5();
        test.run6();
        test.run7();
        test.run8();
        test.run9();
        test.run10();
    }

    @Test
    public void run1() throws Exception {
        String sql = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table T "
            + "WHERE col1 = 10 AND (col2 = :c2 OR col3 = MAX(:c3)) AND name LIKE '%'"
            + " AND seqid IS NULL AND (gameid IN :gameids OR gameName IN ('%', 'zzz'))"
            + " AND time BETWEEN :min AND :range_max AND col2 >= :c2"
            + " AND id IN (SELECT id FROM table2 WHERE name LIKE :name AND time > 1)";
        Map<String, Object> params = Utility.ofMap("min2", 1, "c2", 3, "range_max", 100, "gameids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("新countsql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("========================================1=========================================");
    }

    @Test
    public void run2() throws Exception {
        String sql = "SELECT 1";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=========================================2========================================");
    }

    @Test
    public void run3() throws Exception {
        String sql = "INSERT INTO dayrecord (recordid, content, createTime) VALUES (1, 2, 3)";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("==========================================3=======================================");
    }

    @Test
    public void run4() throws Exception {
        String sql = "INSERT INTO dayrecord (recordid, content, createTime) SELECT recordid, content, NOW() "
            + "FROM hourrecord WHERE createTime BETWEEN :startTime AND :endTime AND id > 0";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("==========================================4=======================================");
    }

    @Test
    public void run5() throws Exception {
        String sql = "UPDATE dayrecord SET id = MAX(:id), remark = :remark, name = CASE WHEN type = 1 THEN :v1 "
            + "WHEN type = 2 THEN :v2 ELSE :v3 END WHERE createTime BETWEEN :startTime AND :endTime AND id IN :ids";
        Map<String, Object> params = Utility.ofMap("id", 100, "v1", 1, "v2", 2, "v3", 3, "remark", "this is remark", "startTime", 1, "ids", List.of(2, 3));

        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        System.out.println(sqlParser.Statement());

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("==========================================5=======================================");
    }

    @Test
    public void run6() throws Exception {
        String sql = "UPDATE dayrecord SET id = MAX(:id), remark = :remark, name = CASE WHEN type = 1 THEN :v1 "
            + "WHEN type = 2 THEN :v2 ELSE :v3 END WHERE createTime BETWEEN :startTime AND :endTime AND id IN :ids";
        Map<String, Object> params = Utility.ofMap("id", 100, "v1", 1, "v2", 2, "v3", 3, "remark", "this is remark", "startTime", 1, "ids", new ArrayList<>());

        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        System.out.println(sqlParser.Statement());

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("==========================================6=======================================");
    }

    @Test
    public void run7() throws Exception {
        String sql = "UPDATE dayrecord SET id = :idx, remark = :remark, name = CASE WHEN type = 1 THEN :v1 "
            + "WHEN type = 2 THEN :v2 ELSE :v3 END WHERE createTime BETWEEN :startTime AND :endTime AND id IN (1,2,:ids)";
        Map<String, Object> params = Utility.ofMap("idx", 100, "v1", 1, "v2", 2, "v3", 3, "remark", "this is remark",
            "startTime", 1, "sts", List.of(2, 3), "ids", List.of(3, 4));

        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        System.out.println(sqlParser.Statement());

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("==========================================7=======================================");
    }

    @Test
    public void run8() throws Exception {
        String sql = "SELECT u.* FROM userdetail u LEFT JOIN role r ON r.userid = u.userid WHERE u.id = :idx ORDER BY u.createTime DESC";
        Map<String, Object> params = Utility.ofMap("idx", 100);

        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        PlainSelect stmt = (PlainSelect) sqlParser.Statement();
        System.out.println(stmt);
        System.out.println(stmt.getOrderByElements());
        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("count-sql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("==========================================8=======================================");
    }

    @Test
    public void run9() throws Exception {
        String sql = "SELECT * FROM (SELECT * FROM pooldatarecord_20220114 UNION SELECT * FROM pooldatarecord_20220119 WHERE userid = :idx) a";
        Map<String, Object> params = Utility.ofMap("idx", 100);
        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("count-sql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("==========================================9=======================================");
    }

    @Test
    public void run10() throws Exception {
        String sql = "SELECT u.* FROM userdetail u LEFT JOIN role r ON r.userid = u.userid "
            + "WHERE u.id = :idx AND u.name IN (SELECT name FROM orderdetail WHERE status = :status UNION SELECT name FROM orderdetail_his) "
            + "ORDER BY u.createTime DESC";
        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        PlainSelect stmt = (PlainSelect) sqlParser.Statement();
        System.out.println(((InExpression)((BinaryExpression)stmt.getWhere()).getRightExpression()).getRightExpression().getClass());
        
        Map<String, Object> params = Utility.ofMap("idx", 100);
        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("count-sql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("==========================================10=======================================");
    }
}
