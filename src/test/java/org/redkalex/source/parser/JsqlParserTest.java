/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.junit.jupiter.api.Test;
import org.redkale.source.DataNativeSqlParser;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class JsqlParserTest {

    private static final java.util.function.Function<Integer, String> signFunc = index -> "?";

    public static void main(String[] args) throws Throwable {
        JsqlParserTest test = new JsqlParserTest();
        test.run1();
        test.run2();
        test.run3();
        test.run4();
        test.run5();
        test.run6();
    }

    @Test
    public void run1() throws Exception {
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
        DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("新countsql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run2() throws Exception {
        String sql = "SELECT 1";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run3() throws Exception {
        String sql = "INSERT INTO dayrecord (recordid, content, createTime) VALUES (1, 2, 3)";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run4() throws Exception {
        String sql = "INSERT INTO dayrecord (recordid, content, createTime) SELECT recordid, content, NOW() FROM hourrecord WHERE createTime BETWEEN :startTime AND :endTime AND id > 0";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run5() throws Exception {
        String sql = "UPDATE dayrecord SET id = MAX(:id), remark = :remark, name = CASE WHEN type = 1 THEN :v1 WHEN type = 2 THEN :v2 ELSE :v3 END WHERE createTime BETWEEN :startTime AND :endTime AND id IN :ids";
        Map<String, Object> params = Utility.ofMap("id", 100, "v1", 1, "v2", 2, "v3", 3, "remark", "this is remark", "startTime", 1, "ids", List.of(2, 3));

        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        System.out.println(sqlParser.Statement());

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run6() throws Exception {
        String sql = "UPDATE dayrecord SET id = SELECT MAX(:id) FROM tt WHERE status IN :sts, remark = :remark, name = CASE WHEN type = 1 THEN :v1 WHEN type = 2 THEN :v2 ELSE :v3 END WHERE createTime BETWEEN :startTime AND :endTime AND id IN :ids";
        Map<String, Object> params = Utility.ofMap("id", 100, "v1", 1, "v2", 2, "v3", 3, "remark", "this is remark", "startTime", 1, "sts", List.of(2, 3), "ids", List.of(2, 3));

        CCJSqlParser sqlParser = new CCJSqlParser(sql).withAllowComplexParsing(true);
        System.out.println(sqlParser.Statement());

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlParser.NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }
}
