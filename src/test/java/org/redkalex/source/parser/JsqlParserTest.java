/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import java.util.function.IntFunction;
import org.junit.jupiter.api.*;
import org.redkale.boot.LoggingBaseHandler;
import org.redkale.source.*;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class JsqlParserTest {

    private static final IntFunction<String> signFunc = index -> "?";

    public static void main(String[] args) throws Throwable {
        LoggingBaseHandler.initDebugLogConfig();
        JsqlParserTest test = new JsqlParserTest();
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
        test.run11();
        test.run12();
        test.run13();
    }

    @Test
    public void run1() throws Exception {
        String sql = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table_${mytab} T "
            + "WHERE col1 = 10 AND (col2 = ##{c2} OR col3 = MAX(#{c3})) AND name LIKE '%'"
            + " AND seqid IS NULL AND (gameid IN :gameids OR gameName IN ('%', 'zzz'))"
            + " AND time BETWEEN #{time.min} AND #{time.max} AND col2 >= #{c2}"
            + " AND id IN (SELECT id FROM table2 WHERE name LIKE :name AND time > 1)";
        Map<String, Object> params = Utility.ofMap("mytab", "20240807", "min2", 1, "c2", 3,
            "time", new Range.IntRange(1, 2), "gameids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, true, params);
        String expect = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table_20240807 T "
            + "WHERE col1 = 10 AND (col2 = ?) AND name LIKE '%' "
            + "AND seqid IS NULL AND (gameid IN (2, 3) OR gameName IN ('%', 'zzz')) "
            + "AND time BETWEEN ? AND ? AND col2 >= ? AND id IN (SELECT id FROM table2 WHERE time > 1)";
        Assertions.assertEquals(expect, statement.getNativeSql());
        String expectCount = "SELECT COUNT(DISTINCT (col1, col2, col3)) FROM table_20240807 T "
            + "WHERE col1 = 10 AND (col2 = ?) AND name LIKE '%' AND seqid IS NULL "
            + "AND (gameid IN (2, 3) OR gameName IN ('%', 'zzz')) AND time BETWEEN ? "
            + "AND ? AND col2 >= ? AND id IN (SELECT id FROM table2 WHERE time > 1)";
        Assertions.assertEquals(expectCount, statement.getNativeCountSql());

        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("新countsql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================01============================================");
    }

    @Test
    public void run2() throws Exception {
        String sql = "SELECT 1";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, false, params);
        Assertions.assertEquals(sql, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================02============================================");
    }

    @Test
    public void run3() throws Exception {
        String sql = "INSERT INTO dayrecord (recordid, content, createTime) VALUES (1, ##{v2}, 3)";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        SourceException exp = null;
        DataNativeSqlStatement statement = null;
        try {
            statement = parser.parse(signFunc, "mysql", sql, false, params);
        } catch (SourceException e) {
            exp = e;
        }
        Assertions.assertEquals("Missing parameter v2", exp == null ? null : exp.getMessage());
        System.out.println("=====================================03============================================");
    }

    @Test
    public void run4() throws Exception {
        String sql = "INSERT INTO dayrecord (recordid, content, createTime) SELECT recordid, content, NOW() "
            + "FROM hourrecord WHERE createTime BETWEEN #{startTime} AND #{endTime} AND id > 0";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, false, params);
        String expect = "INSERT INTO dayrecord (recordid, content, createTime) SELECT recordid, content, NOW() "
            + "FROM hourrecord WHERE createTime BETWEEN ? AND ? AND id > 0";
        Assertions.assertEquals(expect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================04============================================");
    }

    @Test
    public void run5() throws Exception {
        String sql = "UPDATE dayrecord SET id = MAX(${id}), remark = #{remark}, "
            + "name = CASE WHEN type = 1 THEN #{v1} WHEN type = 2 THEN #{v2} ELSE #{v3} END "
            + "WHERE createTime BETWEEN #{startTime} AND #{endTime} AND id IN #{ids}";
        Map<String, Object> params = Utility.ofMap("id", 100, "v1", 1, "v2", 2, "v3", 3,
            "remark", "this is remark", "startTime", 1, "ids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, false, params);
        String expect = "UPDATE dayrecord SET id = MAX(100), remark = ?, name = CASE WHEN type = 1 THEN ? "
            + "WHEN type = 2 THEN ? ELSE ? END WHERE id IN (2, 3)";
        Assertions.assertEquals(expect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================05============================================");
    }

    @Test
    public void run6() throws Exception {
        String sql = "UPDATE dayrecord SET id = :idx, remark = :remark, "
            + "name = CASE WHEN type = 1 THEN :v1 WHEN type = 2 THEN :v2 ELSE :v3 END "
            + "WHERE createTime BETWEEN :startTime AND :endTime AND id IN :ids";
        Map<String, Object> params = Utility.ofMap("idx", 100, "v1", 1, "v2", 2, "v3", 3,
            "remark", "this is remark", "startTime", 1, "sts", List.of(2, 3), "ids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, false, params);
        String expect = "UPDATE dayrecord SET id = ?, remark = ?, name = CASE WHEN type = 1 THEN ? "
            + "WHEN type = 2 THEN ? ELSE ? END WHERE id IN (2, 3)";
        Assertions.assertEquals(expect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================06============================================");
    }

    @Test
    public void run7() throws Exception {
        String sql = "UPDATE dayrecord SET id = :idx, remark = #{remark}, "
            + "name = CASE WHEN type = MOD(#{t1},##{t2}) THEN #{v1} WHEN type = 2 THEN #{v2} ELSE #{v3} END "
            + "WHERE createTime BETWEEN :startTime AND :endTime AND id IN #{ids}";
        Map<String, Object> params = Utility.ofMap("idx", 100, "v1", 1, "v2", 2, "v3", 3,
            "remark", "this is remark", "t1", 36, "startTime", 1,
            "sts", List.of(2, 3), "ids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        SourceException exp = null;
        DataNativeSqlStatement statement = null;
        try {
            statement = parser.parse(signFunc, "mysql", sql, false, params);
        } catch (SourceException e) {
            exp = e;
        }
        Assertions.assertEquals("Missing parameter t2", exp == null ? null : exp.getMessage());
        System.out.println("=====================================07============================================");
    }

    @Test
    public void run8() throws Exception {
        String sql = "SELECT * FROM userdetail WHERE id = #{id} AND MOD(#{t1},#{t2}) = 3";
        Map<String, Object> params = Utility.ofMap("id", 1, "t1", 30, "t", 4);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, true, params);
        String repect = "SELECT * FROM userdetail WHERE id = ?";
        Assertions.assertEquals(repect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================08============================================");
    }

    @Test
    public void run9() throws Exception {
        String sql = "SELECT * FROM userdetail WHERE id = #{id} AND type = MOD(#{t1},#{t2})";
        Map<String, Object> params = Utility.ofMap("id", 1, "t1", 30, "t", 4);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, true, params);
        String repect = "SELECT * FROM userdetail WHERE id = ?";
        Assertions.assertEquals(repect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================09============================================");
    }

    @Test
    public void run10() throws Exception {
        String sql = "SELECT u.* FROM userdetail u LEFT JOIN role r ON r.userid = u.userid "
            + "WHERE u.id = #{id} AND r.type = MOD(#{t1},#{t2}) ORDER BY u.createTime DESC";
        Map<String, Object> params = Utility.ofMap("id", 1, "t1", 30, "t", 4);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, true, params);
        String repect = "SELECT u.* FROM userdetail u LEFT JOIN role r ON r.userid = u.userid "
            + "WHERE u.id = ? ORDER BY u.createTime DESC";
        Assertions.assertEquals(repect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());

        params = Utility.ofMap("id", 1, "t1", 30, "t1", "10", "t2", "5");
        statement = parser.parse(signFunc, "mysql", sql, true, params);
        repect = "SELECT u.* FROM userdetail u LEFT JOIN role r ON r.userid = u.userid "
            + "WHERE u.id = ? AND r.type = MOD(?, ?) ORDER BY u.createTime DESC";
        Assertions.assertEquals(repect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("count-sql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================10============================================");
    }

    @Test
    public void run11() throws Exception {
        String sql = "TRUNCATE TABLE userdetail";
        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, false, null);
        Assertions.assertEquals(sql, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================11============================================");
    }

    @Test
    public void run12() throws Exception {
        String sql = "ALTER TABLE userdetail ADD COLUMN name VARCHAR (32) NOT NULL DEFAULT '' COMMENT '名称'";
        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, false, null);
        Assertions.assertEquals(sql, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================12============================================");
    }

    @Test
    public void run13() throws Exception {
        String sql = "SELECT * FROM (SELECT * FROM pooldatarecord_20220114 UNION SELECT * FROM pooldatarecord_20220119 "
            + "WHERE userid = :idx) a ORDER BY name DESC";
        Map<String, Object> params = Utility.ofMap("idx", 100);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        DataNativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, true, params);

        String repect = "SELECT * FROM (SELECT * FROM pooldatarecord_20220114 UNION SELECT * FROM pooldatarecord_20220119 "
            + "WHERE userid = ?) a ORDER BY name DESC";
        Assertions.assertEquals(repect, statement.getNativeSql());
        repect = "SELECT COUNT(1) FROM (SELECT * FROM pooldatarecord_20220114 UNION SELECT * FROM pooldatarecord_20220119 WHERE userid = ?) a";
        Assertions.assertEquals(repect, statement.getNativeCountSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("count-sql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
        System.out.println("=====================================13============================================");
    }
}
