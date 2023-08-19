/*
 *
 */
package org.redkalex.source.parser;

import java.util.*;
import org.junit.jupiter.api.*;
import org.redkale.boot.LoggingBaseHandler;
import org.redkale.source.DataNativeSqlParser.NativeSqlStatement;
import org.redkale.source.*;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class JsqlParser2Test {

    private static final java.util.function.Function<Integer, String> signFunc = index -> "?";

    public static void main(String[] args) throws Throwable {
        LoggingBaseHandler.initDebugLogConfig();
        JsqlParser2Test test = new JsqlParser2Test();
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
    }

    @Test
    public void run1() throws Exception {
        String sql = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table_#{mytab} T "
            + "WHERE col1 = 10 AND (col2 = $${c2} OR col3 = MAX(${c3})) AND name LIKE '%'"
            + " AND seqid IS NULL AND (gameid IN :gameids OR gameName IN ('%', 'zzz'))"
            + " AND time BETWEEN ${time.min} AND ${time.max} AND col2 >= ${c2}"
            + " AND id IN (SELECT id FROM table2 WHERE name LIKE :name AND time > 1)";
        Map<String, Object> params = Utility.ofMap("mytab", "20240807", "min2", 1, "c2", 3, "time", new Range.IntRange(1, 2), "gameids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        String expect = "SELECT DISTINCT col1 AS a, col2 AS b, col3 AS c FROM table_20240807 T WHERE col1 = 10 AND (col2 = ?) AND name LIKE '%' AND seqid IS NULL AND (gameid IN (2, 3) OR gameName IN ('%', 'zzz')) AND time BETWEEN ? AND ? AND col2 >= ? AND id IN (SELECT id FROM table2 WHERE time > 1)";
        Assertions.assertEquals(expect, statement.getNativeSql());
        String expectCount = "SELECT COUNT(DISTINCT col1 AS a, col2 AS b, col3 AS c) FROM table_20240807 T WHERE col1 = 10 AND (col2 = ?) AND name LIKE '%' AND seqid IS NULL AND (gameid IN (2, 3) OR gameName IN ('%', 'zzz')) AND time BETWEEN ? AND ? AND col2 >= ? AND id IN (SELECT id FROM table2 WHERE time > 1)";
        Assertions.assertEquals(expectCount, statement.getNativeCountSql());

        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("新countsql = " + statement.getNativeCountSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run2() throws Exception {
        String sql = "SELECT 1";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        Assertions.assertEquals(sql, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run3() throws Exception {
        String sql = "INSERT INTO dayrecord (recordid, content, createTime) VALUES (1, ${v2}, 3)";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        SourceException exp = null;
        NativeSqlStatement statement = null;
        try {
            statement = parser.parse(signFunc, "mysql", sql, params);
        } catch (SourceException e) {
            exp = e;
        }
        Assertions.assertEquals("Missing parameter [v2]", exp == null ? null : exp.getMessage());
    }

    @Test
    public void run4() throws Exception {
        String sql = "INSERT INTO dayrecord (recordid, content, createTime) SELECT recordid, content, NOW() FROM hourrecord WHERE createTime BETWEEN ${startTime} AND ${endTime} AND id > 0";
        Map<String, Object> params = Utility.ofMap("startTime", 1, "endTime", 3);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run5() throws Exception {
        String sql = "UPDATE dayrecord SET id = MAX(#{id}), remark = ${remark}, name = CASE WHEN type = 1 THEN ${v1} WHEN type = 2 THEN ${v2} ELSE ${v3} END WHERE createTime BETWEEN ${startTime} AND ${endTime} AND id IN ${ids}";
        Map<String, Object> params = Utility.ofMap("id", 100, "v1", 1, "v2", 2, "v3", 3, "remark", "this is remark", "startTime", 1, "ids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run6() throws Exception {
        String sql = "UPDATE dayrecord SET id = SELECT MAX(${id}) FROM tt WHERE status IN :sts, remark = :remark, name = CASE WHEN type = 1 THEN :v1 WHEN type = 2 THEN :v2 ELSE :v3 END WHERE createTime BETWEEN :startTime AND :endTime AND id IN :ids";
        Map<String, Object> params = Utility.ofMap("id", 100, "v1", 1, "v2", 2, "v3", 3, "remark", "this is remark", "startTime", 1, "sts", List.of(2, 3), "ids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run7() throws Exception {
        String sql = "UPDATE dayrecord SET id = SELECT MAX(${id}) FROM tt WHERE status IN :sts, remark = ${remark}, name = CASE WHEN type = MOD(${t1},${t2}) THEN ${v1} WHEN type = 2 THEN ${v2} ELSE ${v3} END WHERE createTime BETWEEN :startTime AND :endTime AND id IN ${ids}";
        Map<String, Object> params = Utility.ofMap("id", 100, "v1", 1, "v2", 2, "v3", 3, "remark", "this is remark", "t1", 36, "startTime", 1, "sts", List.of(2, 3), "ids", List.of(2, 3));

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        SourceException exp = null;
        NativeSqlStatement statement = null;
        try {
            statement = parser.parse(signFunc, "mysql", sql, params);
        } catch (SourceException e) {
            exp = e;
        }
        Assertions.assertEquals("Missing parameter [t2]", exp == null ? null : exp.getMessage());
    }

    @Test
    public void run8() throws Exception {
        String sql = "SELECT * FROM userdetail WHERE id = ${id} AND MOD(${t1},${t2}) = 3";
        Map<String, Object> params = Utility.ofMap("id", 1, "t1", 30, "t", 4);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        String repect = "SELECT * FROM userdetail WHERE id = ?";
        Assertions.assertEquals(repect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run9() throws Exception {
        String sql = "SELECT * FROM userdetail WHERE id = ${id} AND type = MOD(${t1},${t2})";
        Map<String, Object> params = Utility.ofMap("id", 1, "t1", 30, "t", 4);

        DataNativeJsqlParser parser = new DataNativeJsqlParser();
        NativeSqlStatement statement = parser.parse(signFunc, "mysql", sql, params);
        String repect = "SELECT * FROM userdetail WHERE id = ?";
        Assertions.assertEquals(repect, statement.getNativeSql());
        System.out.println("新sql = " + statement.getNativeSql());
        System.out.println("paramNames = " + statement.getParamNames());
    }

    @Test
    public void run10() throws Exception {
    }

    @Test
    public void run11() throws Exception {
    }

}
