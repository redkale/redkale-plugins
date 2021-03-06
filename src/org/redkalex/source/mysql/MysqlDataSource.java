/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.*;
import org.redkale.net.*;
import org.redkale.service.Local;
import org.redkale.source.*;
import static org.redkale.source.DataSources.JDBC_CONNECTIONS_LIMIT;
import org.redkale.util.*;
import static org.redkale.boot.Application.RESNAME_APP_GROUP;

/**
 * MySQL数据库的DataSource实现
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(DataSource.class)
public class MysqlDataSource extends DataSqlSource {

    private static final byte[] BYTES_NULL = "NULL".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SQL_SET_AUTOCOMMIT_0 = "SET autocommit=0".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SQL_SET_AUTOCOMMIT_1 = "SET autocommit=1".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SQL_COMMIT = "COMMIT".getBytes(StandardCharsets.UTF_8);

    private static final byte[] SQL_ROLLBACK = "ROLLBACK".getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws Throwable {
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        ResourceFactory.root().register(RESNAME_APP_GROUP, asyncGroup);
        Properties prop = new Properties();
        prop.setProperty(DataSources.JDBC_URL, "jdbc:mysql://localhost:3306/platf_core?characterEncoding=utf8");
        prop.setProperty(DataSources.JDBC_USER, "root");
        prop.setProperty(DataSources.JDBC_PWD, "");
        MysqlDataSource source = new MysqlDataSource("", null, prop, prop);
        ResourceFactory.root().inject(source);
        source.init(null);
        source.getReadPoolSource().poll();
        source.directExecute("SET NAMES UTF8MB4");
        source.directQuery("select * from userrobot limit 2", (ResultSet reset) -> {
            try {
                if (reset.next()) {
                    System.out.println("存在值: " + reset.getInt("userid"));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    public MysqlDataSource(String unitName, URL persistxml, Properties readprop, Properties writeprop) {
        super(unitName, persistxml, readprop, writeprop);
    }

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (readprop.getProperty(JDBC_CONNECTIONS_LIMIT) == null) {
            try {
                CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
                connFuture.thenCompose(conn -> executeQuery(null, conn, "SHOW VARIABLES LIKE 'max_connections'").thenAccept((ResultSet reset) -> {
                    try {
                        if (reset.next()) {
                            String maxstr = reset.getString(2);
                            logger.log(Level.INFO, "DataSource[name=" + (name == null || name.isEmpty() ? "''" : name) + "] max_connections = " + maxstr);
                            int maxconn = Math.max(10, Integer.parseInt(maxstr) * 8 / 10);
                            updateMaxconns(maxconn);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                })).get(3, TimeUnit.SECONDS);
            } catch (Exception e) {
            }
        }
    }

    @Local
    protected PoolSource readPoolSource() {
        return readPool;
    }

    @Local
    protected PoolSource writePoolSource() {
        return writePool;
    }

    @Override
    protected String prepareParamSign(int index) {
        return "$" + index;
    }

    @Override
    protected final boolean isAsync() {
        return true;
    }

    @Override
    protected PoolSource createPoolSource(DataSource source, AsyncGroup asyncGroup, String rwtype, ArrayBlockingQueue queue, Semaphore semaphore, Properties prop) {
        return new MyPoolSource(asyncGroup, rwtype, queue, semaphore, prop, logger);
    }

    @Override
    protected <T> CompletableFuture<Integer> insertDB(EntityInfo<T> info, T... values) {
        final Attribute<T, Serializable>[] attrs = info.getInsertAttributes();
        final byte[][] sqlBytesArray = new byte[1][];
        String presql = info.getInsertPrepareSQL(values[0]);
        byte[] prebs = (presql.substring(0, presql.indexOf("VALUES")) + "VALUES").getBytes(StandardCharsets.UTF_8); //不会存在非ASCII字符
        ByteArray ba = new ByteArray();
        ba.put(prebs);
        for (int i = 0; i < values.length; i++) {
            if (i > 0) ba.put((byte) ',');
            ba.put((byte) '(');
            for (int j = 0; j < attrs.length; j++) {
                if (j > 0) ba.put((byte) ',');
                byte[] param = formatPrepareParam(info, attrs[j], attrs[j].get(values[i]));
                if (param == null) {
                    ba.put(BYTES_NULL);
                } else {
                    ba.put((byte) 0x27);
                    for (byte b : param) {
                        if (b == 0x5c || b == 0x27) ba.put((byte) 0x5c);
                        ba.put(b);
                    }
                    ba.put((byte) 0x27);
                }
            }
            ba.put((byte) ')');
        }
        sqlBytesArray[0] = ba.getBytes();
        if (info.isLoggable(logger, Level.FINEST)) {
            String realsql = ba.toString(StandardCharsets.UTF_8);
            if (info.isLoggable(logger, Level.FINEST, realsql)) logger.finest(info.getType().getSimpleName() + " insert sql=" + realsql);
        }
        CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
        return connFuture.thenCompose((conn) -> executeBatchUpdate(info, conn, UpdateMode.INSERT, values[0], sqlBytesArray).thenApply((int[] rs) -> {
            int count = 0;
            for (int i : rs) count += i;
            return count;
        }));
    }

    @Override
    protected <T> CompletableFuture<Integer> deleteDB(EntityInfo<T> info, Flipper flipper, String sql) {
        final String realsql = flipper == null || flipper.getLimit() <= 0 ? sql : (sql + " LIMIT " + flipper.getLimit());
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, realsql)) logger.finest(info.getType().getSimpleName() + " delete sql=" + realsql);
        }
        CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
        return connFuture.thenCompose((conn) -> executeOneUpdate(info, conn, UpdateMode.DELETE, realsql.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    protected <T> CompletableFuture<Integer> clearTableDB(EntityInfo<T> info, final String table, String sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " clearTable sql=" + sql);
        }
        CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
        return connFuture.thenCompose((conn) -> {
            CompletableFuture<Integer> future = executeOneUpdate(info, conn, UpdateMode.CLEAR, sql.getBytes(StandardCharsets.UTF_8));
            final CompletableFuture<Integer> newFuture = new CompletableFuture<>();
            future.whenComplete((o, ex1) -> {
                if (ex1 == null) {
                    newFuture.complete(o);
                    return;
                }
                try {
                    while (ex1 instanceof CompletionException) ex1 = ex1.getCause();
                    if (info.isTableNotExist((SQLException) ex1)) {
                        newFuture.complete(-1);
                    } else {
                        newFuture.completeExceptionally(ex1);
                    }
                } catch (Throwable e) {
                    newFuture.completeExceptionally(ex1);
                }
            });
            return newFuture;
        });
    }

    @Override
    protected <T> CompletableFuture<Integer> dropTableDB(EntityInfo<T> info, final String table, String sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " dropTable sql=" + sql);
        }
        CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
        return connFuture.thenCompose((conn) -> {
            CompletableFuture<Integer> future = executeOneUpdate(info, conn, UpdateMode.DROP, sql.getBytes(StandardCharsets.UTF_8));
            final CompletableFuture<Integer> newFuture = new CompletableFuture<>();
            future.whenComplete((o, ex1) -> {
                if (ex1 == null) {
                    newFuture.complete(o);
                    return;
                }
                try {
                    while (ex1 instanceof CompletionException) ex1 = ex1.getCause();
                    if (info.isTableNotExist((SQLException) ex1)) {
                        newFuture.complete(-1);
                    } else {
                        newFuture.completeExceptionally(ex1);
                    }
                } catch (Throwable e) {
                    newFuture.completeExceptionally(ex1);
                }
            });
            return newFuture;
        });
    }

    @Override
    protected <T> CompletableFuture<Integer> updateDB(EntityInfo<T> info, ChannelContext channel, final T... values) {
        final Attribute<T, Serializable> primary = info.getPrimary();
        final Attribute<T, Serializable>[] attrs = info.getUpdateAttributes();
        final byte[][] sqlBytesArray = new byte[values.length][];
        final char[] sqlChs = info.getUpdatePrepareSQL(values[0]).toCharArray(); //不会存在非ASCII字符
        ByteArray ba = new ByteArray();
        for (int i = 0; i < values.length; i++) {
            int index = -1;
            for (char ch : sqlChs) {
                if (ch != '?') {
                    ba.put((byte) ch);
                    continue;
                }
                index++;
                byte[] param = index < attrs.length ? formatPrepareParam(info, attrs[index], attrs[index].get(values[i])) : formatPrepareParam(info, primary, primary.get(values[i])); //最后一个是主键
                if (param == null) {
                    ba.put(BYTES_NULL);
                } else {
                    ba.put((byte) 0x27);
                    for (byte b : param) {
                        if (b == 0x5c || b == 0x27) ba.put((byte) 0x5c);
                        ba.put(b);
                    }
                    ba.put((byte) 0x27);
                }
            }
            sqlBytesArray[i] = ba.getBytes();
            if (info.isLoggable(logger, Level.FINEST)) {
                String realsql = ba.toString(StandardCharsets.UTF_8);
                if (info.isLoggable(logger, Level.FINEST, realsql)) logger.finest(info.getType().getSimpleName() + " update sql=" + realsql);
            }
            ba.clear();
        }
        CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
        return connFuture.thenCompose((conn) -> executeBatchUpdate(info, conn, UpdateMode.UPDATE, null, sqlBytesArray).thenApply((int[] rs) -> {
            int count = 0;
            for (int i : rs) count += i;
            return count;
        }));
    }

    @Override
    protected <T> CompletableFuture<Integer> updateDB(EntityInfo<T> info, Flipper flipper, String sql, boolean prepared, Object... params) {
        final String realsql = flipper == null || flipper.getLimit() <= 0 ? sql : (sql + " LIMIT " + flipper.getLimit());
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, realsql)) logger.finest(info.getType().getSimpleName() + " update sql=" + realsql);
        }
        if (!prepared) {
            CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
            return connFuture.thenCompose((conn) -> executeOneUpdate(info, conn, UpdateMode.UPDATE, realsql.getBytes(StandardCharsets.UTF_8)));
        }
        ByteArray ba = new ByteArray();
        String[] subsqls = realsql.split("\\" + prepareParamSign(1).replace("1", "") + "\\d+");
        for (int i = 0; i < params.length; i++) {
            ba.put(subsqls[i].getBytes(StandardCharsets.UTF_8));
            byte[] param = formatPrepareParam(info, null, params[i]);
            if (param == null) {
                ba.put(BYTES_NULL);
            } else {
                ba.put((byte) 0x27);
                for (byte b : param) {
                    if (b == 0x5c || b == 0x27) ba.put((byte) 0x5c);
                    ba.put(b);
                }
                ba.put((byte) 0x27);
            }
        }
        for (int i = params.length; i < subsqls.length; i++) {
            ba.put(subsqls[i].getBytes(StandardCharsets.UTF_8));
        }
        CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
        return connFuture.thenCompose((conn) -> executeOneUpdate(info, conn, UpdateMode.UPDATE, ba.getBytes()));
    }

    @Override
    protected <T, N extends Number> CompletableFuture<Map<String, N>> getNumberMapDB(EntityInfo<T> info, String sql, FilterFuncColumn... columns) {
        CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
        return connFuture.thenCompose((conn) -> exceptionallyQueryTableNotExist(executeQuery(info, conn, sql), info).thenApply((ResultSet set) -> {
            final Map map = new HashMap<>();
            try {
                if (set.next()) {
                    int index = 0;
                    for (FilterFuncColumn ffc : columns) {
                        for (String col : ffc.cols()) {
                            Object o = set.getObject(++index);
                            Number rs = ffc.getDefvalue();
                            if (o != null) rs = (Number) o;
                            map.put(ffc.col(col), rs);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return map;
        }));
    }

    @Override
    protected <T> CompletableFuture<Number> getNumberResultDB(EntityInfo<T> info, String sql, Number defVal, String column) {
        CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
        return connFuture.thenCompose((conn) -> exceptionallyQueryTableNotExist(executeQuery(info, conn, sql), info)
            .thenApply((ResultSet set) -> {
                Number rs = defVal;
                try {
                    if (set.next()) {
                        Object o = set.getObject(1);
                        if (o != null) rs = (Number) o;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return rs;
            }));
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N>> queryColumnMapDB(EntityInfo<T> info, String sql, String keyColumn) {
        CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
        return connFuture.thenCompose((conn) -> exceptionallyQueryTableNotExist(executeQuery(info, conn, sql), info).thenApply((ResultSet set) -> {
            Map<K, N> rs = new LinkedHashMap<>();
            try {
                while (set.next()) {
                    rs.put((K) set.getObject(1), (N) set.getObject(2));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return rs;
        }));
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K[], N[]>> queryColumnMapDB(EntityInfo<T> info, String sql, final ColumnNode[] funcNodes, final String[] groupByColumns) {
        CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
        return connFuture.thenCompose((conn) -> executeQuery(info, conn, sql).thenApply((ResultSet set) -> {
            Map rs = new LinkedHashMap<>();
            try {
                while (set.next()) {
                    int index = 0;
                    Serializable[] keys = new Serializable[groupByColumns.length];
                    for (int i = 0; i < keys.length; i++) {
                        keys[i] = (Serializable) set.getObject(++index);
                    }
                    Number[] vals = new Number[funcNodes.length];
                    for (int i = 0; i < vals.length; i++) {
                        vals[i] = (Number) set.getObject(++index);
                    }
                    rs.put(keys, vals);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return rs;
        }));
    }

    @Override
    protected <T> CompletableFuture<T> findDB(EntityInfo<T> info, ChannelContext channel, String sql, boolean onlypk, SelectColumn selects) {
        CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
        return connFuture.thenCompose((conn) -> exceptionallyQueryTableNotExist(executeQuery(info, conn, sql), info).thenApply((ResultSet set) -> {
            T rs = null;
            try {
                rs = set.next() ? getEntityValue(info, selects, set) : null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return rs;
        }));
    }

    @Override
    protected <T> CompletableFuture<Serializable> findColumnDB(EntityInfo<T> info, String sql, boolean onlypk, String column, Serializable defValue) {
        CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
        return connFuture.thenCompose((conn) -> exceptionallyQueryTableNotExist(executeQuery(info, conn, sql), info).thenApply((ResultSet set) -> {
            Serializable val = defValue;
            try {
                if (set.next()) {
                    final Attribute<T, Serializable> attr = info.getAttribute(column);
                    val = getFieldValue(info, attr, set, 1);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return val == null ? defValue : val;
        }));
    }

    @Override
    protected <T> CompletableFuture<Boolean> existsDB(EntityInfo<T> info, String sql, boolean onlypk) {
        CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
        return connFuture.thenCompose((conn) -> exceptionallyQueryTableNotExist(executeQuery(info, conn, sql), info).thenApply((ResultSet set) -> {
            try {
                boolean rs = set.next() ? (set.getInt(1) > 0) : false;
                return rs;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    protected <T> CompletableFuture<Sheet<T>> querySheetDB(EntityInfo<T> info, final boolean readcache, boolean needtotal, final boolean distinct, SelectColumn selects, Flipper flipper, FilterNode node) {
        final SelectColumn sels = selects;
        final Map<Class, String> joinTabalis = node == null ? null : getJoinTabalis(node);
        final CharSequence join = node == null ? null : createSQLJoin(node, this, false, joinTabalis, new HashSet<>(), info);
        final CharSequence where = node == null ? null : createSQLExpress(node, info, joinTabalis);
        final String listsql = "SELECT " + (distinct ? "DISTINCT " : "") + info.getQueryColumns("a", selects) + " FROM " + info.getTable(node) + " a" + (join == null ? "" : join)
            + ((where == null || where.length() == 0) ? "" : (" WHERE " + where)) + createSQLOrderby(info, flipper) + (flipper == null || flipper.getLimit() < 1 ? "" : (" LIMIT " + flipper.getLimit() + " OFFSET " + flipper.getOffset()));
        if (readcache && info.isLoggable(logger, Level.FINEST, listsql)) logger.finest(info.getType().getSimpleName() + " query sql=" + listsql);
        if (!needtotal) {
            CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
            return connFuture.thenCompose((conn) -> exceptionallyQueryTableNotExist(executeQuery(info, conn, listsql), info)
                .thenApply((ResultSet set) -> {
                    try {
                        final List<T> list = new ArrayList();
                        while (set != null && set.next()) {
                            list.add(getEntityValue(info, sels, set));
                        }
                        return Sheet.asSheet(list);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
        }
        final String countsql = "SELECT " + (distinct ? "DISTINCT COUNT(" + info.getQueryColumns("a", selects) + ")" : "COUNT(*)") + " FROM " + info.getTable(node) + " a" + (join == null ? "" : join)
            + ((where == null || where.length() == 0) ? "" : (" WHERE " + where));
        return getNumberResultDB(info, countsql, 0, countsql).thenCompose(total -> {
            if (total.longValue() <= 0) return CompletableFuture.completedFuture(new Sheet<>(0, new ArrayList()));
            CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
            return connFuture.thenCompose((conn) -> executeQuery(info, conn, listsql).thenApply((ResultSet set) -> {
                try {
                    final List<T> list = new ArrayList();
                    while (set.next()) {
                        list.add(getEntityValue(info, sels, set));
                    }
                    return new Sheet(total.longValue(), list);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        });
    }

    protected CompletableFuture<ResultSet> exceptionallyQueryTableNotExist(CompletableFuture<ResultSet> future, EntityInfo info) {
        if (info == null || info.getTableStrategy() == null) return future;
        return future.exceptionally(ex -> {
            Throwable sqlex = ex;
            while (sqlex instanceof CompletionException) sqlex = sqlex.getCause();
            if (info.getTableStrategy() != null && sqlex instanceof SQLException && info.isTableNotExist((SQLException) sqlex)) {
                return new MyResultSet(info, new MyColumnDescPacket[0], new ArrayList<>());
            } else {
                future.obtrudeException(sqlex);
                return null;
            }
        });
    }

    protected <T> CompletableFuture<Integer> exceptionallyUpdateTableNotExist(CompletableFuture<Integer> future,
        EntityInfo<T> info, final AsyncConnection conn, final UpdateMode mode, final ByteArray array, final T oneEntity, final byte[] sqlBytes) {
        final CompletableFuture<Integer> newFuture = new CompletableFuture<>();
        future.whenComplete((o, ex1) -> {
            if (ex1 == null) {
                newFuture.complete(o);
                return;
            }
            try {
                while (ex1 instanceof CompletionException) ex1 = ex1.getCause();
                if (info.getTableStrategy() != null && ex1 instanceof SQLException && info.isTableNotExist((SQLException) ex1)) {
                    if (mode != UpdateMode.INSERT) { //update、delete、clear或drop
                        newFuture.complete((mode == UpdateMode.DROP || mode == UpdateMode.CLEAR) ? -1 : 0);
                        return;
                    }
                    //分表分库
                    final String newTable = info.getTable(oneEntity);
                    final byte[] createTableSqlBytes = info.getTableCopySQL(newTable).getBytes(StandardCharsets.UTF_8);
                    executeAtomicOneUpdate(info, conn, array, createTableSqlBytes).whenComplete((o2, ex2) -> {
                        if (ex2 == null) { //建分表成功
                            info.addDisTable(newTable);
                            //重新执行一遍sql语句
                            executeAtomicOneUpdate(info, conn, array, sqlBytes).whenComplete((o3, ex3) -> {
                                if (ex3 == null) {
                                    newFuture.complete(o3);
                                } else {
                                    while (ex3 instanceof CompletionException) ex3 = ex3.getCause();
                                    newFuture.completeExceptionally(ex3);
                                }
                            });
                        } else {
                            while (ex2 instanceof CompletionException) ex2 = ex2.getCause();
                            if (newTable.indexOf('.') > 0 && ex2 instanceof SQLException
                                && ("HY000".equals(((SQLException) ex2).getSQLState()) || "42000".equals(((SQLException) ex2).getSQLState()))) { //可能是database不存在
                                executeAtomicOneUpdate(info, conn, array, ("CREATE DATABASE " + newTable.substring(0, newTable.indexOf('.'))).getBytes()).whenComplete((o3, ex3) -> {
                                    if (ex3 == null) { //建库成功
                                        executeAtomicOneUpdate(info, conn, array, createTableSqlBytes).whenComplete((o4, ex4) -> { //建表
                                            if (ex4 == null) { //建表成功
                                                info.addDisTable(newTable);
                                                //重新执行一遍sql语句
                                                executeAtomicOneUpdate(info, conn, array, sqlBytes).whenComplete((o5, ex5) -> {
                                                    if (ex5 == null) {
                                                        newFuture.complete(o5);
                                                    } else {
                                                        while (ex5 instanceof CompletionException) ex5 = ex5.getCause();
                                                        newFuture.completeExceptionally(ex5);
                                                    }
                                                });
                                            } else {
                                                while (ex4 instanceof CompletionException) ex4 = ex4.getCause();
                                                newFuture.completeExceptionally(ex4);
                                            }
                                        });
                                    } else {
                                        while (ex3 instanceof CompletionException) ex3 = ex3.getCause();
                                        newFuture.completeExceptionally(ex3);
                                    }
                                });
                            } else { //不是建库的问题
                                newFuture.completeExceptionally(ex2);
                            }
                        }
                    });
                } else {
                    newFuture.completeExceptionally(ex1);
                }
            } catch (Throwable t) {
                newFuture.completeExceptionally(t);
            }
        });
        return newFuture;
    }

    protected static <T> byte[] formatPrepareParam(EntityInfo<T> info, Attribute<T, Serializable> attr, Object param) {
        if (param == null && info.isNotNullJson(attr)) return new byte[0];
        if (param == null) return null;
        if (param instanceof CharSequence) {
            return param.toString().getBytes(StandardCharsets.UTF_8);
        }
        if (param instanceof Boolean) {
            return (Boolean) param ? new byte[]{0x31} : new byte[]{0x30};
        }
        if (param instanceof byte[]) {
            return (byte[]) param;
        }
        if (param instanceof java.sql.Blob) {
            java.sql.Blob blob = (java.sql.Blob) param;
            try {
                return blob.getBytes(1, (int) blob.length());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        if (!(param instanceof Number) && !(param instanceof CharSequence) && !(param instanceof java.util.Date)
            && !param.getClass().getName().startsWith("java.sql.") && !param.getClass().getName().startsWith("java.time.")) {
            if (attr == null) return info.getJsonConvert().convertTo(param).getBytes(StandardCharsets.UTF_8);
            return info.getJsonConvert().convertTo(attr.genericType(), param).getBytes(StandardCharsets.UTF_8);
        }
        return String.valueOf(param).getBytes(StandardCharsets.UTF_8);
    }

    protected <T> CompletableFuture<Integer> executeOneUpdate(final EntityInfo<T> info, final AsyncConnection conn, final UpdateMode mode, final byte[] sqlBytes) {
        return executeBatchUpdate(info, conn, mode, null, sqlBytes).thenApply(a -> a[0]);
    }

    protected <T> CompletableFuture<int[]> executeBatchUpdate(final EntityInfo<T> info, final AsyncConnection conn, final UpdateMode mode, final T oneEntity, final byte[]... sqlBytesArray) {
        final ByteArray array = conn.getSubobject();
        if (sqlBytesArray.length == 1) {
            return executeAtomicOneUpdate(info, conn, array, SQL_SET_AUTOCOMMIT_1).thenCompose(o
                -> mode == UpdateMode.INSERT ? exceptionallyUpdateTableNotExist(executeAtomicOneUpdate(info, conn, array, sqlBytesArray[0]), info, conn, mode, array, oneEntity, sqlBytesArray[0])
                    : executeAtomicOneUpdate(info, conn, array, sqlBytesArray[0])).thenApply(a -> new int[]{a}).whenComplete((o, t) -> {
                if (t == null) {
                    writePool.offerConnection(conn);
                } else {
                    conn.dispose();
                }
            });
        }
        //多个
        final int[] rs = new int[sqlBytesArray.length + 2];
        CompletableFuture<Integer> future = executeAtomicOneUpdate(info, conn, array, SQL_SET_AUTOCOMMIT_0);
        future.thenAccept((Integer a) -> rs[0] = a);
        for (int i = 0; i < sqlBytesArray.length; i++) {
            final int index = i + 1;
            final byte[] sqlBytes = sqlBytesArray[i];
            future = future.thenCompose(a -> {
                CompletableFuture<Integer> nextFuture = executeAtomicOneUpdate(info, conn, array, sqlBytes);
                nextFuture.thenAccept(b -> rs[index] = b);
                if (mode == UpdateMode.INSERT && info != null && info.getTableStrategy() != null) nextFuture = exceptionallyUpdateTableNotExist(nextFuture, info, conn, mode, array, oneEntity, sqlBytes);
                nextFuture.whenComplete((o, t) -> {
                    if (t != null) executeAtomicOneUpdate(info, conn, array, SQL_ROLLBACK).join();
                });
                return nextFuture;
            });
        }
        future = future.thenCompose(a -> {
            CompletableFuture<Integer> nextFuture = executeAtomicOneUpdate(info, conn, array, SQL_COMMIT);
            nextFuture.thenAccept(b -> rs[sqlBytesArray.length] = b);
            return nextFuture;
        });
        return future.thenApply(a -> Arrays.copyOfRange(rs, 1, sqlBytesArray.length + 1)).whenComplete((o, t) -> {
            if (t == null) {
                writePool.offerConnection(conn);
            } else {
                conn.dispose();
            }
        });
    }

    protected <T> CompletableFuture<Integer> executeAtomicOneUpdate(final EntityInfo<T> info, final AsyncConnection conn, final ByteArray array, final byte[] sqlBytes) {
        final ByteArray writer = conn.getSubobject();
        writer.clear();
        {
            new MyQueryPacket(sqlBytes).writeTo(writer);
        }
        final CompletableFuture<Integer> future = new CompletableFuture();
        conn.write(writer, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment1) {
                if (result < 0) {
                    failed(new SQLException("Write Buffer Error"), attachment1);
                    return;
                }

                final List<ByteBuffer> readBuffs = new ArrayList<>();
                conn.read(new CompletionHandler<Integer, ByteBuffer>() {
                    @Override
                    public void completed(Integer result, ByteBuffer attachment2) {
                        if (result < 0) {
                            failed(new SQLException("Read Buffer Error"), attachment2);
                            return;
                        }
                        if (result == 16 * 1024 || !attachment2.hasRemaining()) { //mysqlsql数据包上限为16*1024还有数据
                            attachment2.flip();
                            readBuffs.add(attachment2);
                            conn.read(this);
                            return;
                        }
                        attachment2.flip();
                        readBuffs.add(attachment2);
                        final ByteBufferReader bufferReader = ByteBufferReader.create(readBuffs);
                        MyOKPacket okPacket = new MyOKPacket(-1, bufferReader, array);
                        //System.out.println("执行sql=" + new String(sqlBytes, StandardCharsets.UTF_8) + ", 结果： " + okPacket);
                        if (!okPacket.isOK()) {
                            future.completeExceptionally(new SQLException(okPacket.toMessageString("MySQLOKPacket statusCode not success") + " sql=" + new String(sqlBytes, StandardCharsets.UTF_8), okPacket.sqlState, okPacket.vendorCode));
                            //不能关conn
                        } else {
                            for (ByteBuffer buf : readBuffs) {
                                conn.offerBuffer(buf);
                            }
                            future.complete((int) okPacket.updateCount);
                        }
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment2) {
                        conn.offerBuffer(attachment2);
                        future.completeExceptionally(exc);
                        //不能关conn
                    }
                });
            }

            @Override
            public void failed(Throwable exc, Void attachment1) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    //info可以为null,供directQuery
    protected <T> CompletableFuture<ResultSet> executeQuery(final EntityInfo<T> info, final AsyncConnection conn, final String sql) {
        final ByteArray writer = conn.getSubobject();
        writer.clear();
        {
            new MyQueryPacket(sql.getBytes(StandardCharsets.UTF_8)).writeTo(writer);
        }
        final CompletableFuture<ResultSet> future = new CompletableFuture();
        conn.write(writer, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment1) {
                if (result < 0) {
                    failed(new SQLException("Write Buffer Error"), attachment1);
                    return;
                }

                final List<ByteBuffer> readBuffs = new ArrayList<>();
                conn.read(new CompletionHandler<Integer, ByteBuffer>() {
                    @Override
                    public void completed(Integer result, ByteBuffer attachment2) {
                        if (result < 0) {
                            failed(new SQLException("Read Buffer Error"), attachment2);
                            return;
                        }
                        if (result == 16 * 1024 || !attachment2.hasRemaining()) { //mysqlsql数据包上限为16*1024还有数据
                            attachment2.flip();
                            readBuffs.add(attachment2);
                            conn.read(this);
                            return;
                        }
                        attachment2.flip();
                        readBuffs.add(attachment2);
                        final ByteBufferReader bufferReader = ByteBufferReader.create(readBuffs);
                        boolean endok = false;
                        boolean futureover = false;
                        boolean success = false;
                        SQLException ex = null;
                        int packetLength = Mysqls.readUB3(bufferReader);
                        MyResultSet resultSet = null;
                        if (packetLength < 4) {
                            MyColumnCountPacket countPacket = new MyColumnCountPacket(packetLength, bufferReader);
                            //System.out.println("查询sql=" + sql + ", 字段数： " + countPacket.columnCount);
                            //System.out.println("--------- column desc start  -------------");
                            MyColumnDescPacket[] colDescs = new MyColumnDescPacket[countPacket.columnCount];
                            for (int i = 0; i < colDescs.length; i++) {
                                colDescs[i] = new MyColumnDescPacket(bufferReader);
                            }
                            //读取EOF包
                            MyEOFPacket eofPacket = new MyEOFPacket(-1, -1000, bufferReader);
                            //System.out.println("字段描述EOF包： " + eofPacket);

                            List<MyRowDataPacket> rows = new ArrayList<>();
                            int colPacketLength = Mysqls.readUB3(bufferReader);
                            int packetIndex = bufferReader.get();
                            int typeid = bufferReader.preget() & 0xff;
                            while (typeid != Mysqls.TYPE_ID_EOF) { //EOF包
                                final MyRowDataPacket rowData = new MyRowDataPacket(colDescs, colPacketLength, packetIndex, bufferReader, countPacket.columnCount);
                                while (!rowData.readColumnValue(bufferReader) || bufferReader.remaining() < 3 + 6) {
                                    final CompletableFuture<ByteBuffer> patchFuture = new CompletableFuture<>();
                                    conn.read(new CompletionHandler<Integer, ByteBuffer>() {
                                        @Override
                                        public void completed(Integer result3, ByteBuffer attachment3) {
                                            if (result3 < 0) {
                                                failed(new SQLException("Read Buffer Error"), attachment3);
                                                return;
                                            }
                                            attachment3.flip();
                                            patchFuture.complete(attachment3);
                                        }

                                        @Override
                                        public void failed(Throwable exc, ByteBuffer attachment3) {
                                            patchFuture.completeExceptionally(exc);
                                        }
                                    });
                                    bufferReader.append(patchFuture.join());
                                }
                                colPacketLength = Mysqls.readUB3(bufferReader);
                                packetIndex = bufferReader.get();
                                typeid = bufferReader.preget() & 0xff;
                                rows.add(rowData);
                            }
                            eofPacket = new MyEOFPacket(colPacketLength, packetIndex, bufferReader);
                            //System.out.println("查询结果包解析完毕： " + eofPacket);

                            resultSet = new MyResultSet(info, colDescs, rows);
                            success = true;
                            endok = true;
                            futureover = true;
                        } else {
                            MyOKPacket okPacket = new MyOKPacket(packetLength, bufferReader, writer);
                            //System.out.println("查询sql=" + sql + ", 异常： " + okPacket);
                            ex = new SQLException(okPacket.toMessageString("MySQLOKPacket statusCode not success"), okPacket.sqlState, okPacket.vendorCode);
                        }

                        if (!futureover) future.completeExceptionally(ex == null ? new SQLException("SQL(" + sql + ") executeQuery error") : ex);
                        if (endok) {
                            readPool.offerConnection(conn);
                            future.complete(resultSet);
                        } else {
                            conn.dispose();
                        }
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment2) {
                        //不用bufferPool.accept
                        future.completeExceptionally(exc);
                        conn.dispose();
                    }
                });
            }

            @Override
            public void failed(Throwable exc, Void attachment1) {
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    @Local
    @Override
    public int directExecute(String sql) {
        CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
        return connFuture.thenCompose((conn) -> executeOneUpdate(null, conn, UpdateMode.OTHER, sql.getBytes(StandardCharsets.UTF_8))).join();
    }

    @Local
    @Override
    public int[] directExecute(String... sqls) {
        byte[][] sqlBytesArray = new byte[sqls.length][];
        for (int i = 0; i < sqls.length; i++) {
            sqlBytesArray[i] = sqls[i].getBytes(StandardCharsets.UTF_8);

        }
        CompletableFuture<AsyncConnection> connFuture = writePool.pollAsync();
        return connFuture.thenCompose((conn) -> executeBatchUpdate(null, conn, UpdateMode.OTHER, null, sqlBytesArray)).join();
    }

    @Local
    @Override
    public <V> V directQuery(String sql, Function<ResultSet, V> handler) {
        CompletableFuture<AsyncConnection> connFuture = readPool.pollAsync();
        return connFuture.thenCompose((conn) -> executeQuery(null, conn, sql).thenApply((ResultSet set) -> {
            return handler.apply(set);
        })).join();
    }

}
