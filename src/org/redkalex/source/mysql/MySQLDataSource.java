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
import static java.nio.charset.StandardCharsets.UTF_8;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.logging.*;
import org.redkale.net.AsyncConnection;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;
import static org.redkalex.source.mysql.MyPoolSource.CONN_ATTR_BYTESBAME;

/**
 * 尚未实现
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(DataSource.class)
public class MySQLDataSource extends DataSqlSource<AsyncConnection> {

    public static void main(String[] args) throws Throwable {
        final Logger logger = Logger.getLogger(MySQLDataSource.class.getSimpleName());
        final int capacity = 16 * 1024;
        final ObjectPool<ByteBuffer> bufferPool = new ObjectPool<>(new AtomicLong(), new AtomicLong(), 16,
            (Object... params) -> ByteBuffer.allocateDirect(capacity), null, (e) -> {
                if (e == null || e.isReadOnly() || e.capacity() != capacity) return false;
                e.clear();
                return true;
            });

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4, (Runnable r) -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        Properties prop = new Properties();
        prop.setProperty(DataSources.JDBC_URL, "jdbc:mysql://localhost:3306/platf_core?characterEncoding=utf8");
        prop.setProperty(DataSources.JDBC_USER, "root");
        prop.setProperty(DataSources.JDBC_PWD, "");
        MySQLDataSource source = new MySQLDataSource("", null, prop, prop);
        source.getReadPoolSource().poll();
        source.directExecute("SET NAMES utf8");
        source.directExecute("UPDATE almsrecord SET createtime = 0");
    }

    public MySQLDataSource(String unitName, URL persistxml, Properties readprop, Properties writeprop) {
        super(unitName, persistxml, readprop, writeprop);
    }

    @Local
    protected PoolSource<AsyncConnection> readPoolSource() {
        return readPool;
    }

    @Local
    protected PoolSource<AsyncConnection> writePoolSource() {
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
    protected PoolSource<AsyncConnection> createPoolSource(DataSource source, String rwtype, ArrayBlockingQueue queue, Semaphore semaphore, Properties prop) {
        return new MyPoolSource(rwtype, queue, semaphore, prop, logger, bufferPool, executor);
    }

    @Override
    protected <T> CompletableFuture<Integer> insertDB(EntityInfo<T> info, T... values) {
        final Attribute<T, Serializable>[] attrs = info.getInsertAttributes();
        final Object[][] objs = new Object[values.length][];
        for (int i = 0; i < values.length; i++) {
            final Object[] params = new Object[attrs.length];
            for (int j = 0; j < attrs.length; j++) {
                params[j] = attrs[j].get(values[i]);
            }
            objs[i] = params;
        }
        String sql0 = info.getInsertDollarPrepareSQL(values[0]);
        final String sql = sql0;
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(info, conn, sql, values, true, objs));
    }

    @Override
    protected <T> CompletableFuture<Integer> deleteDB(EntityInfo<T> info, Flipper flipper, String sql) {
        final String realsql = flipper == null || flipper.getLimit() <= 0 ? sql : (sql + " LIMIT " + flipper.getLimit());
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, realsql)) logger.finest(info.getType().getSimpleName() + " delete sql=" + realsql);
        }
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(info, conn, realsql, null, false));
    }

    @Override
    protected <T> CompletableFuture<Integer> clearTableDB(EntityInfo<T> info, String sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " clearTable sql=" + sql);
        }
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(info, conn, sql, null, false));
    }

    @Override
    protected <T> CompletableFuture<Integer> dropTableDB(EntityInfo<T> info, String sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " dropTable sql=" + sql);
        }
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(info, conn, sql, null, false));
    }

    @Override
    protected <T> CompletableFuture<Integer> updateDB(EntityInfo<T> info, final T... values) {
        final Attribute<T, Serializable> primary = info.getPrimary();
        final Attribute<T, Serializable>[] attrs = info.getUpdateAttributes();
        final Object[][] objs = new Object[values.length][];
        for (int i = 0; i < values.length; i++) {
            final Object[] params = new Object[attrs.length + 1];
            for (int j = 0; j < attrs.length; j++) {
                params[j] = attrs[j].get(values[i]);
            }
            params[attrs.length] = primary.get(values[i]); //最后一个是主键
            objs[i] = params;
        }
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(info, conn, info.getUpdateDollarPrepareSQL(values[0]), null, false, objs));
    }

    @Override
    protected <T> CompletableFuture<Integer> updateDB(EntityInfo<T> info, Flipper flipper, String sql, boolean prepared, Object... params) {
        final String realsql = flipper == null || flipper.getLimit() <= 0 ? sql : (sql + " LIMIT " + flipper.getLimit());
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, realsql)) logger.finest(info.getType().getSimpleName() + " update sql=" + realsql);
        }
        Object[][] objs = params == null || params.length == 0 ? null : new Object[][]{params};
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(info, conn, realsql, null, false, objs));
    }

    @Override
    protected <T, N extends Number> CompletableFuture<Map<String, N>> getNumberMapDB(EntityInfo<T> info, String sql, FilterFuncColumn... columns) {
        return readPool.pollAsync().thenCompose((conn) -> executeQuery(info, conn, sql).thenApply((ResultSet set) -> {
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
        return readPool.pollAsync().thenCompose((conn) -> executeQuery(info, conn, sql).thenApply((ResultSet set) -> {
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
        return readPool.pollAsync().thenCompose((conn) -> executeQuery(info, conn, sql).thenApply((ResultSet set) -> {
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
    protected <T> CompletableFuture<T> findDB(EntityInfo<T> info, String sql, boolean onlypk, SelectColumn selects) {
        return readPool.pollAsync().thenCompose((conn) -> executeQuery(info, conn, sql).thenApply((ResultSet set) -> {
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
        return readPool.pollAsync().thenCompose((conn) -> executeQuery(info, conn, sql).thenApply((ResultSet set) -> {
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
        return readPool.pollAsync().thenCompose((conn) -> executeQuery(info, conn, sql).thenApply((ResultSet set) -> {
            try {
                boolean rs = set.next() ? (set.getInt(1) > 0) : false;
                return rs;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    protected <T> CompletableFuture<Sheet<T>> querySheetDB(EntityInfo<T> info, final boolean readcache, boolean needtotal, SelectColumn selects, Flipper flipper, FilterNode node) {
        final SelectColumn sels = selects;
        final Map<Class, String> joinTabalis = node == null ? null : getJoinTabalis(node);
        final CharSequence join = node == null ? null : createSQLJoin(node, this, false, joinTabalis, new HashSet<>(), info);
        final CharSequence where = node == null ? null : createSQLExpress(node, info, joinTabalis);
        final String listsql = "SELECT " + info.getQueryColumns("a", selects) + " FROM " + info.getTable(node) + " a" + (join == null ? "" : join)
            + ((where == null || where.length() == 0) ? "" : (" WHERE " + where)) + createSQLOrderby(info, flipper) + (flipper == null || flipper.getLimit() < 1 ? "" : (" LIMIT " + flipper.getLimit() + " OFFSET " + flipper.getOffset()));
        if (readcache && info.isLoggable(logger, Level.FINEST, listsql)) logger.finest(info.getType().getSimpleName() + " query sql=" + listsql);
        if (!needtotal) {
            return readPool.pollAsync().thenCompose((conn) -> executeQuery(info, conn, listsql).thenApply((ResultSet set) -> {
                try {
                    final List<T> list = new ArrayList();
                    while (set.next()) {
                        list.add(getEntityValue(info, sels, set));
                    }
                    return Sheet.asSheet(list);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        final String countsql = "SELECT COUNT(*) FROM " + info.getTable(node) + " a" + (join == null ? "" : join)
            + ((where == null || where.length() == 0) ? "" : (" WHERE " + where));
        return getNumberResultDB(info, countsql, 0, countsql).thenCompose(total -> {
            if (total.longValue() <= 0) return CompletableFuture.completedFuture(new Sheet<>(0, new ArrayList()));
            return readPool.pollAsync().thenCompose((conn) -> executeQuery(info, conn, listsql).thenApply((ResultSet set) -> {
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

    protected static byte[] formatPrepareParam(Object param) {
        if (param == null) return null;
        if (param instanceof byte[]) return (byte[]) param;
        return String.valueOf(param).getBytes(UTF_8);
    }

    protected <T> CompletableFuture<Integer> executeUpdate(final EntityInfo<T> info, final AsyncConnection conn, final String sql, final T[] values, final boolean insert2, final Object[]... parameters) {
        return executeUpdate(info, conn, sql).thenApply(a -> a[0]);
    }

    protected <T> CompletableFuture<int[]> executeUpdate(final EntityInfo<T> info, final AsyncConnection conn, final String... sqls) {
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        final int[] rs = new int[sqls.length];
        CompletableFuture<Integer> future = executeOneUpdate(info, conn, bytes, sqls[0]);
        future.thenAccept((Integer a) -> {
            rs[0] = a;
            System.out.println("i = " + 0 + ", update = " + a + ", sql = " + sqls[0]);
        });
        if (sqls.length == 1) {
            return future.thenApply(a -> rs).whenComplete((o, t) -> {
                if (t != null) writePool.offerConnection(conn);
            });
        }
        for (int i = 1; i < sqls.length; i++) {
            final int index = i;
            final String sql = sqls[i];
            future = future.thenCompose(a -> {
                CompletableFuture<Integer> nextFuture = executeOneUpdate(info, conn, bytes, sql);
                nextFuture.thenAccept(b -> {
                    rs[index] = b;
                    System.out.println("i = " + index + ", update = " + b + ", sql = " + sqls[index]);
                });
                return nextFuture;
            });
        }
        return future.thenApply(a -> rs).whenComplete((o, t) -> {
            if (t != null) writePool.offerConnection(conn);
        });
    }

    protected <T> CompletableFuture<Integer> executeOneUpdate(final EntityInfo<T> info, final AsyncConnection conn, final byte[] bytes, final String sql) {
        final ByteBufferWriter writer = ByteBufferWriter.create(bufferPool);
        {
            new MySQLQueryPacket(sql).writeTo(writer);
        }
        final ByteBuffer[] buffers = writer.toBuffers();
        final CompletableFuture<Integer> future = new CompletableFuture();
        conn.write(buffers, buffers, new CompletionHandler<Integer, ByteBuffer[]>() {
            @Override
            public void completed(Integer result, ByteBuffer[] attachment1) {
                if (result < 0) {
                    failed(new SQLException("Write Buffer Error"), attachment1);
                    return;
                }
                int index = -1;
                for (int i = 0; i < attachment1.length; i++) {
                    if (attachment1[i].hasRemaining()) {
                        index = i;
                        break;
                    }
                    bufferPool.accept(attachment1[i]);
                }
                if (index == 0) {
                    conn.write(attachment1, attachment1, this);
                    return;
                } else if (index > 0) {
                    ByteBuffer[] newattachs = new ByteBuffer[attachment1.length - index];
                    System.arraycopy(attachment1, index, newattachs, 0, newattachs.length);
                    conn.write(newattachs, newattachs, this);
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
                        final ByteBufferReader buffer = ByteBufferReader.create(readBuffs);
                        MySQLOKPacket okPacket = new MySQLOKPacket(-1, buffer, bytes);
                        System.out.println("执行sql=" + sql + ", 结果： " + okPacket);
                        if (!okPacket.isOK()) {
                            future.completeExceptionally(new SQLException(okPacket.toMessageString("MySQLOKPacket statusCode not success"), okPacket.sqlState, okPacket.vendorCode));
                            conn.dispose();
                        } else {
                            for (ByteBuffer buf : readBuffs) {
                                bufferPool.accept(buf);
                            }
                            future.complete((int) okPacket.updateCount);
                        }
                    }

                    @Override
                    public void failed(Throwable exc, ByteBuffer attachment2) {
                        conn.offerBuffer(attachment2);
                        future.completeExceptionally(exc);
                        conn.dispose();
                    }
                });
            }

            @Override
            public void failed(Throwable exc, ByteBuffer[] attachment1) {
                for (ByteBuffer attach : attachment1) {
                    bufferPool.accept(attach);
                }
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    //info可以为null,供directQuery
    protected <T> CompletableFuture<ResultSet> executeQuery(final EntityInfo<T> info, final AsyncConnection conn, final String sql) {
        final byte[] bytes = conn.getAttribute(CONN_ATTR_BYTESBAME);
        final ByteBufferWriter writer = ByteBufferWriter.create(bufferPool);
        {
            new MySQLQueryPacket(sql).writeTo(writer);
        }
        final ByteBuffer[] buffers = writer.toBuffers();
        final CompletableFuture<ResultSet> future = new CompletableFuture();
        conn.write(buffers, buffers, new CompletionHandler<Integer, ByteBuffer[]>() {
            @Override
            public void completed(Integer result, ByteBuffer[] attachment1) {
                if (result < 0) {
                    failed(new SQLException("Write Buffer Error"), attachment1);
                    return;
                }
                int index = -1;
                for (int i = 0; i < attachment1.length; i++) {
                    if (attachment1[i].hasRemaining()) {
                        index = i;
                        break;
                    }
                    bufferPool.accept(attachment1[i]);
                }
                if (index == 0) {
                    conn.write(attachment1, attachment1, this);
                    return;
                } else if (index > 0) {
                    ByteBuffer[] newattachs = new ByteBuffer[attachment1.length - index];
                    System.arraycopy(attachment1, index, newattachs, 0, newattachs.length);
                    conn.write(newattachs, newattachs, this);
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
                        final ByteBufferReader buffer = ByteBufferReader.create(readBuffs);
                        boolean endok = false;
                        boolean futureover = false;
                        boolean success = false;
                        SQLException ex = null;
                        int packetLength = MySQLs.readUB3(buffer);
                        MyResultSet resultSet = null;
                        if (packetLength < 4) {
                            MySQLColumnCountPacket countPacket = new MySQLColumnCountPacket(packetLength, buffer, bytes);
                            System.out.println("查询sql=" + sql + ", 字段数： " + countPacket.columnCount);
                            System.out.println("--------- column desc start  -------------");
                            MySQLColumnDescPacket[] colDescs = new MySQLColumnDescPacket[countPacket.columnCount];
                            for (int i = 0; i < colDescs.length; i++) {
                                colDescs[i] = new MySQLColumnDescPacket(buffer, bytes);
                            }
                            MySQLEOFPacket eofPacket = new MySQLEOFPacket(-1, buffer, bytes);
                            System.out.println("字段描述EOF包： " + eofPacket);

                            List<MySQLRowDataPacket> rows = new ArrayList<>();
                            int colPacketLength = MySQLs.readUB3(buffer);
                            while (colPacketLength != 5) { //EOF包
                                MySQLRowDataPacket rowData = new MySQLRowDataPacket(colDescs, colPacketLength, buffer, countPacket.columnCount, bytes);
                                rows.add(rowData);
                                colPacketLength = MySQLs.readUB3(buffer);
                                System.out.println("行记录: " + rowData);
                            }
                            eofPacket = new MySQLEOFPacket(colPacketLength, buffer, bytes);
                            System.out.println("查询结果包解析完毕： " + eofPacket);

                            resultSet = new MyResultSet(colDescs, rows);
                            success = true;
                            endok = true;
                            futureover = true;
                        } else {
                            MySQLOKPacket okPacket = new MySQLOKPacket(packetLength, buffer, bytes);
                            System.out.println("查询sql=" + sql + ", 异常： " + okPacket);
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
            public void failed(Throwable exc, ByteBuffer[] attachment1) {
                for (ByteBuffer attach : attachment1) {
                    bufferPool.accept(attach);
                }
                future.completeExceptionally(exc);
            }
        });
        return future;
    }

    @Local
    @Override
    public int directExecute(String sql) {
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(null, conn, sql, null, false)).join();
    }

    @Local
    @Override
    public int[] directExecute(String... sqls) {
        return writePool.pollAsync().thenCompose((conn) -> executeUpdate(null, conn, sqls)).join();
    }

    @Local
    @Override
    public <V> V directQuery(String sql, Function<ResultSet, V> handler) {
        return readPool.pollAsync().thenCompose((conn) -> executeQuery(null, conn, sql).thenApply((ResultSet set) -> {
            return handler.apply(set);
        })).join();
    }

}
