/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceType;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 * 部分协议格式参考： http://wp1i.cn/archives/78556.html
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(DataSource.class)
public class MysqlDataSource extends DataSqlSource {

    static final boolean debug = false; //System.getProperty("os.name").contains("Window") || System.getProperty("os.name").contains("Mac");

    protected MyClient readPool;

    protected AsyncGroup readGroup;

    protected MyClient writePool;

    protected AsyncGroup writeGroup;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        this.dbtype = "mysql";
        this.readPool = createMyPool((readConfProps == writeConfProps) ? "rw" : "read", readConfProps);
        if (readConfProps == writeConfProps) {
            this.writePool = readPool;
            this.writeGroup = this.readGroup;
        } else {
            this.writePool = createMyPool("write", writeConfProps);
        }
    }

    private MyClient createMyPool(String rw, Properties prop) {
        String url = prop.getProperty(DATA_SOURCE_URL);
        SourceUrlInfo info = parseSourceUrl(url);
        info.username = prop.getProperty(DATA_SOURCE_USER, "");
        info.password = prop.getProperty(DATA_SOURCE_PASSWORD, "");
        String encoding = prop.getProperty("characterEncoding");
        if (encoding == null || encoding.isEmpty()) encoding = "UTF8MB4";
        info.encoding = encoding;
        int maxConns = Math.max(1, Integer.decode(prop.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
        int maxPipelines = Math.max(1, Integer.decode(prop.getProperty(DATA_SOURCE_PIPELINES, "" + org.redkale.net.client.Client.DEFAULT_MAX_PIPELINES)));
        AsyncGroup ioGroup = clientAsyncGroup;
        if (clientAsyncGroup == null || "write".equalsIgnoreCase(rw)) {
            ioGroup = AsyncGroup.create("Redkalex-MyClient-IOThread-" + rw.toUpperCase(), workExecutor, 16 * 1024, Utility.cpus() * 4).start();
        }
        return new MyClient(ioGroup, resourceName() + "." + rw, new ClientAddress(info.servaddr), maxConns, maxPipelines, prop, info, autoddl(), info.attributes);
    }

    @Override
    protected void updateOneResourceChange(Properties newProps, ResourceEvent[] events) {
        MyClient oldPool = this.readPool;
        AsyncGroup oldGroup = this.readGroup;
        this.readPool = createMyPool("rw", newProps);
        this.writePool = readPool;
        this.writeGroup = this.readGroup;
        if (oldPool != null) oldPool.close();
        if (oldGroup != null && oldGroup != clientAsyncGroup) oldGroup.close();
    }

    @Override
    protected void updateReadResourceChange(Properties newReadProps, ResourceEvent[] events) {
        MyClient oldPool = this.readPool;
        AsyncGroup oldGroup = this.readGroup;
        this.readPool = createMyPool("read", newReadProps);
        if (oldPool != null) oldPool.close();
        if (oldGroup != null && oldGroup != clientAsyncGroup) oldGroup.close();
    }

    @Override
    protected void updateWriteResourceChange(Properties newWriteProps, ResourceEvent[] events) {
        MyClient oldPool = this.writePool;
        AsyncGroup oldGroup = this.writeGroup;
        this.writePool = createMyPool("write", newWriteProps);
        if (oldPool != null) oldPool.close();
        if (oldGroup != null && oldGroup != clientAsyncGroup) oldGroup.close();
    }

    @Override
    public void destroy(AnyValue config) {
        if (readPool != null) {
            readPool.close();
        }
        if (readGroup != null && readGroup != clientAsyncGroup) {
            readGroup.close();
        }
        if (writePool != null && writePool != readPool) {
            writePool.close();
        }
        if (writeGroup != null && writeGroup != clientAsyncGroup && writeGroup != readGroup) {
            readGroup.close();
        }
    }

    @Local
    @Override
    public void close() throws Exception {
        super.close();
        if (readPool != null) {
            readPool.close();
        }
        if (readGroup != null && readGroup != clientAsyncGroup) {
            readGroup.close();
        }
        if (writePool != null && writePool != readPool) {
            writePool.close();
        }
        if (writeGroup != null && writeGroup != clientAsyncGroup && writeGroup != readGroup) {
            readGroup.close();
        }
    }

    @Local
    @Override
    public final <T> EntityInfo<T> loadEntityInfo(Class<T> clazz) {
        return super.loadEntityInfo(clazz);
    }

    @Local
    protected MyClient readPool() {
        return readPool;
    }

    @Local
    protected MyClient writePool() {
        return writePool;
    }

    @Override
    protected String prepareParamSign(int index) {
        return "?"; //不能带index
    }

    @Override
    protected final boolean isAsync() {
        return true;
    }

    @Override
    protected <T> CompletableFuture<Integer> insertDB(EntityInfo<T> info, T... values) {
        final long s = System.currentTimeMillis();
        final Attribute<T, Serializable>[] attrs = info.getInsertAttributes();
        final Object[][] objs = new Object[values.length][];
        for (int i = 0; i < values.length; i++) {
            final Object[] params = new Object[attrs.length];
            for (int j = 0; j < attrs.length; j++) {
                params[j] = getEntityAttrValue(info, attrs[j], values[i]);
            }
            objs[i] = params;
        }
        //if (info.isAutoGenerated()) sql += " RETURNING " + info.getPrimarySQLColumn();
        MyClient pool = writePool();
        if (pool.cachePreparedStatements()) {
            String sql = info.getInsertQuestionPrepareSQL(values[0]);
            WorkThread workThread = WorkThread.currWorkThread();
            AtomicReference<MyClientRequest> reqRef = new AtomicReference();
            AtomicReference<ClientConnection> connRef = new AtomicReference();
            return thenApplyInsertStrategy(info, pool.connect(null).thenCompose(conn -> {
                MyReqExtended req = ((MyClientConnection) conn).pollReqExtended(workThread, info);
                req.prepare(MyClientRequest.REQ_TYPE_EXTEND_INSERT, sql, 0, attrs, objs);
                reqRef.set(req);
                connRef.set(conn);
                return pool.writeChannel(conn, req);
            }), reqRef, connRef, values).thenApply(g -> {
                slowLog(s, sql);
                return g.getUpdateEffectCount();
            });
        } else {
            return executeUpdate(info, info.getInsertQuestionPrepareSQL(values[0]), values, 0, MyClientRequest.REQ_TYPE_EXTEND_INSERT, attrs, objs);
        }
    }

    @Override
    protected <T> CompletableFuture<Integer> deleteDB(EntityInfo<T> info, Flipper flipper, String sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " delete sql=" + sql);
        }
        return executeUpdate(info, sql, null, fetchSize(flipper), 0, null);
    }

    @Override
    protected <T> CompletableFuture<Integer> clearTableDB(EntityInfo<T> info, final String table, String sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " clearTable sql=" + sql);
        }
        return executeUpdate(info, sql, null, 0, 0, null);
    }

    @Override
    protected <T> CompletableFuture<Integer> dropTableDB(EntityInfo<T> info, final String table, String sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " dropTable sql=" + sql);
        }
        return executeUpdate(info, sql, null, 0, 0, null);
    }

    @Override
    protected <T> CompletableFuture<Integer> updateEntityDB(EntityInfo<T> info, final T... values) {
        final long s = System.currentTimeMillis();
        final Attribute<T, Serializable> primary = info.getPrimary();
        final Attribute<T, Serializable>[] attrs = info.getUpdateAttributes();
        MyClient pool = writePool();
        final String casesql = null;// pool.cachePreparedStatements() ? info.getUpdateQuestionPrepareCaseSQL(values) : null; //case会导致executed queries数量不对
        Object[][] objs0;
        if (casesql == null) {
            objs0 = new Object[values.length][];
            for (int i = 0; i < values.length; i++) {
                final Object[] params = new Object[attrs.length + 1];
                for (int j = 0; j < attrs.length; j++) {
                    params[j] = getEntityAttrValue(info, attrs[j], values[i]);
                }
                params[attrs.length] = primary.get(values[i]); //最后一个是主键
                objs0[i] = params;
            }
        } else {
            int len = values.length;
            int len2 = len * 2;
            objs0 = new Object[1][];
            Object[] params = new Object[len * 3];
            Attribute<T, Serializable> otherAttr = attrs[0];
            for (int i = 0; i < len2; i += 2) {
                int j = i / 2;
                params[i] = primary.get(values[j]);
                params[i + 1] = getEntityAttrValue(info, otherAttr, values[j]);
                params[j + len2] = params[i];
            }
            objs0[0] = params;
        }
        final Object[][] objs = objs0;
        if (pool.cachePreparedStatements()) {
            String sql = casesql == null ? info.getUpdateQuestionPrepareSQL(values[0]) : casesql;
            WorkThread workThread = WorkThread.currWorkThread();
            AtomicReference<ClientConnection> connRef = new AtomicReference();
            return thenApplyQueryUpdateStrategy(info, connRef, pool.connect(null).thenCompose(conn -> {
                connRef.set(conn);
                MyReqExtended req = ((MyClientConnection) conn).pollReqExtended(workThread, info);
                req.prepare(MyClientRequest.REQ_TYPE_EXTEND_UPDATE, sql, 0, casesql == null ? Utility.append(attrs, primary) : null, objs);
                return pool.writeChannel(conn, req);
            })).thenApply(g -> {
                slowLog(s, sql);
                return g.getUpdateEffectCount();
            });
        } else {
            return executeUpdate(info, info.getUpdateQuestionPrepareSQL(values[0]), null, 0, MyClientRequest.REQ_TYPE_EXTEND_UPDATE, Utility.append(attrs, primary), objs);
        }
    }

    @Override
    protected <T> CompletableFuture<Integer> updateColumnDB(EntityInfo<T> info, Flipper flipper, String sql, boolean prepared, Object... params) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " update sql=" + sql);
        }
        Object[][] objs = params == null || params.length == 0 ? null : new Object[][]{params};
        return executeUpdate(info, sql, null, fetchSize(flipper), prepared ? MyClientRequest.REQ_TYPE_EXTEND_UPDATE : 0, null, objs);
    }

    @Override
    protected <T, N extends Number> CompletableFuture<Map<String, N>> getNumberMapDB(EntityInfo<T> info, String sql, FilterFuncColumn... columns) {
        return getNumberMapDBApply(info, executeQuery(info, sql), columns);
    }

    @Override
    protected <T> CompletableFuture<Number> getNumberResultDB(EntityInfo<T> info, String sql, Number defVal, String column) {
        return getNumberResultDBApply(info, executeQuery(info, sql), defVal, column);
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N>> queryColumnMapDB(EntityInfo<T> info, String sql, String keyColumn) {
        return queryColumnMapDBApply(info, executeQuery(info, sql), keyColumn);
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K[], N[]>> queryColumnMapDB(EntityInfo<T> info, String sql, final ColumnNode[] funcNodes, final String[] groupByColumns) {
        return queryColumnMapDBApply(info, executeQuery(info, sql), funcNodes, groupByColumns);
    }

    @Override
    protected <T> CompletableFuture<T> findCompose(final EntityInfo<T> info, final SelectColumn selects, Serializable pk) {
        final long s = System.currentTimeMillis();
        MyClient pool = readPool();
        if (info.getTableStrategy() == null && selects == null && pool.cachePreparedStatements()) {
            String sql = info.getFindQuestionPrepareSQL(pk);
            WorkThread workThread = WorkThread.currWorkThread();
            AtomicReference<ClientConnection> connRef = new AtomicReference();
            return thenApplyQueryUpdateStrategy(info, connRef, pool.connect(null).thenCompose(conn -> {
                connRef.set(conn);
                MyReqExtended req = ((MyClientConnection) conn).pollReqExtended(workThread, info);
                req.prepare(MyClientRequest.REQ_TYPE_EXTEND_QUERY, sql, 0, null, new Object[]{pk});
                return pool.writeChannel(conn, req);
            })).thenApply((MyResultSet dataset) -> {
                T rs = dataset.next() ? getEntityValue(info, selects, dataset) : null;
                dataset.close();
                slowLog(s, sql);
                return rs;
            });
        }
        String column = info.getPrimarySQLColumn();
        final String sql = "SELECT " + info.getFullQueryColumns(null, selects) + " FROM " + info.getTable(pk) + " WHERE " + column + "=" + info.formatSQLValue(column, pk, sqlFormatter);
        if (info.isLoggable(logger, Level.FINEST, sql)) logger.finest(info.getType().getSimpleName() + " find sql=" + sql);
        return findDB(info, sql, true, selects);
    }

    @Override
    protected <T> CompletableFuture<T[]> findsComposeAsync(final EntityInfo<T> info, final SelectColumn selects, Serializable... pks) {
        final long s = System.currentTimeMillis();
        MyClient pool = readPool();
        if (info.getTableStrategy() == null && selects == null && pool.cachePreparedStatements()) {
            String sql = info.getFindQuestionPrepareSQL(pks[0]);
            WorkThread workThread = WorkThread.currWorkThread();
            AtomicReference<ClientConnection> connRef = new AtomicReference();
            return thenApplyQueryUpdateStrategy(info, connRef, pool.connect(null).thenCompose(conn -> {
                connRef.set(conn);
                MyReqExtended req = ((MyClientConnection) conn).pollReqExtended(workThread, info);
                Object[][] params = new Object[pks.length][];
                for (int i = 0; i < params.length; i++) {
                    params[i] = new Object[]{pks[i]};
                }
                req.prepare(MyClientRequest.REQ_TYPE_EXTEND_QUERY, sql, 0, null, params);
                req.finds = true;
                return pool.writeChannel(conn, req);
            })).thenApply((MyResultSet dataset) -> {
                T[] rs = info.getArrayer().apply(pks.length);
                int i = -1;
                while (dataset.next()) {
                    rs[++i] = getEntityValue(info, selects, dataset);
                }
                dataset.close();
                slowLog(s, sql);
                return rs;
            });
        } else {
            return super.findsComposeAsync(info, selects, pks);
        }
    }

    @Override
    public <D extends Serializable, T> CompletableFuture<List<T>> findsListAsync(final Class<T> clazz, final java.util.stream.Stream<D> pks) {
        final long s = System.currentTimeMillis();
        final EntityInfo<T> info = loadEntityInfo(clazz);
        Serializable[] ids = pks.toArray(v -> new Serializable[v]);
        MyClient pool = readPool();
        if (info.getTableStrategy() == null && pool.cachePreparedStatements()) {
            String sql = info.getFindQuestionPrepareSQL(ids[0]);
            WorkThread workThread = WorkThread.currWorkThread();
            AtomicReference<ClientConnection> connRef = new AtomicReference();
            return thenApplyQueryUpdateStrategy(info, connRef, pool.connect(null).thenCompose(conn -> {
                connRef.set(conn);
                MyReqExtended req = ((MyClientConnection) conn).pollReqExtended(workThread, info);
                Object[][] params = new Object[ids.length][];
                for (int i = 0; i < params.length; i++) {
                    params[i] = new Object[]{ids[i]};
                }
                req.prepare(MyClientRequest.REQ_TYPE_EXTEND_QUERY, sql, 0, null, params);
                req.finds = true;
                return pool.writeChannel(conn, req);
            })).thenApply((MyResultSet dataset) -> {
                List<T> rs = new ArrayList<>();
                while (dataset.next()) {
                    rs.add(getEntityValue(info, null, dataset));
                }
                dataset.close();
                slowLog(s, sql);
                return rs;
            });
        } else {
            return queryListAsync(info.getType(), (SelectColumn) null, (Flipper) null, FilterNode.create(info.getPrimarySQLColumn(), FilterExpress.IN, ids));
        }
    }

    @Override
    protected <T> CompletableFuture<T> findDB(EntityInfo<T> info, String sql, boolean onlypk, SelectColumn selects) {
        return findDBApply(info, executeQuery(info, sql), onlypk, selects);
    }

    @Override
    protected <T> CompletableFuture<Serializable> findColumnDB(EntityInfo<T> info, String sql, boolean onlypk, String column, Serializable defValue) {
        return findColumnDBApply(info, executeQuery(info, sql), onlypk, column, defValue);
    }

    @Override
    protected <T> CompletableFuture<Boolean> existsDB(EntityInfo<T> info, String sql, boolean onlypk) {
        return existsDBApply(info, executeQuery(info, sql), onlypk);
    }

    @Override
    protected <T> CompletableFuture<Sheet<T>> querySheetDB(EntityInfo<T> info, final boolean readcache, boolean needtotal, final boolean distinct, SelectColumn selects, Flipper flipper, FilterNode node) {
        final long s = System.currentTimeMillis();
        final SelectColumn sels = selects;
        final Map<Class, String> joinTabalis = node == null ? null : getJoinTabalis(node);
        final CharSequence join = node == null ? null : createSQLJoin(node, this, false, joinTabalis, new HashSet<>(), info);
        final CharSequence where = node == null ? null : createSQLExpress(node, info, joinTabalis);
        final MyClient pool = readPool();
        final boolean queryallcacheflag = pool.cachePreparedStatements() && readcache && info.getTableStrategy() == null && sels == null && node == null && flipper == null && !distinct;
        final String listsql = queryallcacheflag ? info.getAllQueryPrepareSQL() : ("SELECT " + (distinct ? "DISTINCT " : "") + info.getQueryColumns("a", selects) + " FROM " + info.getTable(node) + " a" + (join == null ? "" : join)
            + ((where == null || where.length() == 0) ? "" : (" WHERE " + where)) + createSQLOrderby(info, flipper) + (flipper == null || flipper.getLimit() < 1 ? "" : (" LIMIT " + flipper.getLimit() + " OFFSET " + flipper.getOffset())));
        if (readcache && info.isLoggable(logger, Level.FINEST, listsql)) logger.finest(info.getType().getSimpleName() + " query sql=" + listsql);
        if (!needtotal) {
            CompletableFuture<MyResultSet> listfuture;
            if (queryallcacheflag) {
                WorkThread workThread = WorkThread.currWorkThread();
                AtomicReference<ClientConnection> connRef = new AtomicReference();
                listfuture = thenApplyQueryUpdateStrategy(info, connRef, pool.connect(null).thenCompose(conn -> {
                    connRef.set(conn);
                    MyReqExtended req = ((MyClientConnection) conn).pollReqExtended(workThread, info);
                    req.prepare(MyClientRequest.REQ_TYPE_EXTEND_QUERY, listsql, 0, null);
                    return pool.writeChannel(conn, req);
                }));
            } else {
                listfuture = executeQuery(info, listsql);
            }
            return listfuture.thenApply((MyResultSet dataset) -> {
                final List<T> list = new ArrayList();
                while (dataset.next()) {
                    list.add(getEntityValue(info, sels, dataset));
                }
                dataset.close();
                slowLog(s, listsql);
                return Sheet.asSheet(list);
            });
        }
        final String countsql = "SELECT " + (distinct ? "DISTINCT COUNT(" + info.getQueryColumns("a", selects) + ")" : "COUNT(*)") + " FROM " + info.getTable(node) + " a" + (join == null ? "" : join)
            + ((where == null || where.length() == 0) ? "" : (" WHERE " + where));
        return getNumberResultDB(info, countsql, 0, countsql).thenCompose(total -> {
            if (total.longValue() <= 0) return CompletableFuture.completedFuture(new Sheet<>(0, new ArrayList()));
            return executeQuery(info, listsql).thenApply((MyResultSet dataset) -> {
                final List<T> list = new ArrayList();
                while (dataset.next()) {
                    list.add(getEntityValue(info, sels, dataset));
                }
                dataset.close();
                slowLog(s, listsql);
                return new Sheet(total.longValue(), list);
            });
        });
    }

    private static int fetchSize(Flipper flipper) {
        return flipper == null || flipper.getLimit() <= 0 ? 0 : flipper.getLimit();
    }

    protected <T> CompletableFuture<MyResultSet> thenApplyQueryUpdateStrategy(final EntityInfo<T> info, AtomicReference<ClientConnection> connRef, final CompletableFuture<MyResultSet> future) {
        if (info == null || (info.getTableStrategy() == null && !autoddl())) return future;
        final CompletableFuture<MyResultSet> rs = new CompletableFuture<>();
        future.whenComplete((g, t) -> {
            if (t != null) while (t instanceof CompletionException) t = t.getCause();
            if (t == null) {
                rs.complete(g);
            } else if (isTableNotExist(info, t instanceof SQLException ? ((SQLException) t).getSQLState() : null)) {
                if (info.getTableStrategy() == null) {
                    String[] tablesqls = createTableSqls(info);
                    if (tablesqls == null) {  //没有建表DDL
                        rs.completeExceptionally(t);
                    } else {
                        //执行一遍建表操作
                        final MyReqUpdate createTableReq = new MyReqUpdate();
                        createTableReq.prepare(tablesqls[0]); //mysql只会有一条sql
                        writePool().writeChannel(connRef.get(), createTableReq).whenComplete((g2, t2) -> {
                            if (t2 == null) {
                                g2.close();
                                rs.complete(MyResultSet.EMPTY);
                            } else {
                                rs.completeExceptionally(t2);
                            }
                        });
                    }
                } else {
                    rs.complete(MyResultSet.EMPTY);
                }
            } else {
                rs.completeExceptionally(t);
            }
        });
        return rs;
    }

    protected <T> CompletableFuture<MyResultSet> thenApplyInsertStrategy(final EntityInfo<T> info, final CompletableFuture<MyResultSet> future,
        final AtomicReference<MyClientRequest> reqRef, final AtomicReference<ClientConnection> connRef, final T[] values) {
        if (info == null || (info.getTableStrategy() == null && !autoddl())) return future;
        final CompletableFuture<MyResultSet> rs = new CompletableFuture<>();
        future.whenComplete((g, t) -> {
            if (t != null) while (t instanceof CompletionException) t = t.getCause();
            if (t == null) {
                rs.complete(g);
            } else if (isTableNotExist(info, t instanceof SQLException ? ((SQLException) t).getSQLState() : null)) {  //表不存在
                if (info.getTableStrategy() == null) {  //单表模式
                    String[] tablesqls = createTableSqls(info);
                    if (tablesqls == null) {  //没有建表DDL
                        rs.completeExceptionally(t);
                    } else {
                        //执行一遍建表操作
                        final MyReqUpdate createTableReq = new MyReqUpdate();
                        createTableReq.prepare(tablesqls[0]); //mysql只会有一条sql
                        writePool().writeChannel(connRef.get(), createTableReq).whenComplete((g2, t2) -> {
                            if (t2 != null) while (t2 instanceof CompletionException) t2 = t2.getCause();
                            if (t2 == null) { //建表成功
                                //执行一遍新增操作
                                writePool().writeChannel(connRef.get(), reqRef.get().reuse()).whenComplete((g3, t3) -> {
                                    if (t3 == null) {
                                        rs.complete(g3);
                                    } else {
                                        rs.completeExceptionally(t3);
                                    }
                                });
                            } else {
                                rs.completeExceptionally(t2);
                            }
                        });
                    }
                } else {  //分表模式
                    //执行一遍复制表操作
                    final String newTable = info.getTable(values[0]);
                    final MyReqUpdate copyTableReq = new MyReqUpdate();
                    copyTableReq.prepare(getTableCopySQL(info, newTable));
                    writePool().writeChannel(connRef.get(), copyTableReq).whenComplete((g2, t2) -> {
                        if (t2 != null) while (t2 instanceof CompletionException) t2 = t2.getCause();
                        if (t2 == null) {
                            //执行一遍新增操作
                            writePool().writeChannel(connRef.get(), reqRef.get().reuse()).whenComplete((g3, t3) -> {
                                if (t3 == null) {
                                    rs.complete(g3);
                                } else {
                                    rs.completeExceptionally(t3);
                                }
                            });
                        } else if (isTableNotExist(info, t2 instanceof SQLException ? ((SQLException) t2).getSQLState() : null)) { //还是没有表： 1、没有原始表; 2:没有库
                            if (newTable.indexOf('.') < 0) {  //没有原始表需要建表
                                String[] tablesqls = createTableSqls(info);
                                if (tablesqls == null) {  //没有建表DDL
                                    rs.completeExceptionally(t2);
                                } else {
                                    //执行一遍建表操作
                                    final MyReqUpdate createTableReq = new MyReqUpdate();
                                    createTableReq.prepare(tablesqls[0]); //mysql只会有一条sql
                                    writePool().writeChannel(connRef.get(), createTableReq).whenComplete((g4, t4) -> {
                                        if (t4 == null) { //建表成功
                                            //再执行一遍复制表操作
                                            final MyReqUpdate copyTableReq2 = new MyReqUpdate();
                                            copyTableReq2.prepare(getTableCopySQL(info, newTable));
                                            writePool().writeChannel(connRef.get(), copyTableReq2).whenComplete((g5, t5) -> {
                                                if (t5 == null) {
                                                    //再执行一遍新增操作
                                                    writePool().writeChannel(connRef.get(), reqRef.get().reuse()).whenComplete((g6, t6) -> {
                                                        if (t6 == null) {
                                                            rs.complete(g6);
                                                        } else {
                                                            rs.completeExceptionally(t6);
                                                        }
                                                    });
                                                } else {
                                                    rs.completeExceptionally(t5);
                                                }
                                            });
                                        } else {
                                            rs.completeExceptionally(t4);
                                        }
                                    });
                                }
                            } else {  //没有库需要建库
                                final MyReqUpdate createDatabaseReq = new MyReqUpdate();
                                createDatabaseReq.prepare("CREATE DATABASE IF NOT EXISTS " + newTable.substring(0, newTable.indexOf('.')));
                                writePool().writeChannel(connRef.get(), createDatabaseReq).whenComplete((g4, t4) -> {
                                    if (t4 == null) {  //建库成功
                                        //再执行一遍复制表操作
                                        final MyReqUpdate copyTableReq2 = new MyReqUpdate();
                                        copyTableReq2.prepare(getTableCopySQL(info, newTable));
                                        writePool().writeChannel(connRef.get(), copyTableReq2).whenComplete((g5, t5) -> {
                                            if (t5 != null) while (t5 instanceof CompletionException) t5 = t5.getCause();
                                            if (t5 == null) {
                                                //再执行一遍新增操作
                                                writePool().writeChannel(connRef.get(), reqRef.get().reuse()).whenComplete((g6, t6) -> {
                                                    if (t6 == null) {
                                                        rs.complete(g6);
                                                    } else {
                                                        rs.completeExceptionally(t6);
                                                    }
                                                });
                                            } else if (isTableNotExist(info, t5 instanceof SQLException ? ((SQLException) t5).getSQLState() : null)) { //没有原始表需要建表
                                                String[] tablesqls = createTableSqls(info);
                                                if (tablesqls == null) {  //没有建表DDL
                                                    rs.completeExceptionally(t5);
                                                } else {
                                                    //执行一遍建表操作
                                                    final MyReqUpdate createTableReq = new MyReqUpdate();
                                                    createTableReq.prepare(tablesqls[0]); //mysql只会有一条sql
                                                    writePool().writeChannel(connRef.get(), createTableReq).whenComplete((g6, t6) -> {
                                                        if (t6 == null) { //建表成功
                                                            //再再执行一遍复制表操作
                                                            final MyReqUpdate copyTableReq3 = new MyReqUpdate();
                                                            copyTableReq3.prepare(getTableCopySQL(info, newTable));
                                                            writePool().writeChannel(connRef.get(), copyTableReq3).whenComplete((g7, t7) -> {
                                                                if (t7 == null) {
                                                                    //再执行一遍新增操作
                                                                    writePool().writeChannel(connRef.get(), reqRef.get().reuse()).whenComplete((g8, t8) -> {
                                                                        if (t8 == null) {
                                                                            rs.complete(g8);
                                                                        } else {
                                                                            rs.completeExceptionally(t8);
                                                                        }
                                                                    });
                                                                } else {
                                                                    rs.completeExceptionally(t7);
                                                                }
                                                            });
                                                        } else {
                                                            rs.completeExceptionally(t6);
                                                        }
                                                    });
                                                }
                                            } else {
                                                rs.completeExceptionally(t5);
                                            }
                                        });
                                    } else {
                                        rs.completeExceptionally(t4);
                                    }
                                });
                            }
                        } else {
                            rs.completeExceptionally(t2);
                        }
                    });
                }
            } else {
                rs.completeExceptionally(t);
            }
        });
        return rs;
    }

    protected <T> CompletableFuture<Integer> executeUpdate(final EntityInfo<T> info, final String sql, final T[] values, int fetchSize, final int extendType, final Attribute<T, Serializable>[] attrs, final Object[]... parameters) {
        final long s = System.currentTimeMillis();
        final MyClient pool = writePool();
        WorkThread workThread = WorkThread.currWorkThread();
        AtomicReference<MyClientRequest> reqRef = new AtomicReference();
        AtomicReference<ClientConnection> connRef = new AtomicReference();
        CompletableFuture<MyResultSet> future = pool.connect(null).thenCompose(conn -> {
            connRef.set(conn);
            if (extendType > 0) {
                MyReqExtended req = ((MyClientConnection) conn).pollReqExtended(workThread, info);
                req.prepare(extendType, sql, 0, attrs, parameters);
                reqRef.set(req);
                return pool.writeChannel(conn, req);
            } else {
                MyReqUpdate req = ((MyClientConnection) conn).pollReqUpdate(workThread, info);
                req.prepare(sql, fetchSize, attrs, parameters);
                reqRef.set(req);
                return pool.writeChannel(conn, req);
            }
        });
        if (info == null || (info.getTableStrategy() == null && !autoddl())) {
            return future.thenApply(g -> {
                slowLog(s, sql);
                return g.getUpdateEffectCount();
            });
        }
        if (extendType == MyReqExtended.REQ_TYPE_EXTEND_INSERT) {
            return thenApplyInsertStrategy(info, future, reqRef, connRef, values).thenApply(g -> {
                slowLog(s, sql);
                return g.getUpdateEffectCount();
            });
        }
        return thenApplyQueryUpdateStrategy(info, connRef, future).thenApply(g -> {
            slowLog(s, sql);
            return g.getUpdateEffectCount();
        });
    }

    //info可以为null,供directQuery
    protected <T> CompletableFuture<MyResultSet> executeQuery(final EntityInfo<T> info, final String sql) {
        final long s = System.currentTimeMillis();
        final MyClient pool = readPool();
        WorkThread workThread = WorkThread.currWorkThread();
        AtomicReference<ClientConnection> connRef = new AtomicReference();
        return thenApplyQueryUpdateStrategy(info, connRef, pool.connect(null).thenCompose(conn -> {
            connRef.set(conn);
            MyReqQuery req = ((MyClientConnection) conn).pollReqQuery(workThread, info);
            req.prepare(sql);
            return pool.writeChannel(conn, req);
        })).thenApply(rs -> {
            slowLog(s, sql);
            return rs;
        });
    }

    @Local
    @Override
    public int directExecute(String sql) {
        final long s = System.currentTimeMillis();
        final MyClient pool = writePool();
        WorkThread workThread = WorkThread.currWorkThread();
        CompletableFuture<MyResultSet> future = pool.connect(null).thenCompose(conn -> {
            MyReqUpdate req = ((MyClientConnection) conn).pollReqUpdate(workThread, null);
            return pool.writeChannel(conn, req.prepare(sql));
        });
        return future.thenApply(g -> {
            slowLog(s, sql);
            return g.getUpdateEffectCount();
        }).join();
    }

    @Local
    @Override
    public int[] directExecute(String... sqls) {
        if (sqls.length == 1) return new int[]{directExecute(sqls[0])};
        final long s = System.currentTimeMillis();
        final MyClient pool = writePool();
        CompletableFuture<MyResultSet> future = pool.connect(null).thenCompose(conn -> {
            MyReqBatch req = new MyReqBatch();
            return pool.writeChannel(conn, req.prepare(sqls));
        });
        return future.thenApply(g -> {
            slowLog(s, sqls);
            return g.getBatchEffectCounts();
        }).join();
    }

    @Local
    @Override
    public <V> V directQuery(String sql, Function<DataResultSet, V> handler) {
        final long s = System.currentTimeMillis();
        return executeQuery(null, sql).thenApply((DataResultSet dataset) -> {
            V rs = handler.apply(dataset);
            dataset.close();
            slowLog(s, sql);
            return rs;
        }).join();
    }

}