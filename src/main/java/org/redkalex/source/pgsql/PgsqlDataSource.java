/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.logging.Level;
import java.util.stream.Stream;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceType;
import org.redkale.inject.ResourceEvent;
import org.redkale.net.*;
import org.redkale.net.client.*;
import org.redkale.service.Local;
import org.redkale.source.*;
import static org.redkale.source.DataSources.*;
import org.redkale.util.*;
import org.redkalex.source.pgsql.PgPrepareDesc.PgExtendMode;

/**
 * 部分协议格式参考： http://wp1i.cn/archives/78556.html
 *
 * <p>org.postgresql.jdbc.PgDatabaseMetaData.getTables
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(DataSource.class)
public class PgsqlDataSource extends AbstractDataSqlSource {

    static boolean debug = false; // true false

    protected PgClient readPool;

    protected AsyncGroup readGroup;

    protected PgClient writePool;

    protected AsyncGroup writeGroup;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        this.dbtype = "postgresql";
        this.readPool = createPgPool((readConfProps == writeConfProps) ? "rw" : "read", readConfProps);
        if (readConfProps == writeConfProps) {
            this.writePool = readPool;
            this.writeGroup = this.readGroup;
        } else {
            this.writePool = createPgPool("write", writeConfProps);
        }
    }

    private PgClient createPgPool(String rw, Properties prop) {
        String url = prop.getProperty(DATA_SOURCE_URL);
        SourceUrlInfo info = parseSourceUrl(url);
        info.username = prop.getProperty(DATA_SOURCE_USER, "");
        info.password = prop.getProperty(DATA_SOURCE_PASSWORD, "");
        int maxConns = Math.max(1, Integer.decode(prop.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
        int maxPipelines = Math.max(
                1,
                Integer.decode(prop.getProperty(
                        DATA_SOURCE_PIPELINES, "" + org.redkale.net.client.Client.DEFAULT_MAX_PIPELINES)));
        AsyncGroup ioGroup = clientAsyncGroup;
        if (clientAsyncGroup == null || "write".equalsIgnoreCase(rw)) {
            String f = "Redkalex-PgClient-IOThread-" + resourceName() + "-"
                    + (rw.length() < 3 ? rw.toUpperCase() : Utility.firstCharUpperCase(rw)) + "-%s";
            ioGroup = AsyncGroup.create(f, workExecutor, 16 * 1024, Utility.cpus() * 4)
                    .start();
        }
        if ("write".equals(rw)) {
            this.writeGroup = ioGroup;
        } else {
            this.readGroup = ioGroup;
        }
        return new PgClient(
                resourceName(),
                ioGroup,
                resourceName() + "." + rw,
                new ClientAddress(info.servaddr),
                maxConns,
                maxPipelines,
                autoddl(),
                prop,
                info);
    }

    @Override
    protected void updateOneResourceChange(Properties newProps, ResourceEvent[] events) {
        PgClient oldPool = this.readPool;
        AsyncGroup oldGroup = this.readGroup;
        this.readPool = createPgPool("rw", newProps);
        this.writePool = readPool;
        this.writeGroup = this.readGroup;
        if (oldPool != null) {
            oldPool.close();
        }
        if (oldGroup != null && oldGroup != clientAsyncGroup) {
            oldGroup.close();
        }
    }

    @Override
    protected void updateReadResourceChange(Properties newReadProps, ResourceEvent[] events) {
        PgClient oldPool = this.readPool;
        AsyncGroup oldGroup = this.readGroup;
        this.readPool = createPgPool("read", newReadProps);
        if (oldPool != null) {
            oldPool.close();
        }
        if (oldGroup != null && oldGroup != clientAsyncGroup) {
            oldGroup.close();
        }
    }

    @Override
    protected void updateWriteResourceChange(Properties newWriteProps, ResourceEvent[] events) {
        PgClient oldPool = this.writePool;
        AsyncGroup oldGroup = this.writeGroup;
        this.writePool = createPgPool("write", newWriteProps);
        if (oldPool != null) {
            oldPool.close();
        }
        if (oldGroup != null && oldGroup != clientAsyncGroup) {
            oldGroup.close();
        }
    }

    @Override
    protected int readMaxConns() {
        return readPool.getMaxConns();
    }

    @Override
    protected int writeMaxConns() {
        return writePool.getMaxConns();
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
    protected PgClient readPool() {
        return readPool;
    }

    @Local
    protected PgClient writePool() {
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
    public CompletableFuture<Integer> batchAsync(final DataBatch batch) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    protected <T> CompletableFuture<Integer> insertDBAsync(EntityInfo<T> info, T... entitys) {
        final long s = System.currentTimeMillis();
        final Attribute<T, Serializable>[] attrs = info.getInsertAttributes();
        final Serializable[][] objs = new Serializable[entitys.length][];
        for (int i = 0; i < entitys.length; i++) {
            final Serializable[] params = new Serializable[attrs.length];
            for (int j = 0; j < attrs.length; j++) {
                params[j] = getEntityAttrValue(info, attrs[j], entitys[i]);
            }
            objs[i] = params;
        }
        PgClient pool = writePool();
        Map<String, PrepareInfo<T>> prepareInfos =
                info.getTableStrategy() == null ? null : getInsertQuestionPrepareInfo(info, entitys);
        if ((prepareInfos == null || prepareInfos.size() < 2) && pool.cachePreparedStatements()) {
            String sql = info.isAutoGenerated()
                    ? (info.getInsertDollarPrepareSQL(entitys[0]) + " RETURNING " + info.getPrimarySQLColumn())
                    : info.getInsertDollarPrepareSQL(entitys[0]);
            WorkThread workThread = WorkThread.currentWorkThread();
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, info);
                req.prepareParams(PgClientRequest.REQ_TYPE_EXTEND_INSERT, PgExtendMode.INSERT_ENTITY, sql, 0, objs);
                Function<PgResultSet, Integer> transfer = dataset -> {
                    int rs = dataset.getUpdateEffectCount();
                    if (info.isAutoGenerated()) {
                        final Attribute primary = info.getPrimary();
                        int i = -1;
                        while (dataset.next()) {
                            primary.set(entitys[++i], DataResultSet.getRowColumnValue(dataset, primary, 1, null));
                        }
                    }
                    conn.offerResultSet(req, dataset);
                    slowLog(s, sql);
                    return rs;
                };
                return pool.writeChannel(conn, transfer, req);
            });
        } else {
            return executeUpdate(
                    info, new String[] {info.getInsertDollarPrepareSQL(entitys[0])}, entitys, 0, true, attrs, objs);
        }
    }

    @Override
    protected <T> CompletableFuture<Integer> deleteDBAsync(
            EntityInfo<T> info,
            String[] tables,
            Flipper flipper,
            FilterNode node,
            Map<String, List<Serializable>> pkmap,
            String... sqls) {
        if (info.isLoggable(logger, Level.FINEST)) {
            final String debugsql =
                    flipper == null || flipper.getLimit() <= 0 ? sqls[0] : (sqls[0] + " LIMIT " + flipper.getLimit());
            if (info.isLoggable(logger, Level.FINEST, debugsql)) {
                if (sqls.length == 1) {
                    logger.finest(info.getType().getSimpleName() + " delete sql=" + debugsql);
                } else {
                    logger.finest(info.getType().getSimpleName() + " limit " + flipper.getLimit() + " delete sqls="
                            + Arrays.toString(sqls));
                }
            }
        }
        return executeUpdate(info, sqls, null, fetchSize(flipper), false, null, null);
    }

    @Override
    protected <T> CompletableFuture<Integer> clearTableDBAsync(
            EntityInfo<T> info, final String[] tables, FilterNode node, String... sqls) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sqls[0])) {
                if (sqls.length == 1) {
                    logger.finest(info.getType().getSimpleName() + " clearTable sql=" + sqls[0]);
                } else {
                    logger.finest(info.getType().getSimpleName() + " clearTable sqls=" + Arrays.toString(sqls));
                }
            }
        }
        return executeUpdate(info, sqls, null, 0, false, null, null);
    }

    @Override
    protected <T> CompletableFuture<Integer> createTableDBAsync(
            EntityInfo<T> info, String copyTableSql, final Serializable pk, String... sqls) {
        if (copyTableSql == null) {
            return executeUpdate(info, sqls, null, 0, false, null, null);
        } else {
            return executeUpdate(info, new String[] {copyTableSql}, null, 0, false, null, null);
        }
    }

    @Override
    protected <T> CompletableFuture<Integer> dropTableDBAsync(
            EntityInfo<T> info, final String[] tables, FilterNode node, String... sqls) {
        if (info.isLoggable(logger, Level.FINEST)) {
            if (info.isLoggable(logger, Level.FINEST, sqls[0])) {
                if (sqls.length == 1) {
                    logger.finest(info.getType().getSimpleName() + " dropTable sql=" + sqls[0]);
                } else {
                    logger.finest(info.getType().getSimpleName() + " dropTable sqls=" + Arrays.toString(sqls));
                }
            }
        }
        return executeUpdate(info, sqls, null, 0, false, null, null);
    }

    @Override
    protected <T> CompletableFuture<Integer> updateEntityDBAsync(EntityInfo<T> info, final T... values) {
        final long s = System.currentTimeMillis();
        final Attribute<T, Serializable> primary = info.getPrimary();
        final Attribute<T, Serializable>[] attrs = info.getUpdateAttributes();
        PgClient pool = writePool();
        final String caseSql = info.getTableStrategy() == null && pool.cachePreparedStatements()
                ? info.getUpdateDollarPrepareCaseSQL(values)
                : null;
        Serializable[][] objs0;
        if (caseSql == null) {
            objs0 = new Serializable[values.length][];
            for (int i = 0; i < values.length; i++) {
                final Serializable[] params = new Serializable[attrs.length + 1];
                for (int j = 0; j < attrs.length; j++) {
                    params[j] = getEntityAttrValue(info, attrs[j], values[i]);
                }
                params[attrs.length] = primary.get(values[i]); // 最后一个是主键
                objs0[i] = params;
            }
        } else {
            int len = values.length;
            objs0 = new Serializable[1][];
            Serializable[] params = new Serializable[len * 2];
            Attribute<T, Serializable> otherAttr = attrs[0];
            for (int i = 0; i < values.length; i++) {
                params[i] = primary.get(values[i]);
                params[i + len] = getEntityAttrValue(info, otherAttr, values[i]);
            }
            objs0[0] = params;
        }
        final Serializable[][] objs = objs0;
        if (info.getTableStrategy() == null && pool.cachePreparedStatements()) {
            String sql = caseSql == null ? info.getUpdateDollarPrepareSQL(values[0]) : caseSql;
            WorkThread workThread = WorkThread.currentWorkThread();
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, info);
                req.prepareParams(
                        PgClientRequest.REQ_TYPE_EXTEND_UPDATE,
                        caseSql == null ? PgExtendMode.UPDATE_ENTITY : PgExtendMode.UPCASE_ENTITY,
                        sql,
                        0,
                        objs);
                Function<PgResultSet, Integer> transfer = dataset -> {
                    int rs = dataset.getUpdateEffectCount();
                    conn.offerResultSet(req, dataset);
                    slowLog(s, sql);
                    return rs;
                };
                return pool.writeChannel(conn, transfer, req);
            });
        } else {
            return executeUpdate(
                    info,
                    new String[] {info.getUpdateDollarPrepareSQL(values[0])},
                    null,
                    0,
                    false,
                    Utility.append(attrs, primary),
                    objs);
        }
    }

    @Override
    protected <T> CompletableFuture<Integer> updateColumnDBAsync(
            EntityInfo<T> info, Flipper flipper, UpdateSqlInfo sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            final String debugsql = sql.sql; // flipper == null || flipper.getLimit() <= 0 ? sql : (sql + " LIMIT " +
            // flipper.getLimit());
            if (info.isLoggable(logger, Level.FINEST, debugsql)) {
                logger.finest(info.getType().getSimpleName() + " update sql=" + debugsql);
            }
        }
        List<Object[]> objs = null;
        if (sql.blobs != null || sql.tables != null) {
            objs = new ArrayList<>();
            if (sql.tables == null) {
                objs.add(sql.blobs.toArray());
            } else {
                for (String table : sql.tables) {
                    if (sql.blobs != null) {
                        List w = new ArrayList(sql.blobs);
                        w.add(table);
                        objs.add(w.toArray());
                    } else {
                        objs.add(new Object[] {table});
                    }
                }
            }
        }
        Object[][] as = objs != null && !objs.isEmpty() ? objs.toArray(new Object[objs.size()][]) : null;
        return executeUpdate(info, new String[] {sql.sql}, null, fetchSize(flipper), false, null, as);
    }

    @Override
    protected <T, N extends Number> CompletableFuture<Map<String, N>> getNumberMapDBAsync(
            EntityInfo<T> info, String[] tables, String sql, FilterNode node, FilterFuncColumn... columns) {
        return getNumberMapDBApply(info, executeQuery(info, sql), columns);
    }

    @Override
    protected <T> CompletableFuture<Number> getNumberResultDBAsync(
            EntityInfo<T> info,
            String[] tables,
            String sql,
            FilterFunc func,
            Number defVal,
            String column,
            FilterNode node) {
        return getNumberResultDBApply(info, executeQuery(info, sql), defVal, column);
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N>> queryColumnMapDBAsync(
            EntityInfo<T> info,
            String[] tables,
            String sql,
            String keyColumn,
            FilterFunc func,
            String funcColumn,
            FilterNode node) {
        return queryColumnMapDBApply(info, executeQuery(info, sql), keyColumn);
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K[], N[]>> queryColumnMapDBAsync(
            EntityInfo<T> info,
            String[] tables,
            String sql,
            final ColumnNode[] funcNodes,
            final String[] groupByColumns,
            FilterNode node) {
        return queryColumnMapDBApply(info, executeQuery(info, sql), funcNodes, groupByColumns);
    }

    @Override
    public <T> CompletableFuture<T> findAsync(final Class<T> clazz, final SelectColumn selects, final Serializable pk) {
        final EntityInfo<T> info = loadEntityInfo(clazz);
        final EntityCache<T> cache = info.getCache();
        if (cache != null) {
            T rs = selects == null ? cache.find(pk) : cache.find(selects, pk);
            if (cache.isFullLoaded() || rs != null) {
                return CompletableFuture.completedFuture(rs);
            }
        }

        final long s = System.currentTimeMillis();
        PgClient pool = readPool();
        if (info.getTableStrategy() == null && selects == null && pool.cachePreparedStatements()) {
            String sql = info.getFindDollarPrepareSQL(pk);
            WorkThread workThread = WorkThread.currentWorkThread();
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, info);
                req.preparePrimarys(PgClientRequest.REQ_TYPE_EXTEND_QUERY, PgExtendMode.FIND_ENTITY, sql, 0, pk);
                Function<PgResultSet, T> transfer = dataset -> {
                    T rs = (T) dataset.oneEntity;
                    conn.offerResultSet(req, dataset);
                    slowLog(s, sql);
                    return rs;
                };
                return pool.writeChannel(conn, transfer, req);
            });
        }
        String sql = findSql(info, selects, pk);
        if (info.isLoggable(logger, Level.FINEST, sql)) {
            logger.finest(info.getType().getSimpleName() + " find sql=" + sql);
        }
        return findDBApply(info, executeQuery(info, sql), true, selects);
    }

    @Override
    protected <T> CompletableFuture<T> findDBAsync(
            EntityInfo<T> info,
            String[] tables,
            String sql,
            boolean onlypk,
            SelectColumn selects,
            Serializable pk,
            FilterNode node) {
        return findDBApply(info, executeQuery(info, sql), onlypk, selects);
    }

    @Override // 无Cache的findsAsync
    protected <T> CompletableFuture<T[]> findsDBAsync(
            final EntityInfo<T> info, final SelectColumn selects, Serializable... pks) {
        final long s = System.currentTimeMillis();
        PgClient pool = readPool();
        if (info.getTableStrategy() == null && selects == null && pool.cachePreparedStatements()) {
            String sql = info.getFindsDollarPrepareSQL(pks[0]);
            WorkThread workThread = WorkThread.currentWorkThread();
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, info);
                req.preparePrimarys(PgClientRequest.REQ_TYPE_EXTEND_QUERY, PgExtendMode.FINDS_ENTITY, sql, 0, pks);
                Function<PgResultSet, T[]> transfer = dataset -> {
                    T[] rs = dataset.listEntity == null
                            ? info.getArrayer().apply(pks.length)
                            : dataset.listEntity.toArray(info.getArrayer());
                    conn.offerResultSet(req, dataset);
                    slowLog(s, sql);
                    return rs;
                };
                return pool.writeChannel(conn, transfer, req);
            });
        } else {
            return super.findsDBAsync(info, selects, pks);
        }
    }

    @Override
    public <D extends Serializable, T> CompletableFuture<List<T>> findsListAsync(
            final Class<T> clazz, final java.util.stream.Stream<D> pks) {
        final long s = System.currentTimeMillis();
        final EntityInfo<T> info = loadEntityInfo(clazz);
        Serializable[] ids = pks.toArray(serialArrayFunc);
        PgClient pool = readPool();
        if (info.getTableStrategy() == null && pool.cachePreparedStatements()) {
            String sql = info.getFindsDollarPrepareSQL(ids[0]);
            WorkThread workThread = WorkThread.currentWorkThread();
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, info);
                req.preparePrimarys(PgClientRequest.REQ_TYPE_EXTEND_QUERY, PgExtendMode.FINDS_ENTITY, sql, 0, ids);
                Function<PgResultSet, List<T>> transfer = dataset -> {
                    List<T> rs = dataset.listEntity == null ? new ArrayList<>() : (List) dataset.listEntity;
                    conn.offerResultSet(req, dataset);
                    slowLog(s, sql);
                    return rs;
                };
                return pool.writeChannel(conn, transfer, req);
            });
        } else {
            return queryListAsync(
                    info.getType(),
                    (SelectColumn) null,
                    (Flipper) null,
                    FilterNodes.in(info.getPrimarySQLColumn(), ids));
        }
    }

    @Override
    protected <T> CompletableFuture<Serializable> findColumnDBAsync(
            EntityInfo<T> info,
            final String[] tables,
            String sql,
            boolean onlypk,
            String column,
            Serializable defValue,
            Serializable pk,
            FilterNode node) {
        return findColumnDBApply(info, executeQuery(info, sql), onlypk, column, defValue);
    }

    @Override
    protected <T> CompletableFuture<Boolean> existsDBAsync(
            EntityInfo<T> info, final String[] tables, String sql, boolean onlypk, Serializable pk, FilterNode node) {
        return existsDBApply(info, executeQuery(info, sql), onlypk);
    }

    @Override
    public <T> CompletableFuture<List<T>> queryListAsync(final Class<T> clazz) {
        final EntityInfo<T> info = loadEntityInfo(clazz);
        final PgClient pool = readPool();
        if (info.getTableStrategy() == null && pool.cachePreparedStatements()) {
            final EntityCache<T> cache = info.getCache();
            if (cache != null && cache.isFullLoaded()) {
                return CompletableFuture.completedFuture(
                        cache.querySheet(false, false, null, null, null).list(true));
            }
            final long s = System.currentTimeMillis();
            final WorkThread workThread = WorkThread.currentWorkThread();
            final String pageSql = info.getAllQueryPrepareSQL();
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, info);
                req.prepare(PgClientRequest.REQ_TYPE_EXTEND_QUERY, PgExtendMode.LISTALL_ENTITY, pageSql, 0);
                Function<PgResultSet, List<T>> transfer = dataset -> {
                    List<T> rs = dataset.listEntity == null ? new ArrayList<>() : (List) dataset.listEntity;
                    conn.offerResultSet(req, dataset);
                    slowLog(s, pageSql);
                    return rs;
                };
                return pool.writeChannel(conn, transfer, req);
            });
        } else {
            return super.queryListAsync(clazz);
        }
    }

    @Override // 无Cache版querySheetDBAsync
    protected <T> CompletableFuture<Sheet<T>> querySheetDBAsync(
            EntityInfo<T> info,
            final boolean readCache,
            boolean needTotal,
            final boolean distinct,
            SelectColumn selects,
            Flipper flipper,
            FilterNode node) {
        final long s = System.currentTimeMillis();
        final SelectColumn sels = selects;
        final PgClient pool = readPool();
        String[] tables = info.getTables(node);
        final boolean cachePrepared = pool.cachePreparedStatements()
                && readCache
                && info.getTableStrategy() == null
                && sels == null
                && node == null
                && flipper == null
                && !distinct
                && !needTotal;
        PageCountSql sqls = createPageCountSql(info, readCache, needTotal, distinct, sels, tables, flipper, node);

        final String pageSql = cachePrepared ? info.getAllQueryPrepareSQL() : sqls.pageSql;
        if (cachePrepared && info.isLoggable(logger, Level.FINEST, pageSql)) {
            logger.finest(info.getType().getSimpleName() + " query sql=" + pageSql);
        }
        if (!needTotal) {
            CompletableFuture<PgResultSet> listFuture;
            if (cachePrepared) {
                WorkThread workThread = WorkThread.currentWorkThread();
                return pool.connect().thenCompose(conn -> {
                    PgReqExtended req = conn.pollReqExtended(workThread, info);
                    req.prepare(PgClientRequest.REQ_TYPE_EXTEND_QUERY, PgExtendMode.LISTALL_ENTITY, pageSql, 0);
                    Function<PgResultSet, Sheet<T>> transfer = dataset -> {
                        List<T> rs = dataset.listEntity == null ? new ArrayList<>() : (List) dataset.listEntity;
                        conn.offerResultSet(req, dataset);
                        slowLog(s, pageSql);
                        return Sheet.asSheet(rs);
                    };
                    return pool.writeChannel(conn, transfer, req);
                });
            } else {
                listFuture = executeQuery(info, pageSql);
            }
            return listFuture.thenApply(dataset -> {
                final List<T> list = new ArrayList();
                while (dataset.next()) {
                    list.add(getEntityValue(info, sels, dataset));
                }
                dataset.close();
                slowLog(s, pageSql);
                return Sheet.asSheet(list);
            });
        }
        return getNumberResultDBAsync(
                        info,
                        null,
                        sqls.countSql,
                        distinct ? FilterFunc.DISTINCTCOUNT : FilterFunc.COUNT,
                        0,
                        null,
                        node)
                .thenCompose(total -> {
                    if (total.longValue() <= 0) {
                        return CompletableFuture.completedFuture(new Sheet<>(0, new ArrayList()));
                    }
                    return executeQuery(info, pageSql).thenApply((PgResultSet dataset) -> {
                        final List<T> list = new ArrayList();
                        while (dataset.next()) {
                            list.add(getEntityValue(info, sels, dataset));
                        }
                        dataset.close();
                        slowLog(s, pageSql);
                        return new Sheet(total.longValue(), list);
                    });
                });
    }

    private static int fetchSize(Flipper flipper) {
        return flipper == null || flipper.getLimit() <= 0 ? 0 : flipper.getLimit();
    }

    protected <T> CompletableFuture<PgResultSet> thenApplyQueryUpdateStrategy(
            final EntityInfo<T> info,
            final PgClientConnection conn,
            final Function<PgClientConnection, CompletableFuture<PgResultSet>> futureFunc) {
        if (info == null || (info.getTableStrategy() == null && !autoddl())) {
            return futureFunc.apply(conn);
        }
        final CompletableFuture<PgResultSet> rs = new CompletableFuture<>();
        futureFunc.apply(conn).whenComplete((g, t) -> {
            if (t != null) {
                while (t instanceof CompletionException) t = t.getCause();
            }
            if (t == null) {
                rs.complete(g);
            } else if (isTableNotExist(info, t, t instanceof SQLException ? ((SQLException) t).getSQLState() : null)) {
                if (info.getTableStrategy() == null) {
                    String[] tablesqls = createTableSqls(info);
                    if (tablesqls == null) { // 没有建表DDL
                        rs.completeExceptionally(t);
                    } else {
                        // 执行一遍建表操作
                        final PgReqUpdate createTableReq = new PgReqUpdate();
                        createTableReq.prepare(tablesqls[0]);
                        writePool().writeChannel(conn, createTableReq).whenComplete((g2, t2) -> {
                            if (t2 == null) {
                                g2.close();
                                rs.complete(PgResultSet.EMPTY);
                            } else {
                                rs.completeExceptionally(t2);
                            }
                        });
                    }
                } else {
                    rs.complete(PgResultSet.EMPTY);
                }
            } else {
                rs.completeExceptionally(t);
            }
        });
        return rs;
    }

    protected <T> CompletableFuture<PgResultSet> thenApplyInsertStrategy(
            final EntityInfo<T> info,
            final CompletableFuture<PgResultSet> future,
            final ObjectRef<PgClientRequest> reqRef,
            final ObjectRef<ClientConnection> connRef,
            final T[] values) {
        if (info == null || (info.getTableStrategy() == null && !autoddl())) {
            return future;
        }
        final CompletableFuture<PgResultSet> rs = new CompletableFuture<>();
        future.whenComplete((g, t) -> {
            if (t != null) {
                while (t instanceof CompletionException) t = t.getCause();
            }
            if (t == null) {
                rs.complete(g);
            } else if (isTableNotExist(
                    info, t, t instanceof SQLException ? ((SQLException) t).getSQLState() : null)) { // 表不存在
                if (info.getTableStrategy() == null) { // 单表模式
                    String[] tablesqls = createTableSqls(info);
                    if (tablesqls == null) { // 没有建表DDL
                        rs.completeExceptionally(t);
                    } else {
                        // 执行一遍建表操作
                        final PgReqUpdate createTableReq = new PgReqUpdate();
                        createTableReq.prepare(tablesqls[0]);
                        writePool().writeChannel(connRef.get(), createTableReq).whenComplete((g2, t2) -> {
                            if (t2 != null) {
                                while (t2 instanceof CompletionException) t2 = t2.getCause();
                            }
                            if (t2 == null) { // 建表成功
                                // 执行一遍新增操作
                                writePool()
                                        .writeChannel(
                                                connRef.get(), reqRef.get().reuse())
                                        .whenComplete((g3, t3) -> {
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
                } else { // 分表模式
                    // 执行一遍复制表操作
                    final String newTable = info.getTable(values[0]);
                    final PgReqUpdate copyTableReq = new PgReqUpdate();
                    copyTableReq.prepare(getTableCopySql(info, newTable));
                    writePool().writeChannel(connRef.get(), copyTableReq).whenComplete((g2, t2) -> {
                        if (t2 != null) {
                            while (t2 instanceof CompletionException) t2 = t2.getCause();
                        }
                        if (t2 == null) {
                            // 执行一遍新增操作
                            writePool()
                                    .writeChannel(connRef.get(), reqRef.get().reuse())
                                    .whenComplete((g3, t3) -> {
                                        if (t3 == null) {
                                            rs.complete(g3);
                                        } else {
                                            rs.completeExceptionally(t3);
                                        }
                                    });
                        } else if (isTableNotExist(
                                info,
                                t2,
                                t2 instanceof SQLException
                                        ? ((SQLException) t2).getSQLState()
                                        : null)) { // 还是没有表： 1、没有原始表; 2:没有库
                            if (newTable.indexOf('.') < 0) { // 没有原始表需要建表
                                String[] tablesqls = createTableSqls(info);
                                if (tablesqls == null) { // 没有建表DDL
                                    rs.completeExceptionally(t2);
                                } else {
                                    // 执行一遍建表操作
                                    final PgReqUpdate createTableReq = new PgReqUpdate();
                                    createTableReq.prepare(tablesqls[0]);
                                    writePool()
                                            .writeChannel(connRef.get(), createTableReq)
                                            .whenComplete((g4, t4) -> {
                                                if (t4 == null) { // 建表成功
                                                    // 再执行一遍复制表操作
                                                    final PgReqUpdate copyTableReq2 = new PgReqUpdate();
                                                    copyTableReq2.prepare(getTableCopySql(info, newTable));
                                                    writePool()
                                                            .writeChannel(connRef.get(), copyTableReq2)
                                                            .whenComplete((g5, t5) -> {
                                                                if (t5 == null) {
                                                                    // 再执行一遍新增操作
                                                                    writePool()
                                                                            .writeChannel(
                                                                                    connRef.get(),
                                                                                    reqRef.get()
                                                                                            .reuse())
                                                                            .whenComplete((g6, t6) -> {
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
                            } else { // 没有库需要建库
                                final PgReqUpdate createDatabaseReq = new PgReqUpdate();
                                createDatabaseReq.prepare(
                                        "CREATE SCHEMA IF NOT EXISTS " + newTable.substring(0, newTable.indexOf('.')));
                                writePool()
                                        .writeChannel(connRef.get(), createDatabaseReq)
                                        .whenComplete((g4, t4) -> {
                                            if (t4 == null) { // 建库成功
                                                // 再执行一遍复制表操作
                                                final PgReqUpdate copyTableReq2 = new PgReqUpdate();
                                                copyTableReq2.prepare(getTableCopySql(info, newTable));
                                                writePool()
                                                        .writeChannel(connRef.get(), copyTableReq2)
                                                        .whenComplete((g5, t5) -> {
                                                            if (t5 != null) {
                                                                while (t5 instanceof CompletionException)
                                                                    t5 = t5.getCause();
                                                            }
                                                            if (t5 == null) {
                                                                // 再执行一遍新增操作
                                                                writePool()
                                                                        .writeChannel(
                                                                                connRef.get(),
                                                                                reqRef.get()
                                                                                        .reuse())
                                                                        .whenComplete((g6, t6) -> {
                                                                            if (t6 == null) {
                                                                                rs.complete(g6);
                                                                            } else {
                                                                                rs.completeExceptionally(t6);
                                                                            }
                                                                        });
                                                            } else if (isTableNotExist(
                                                                    info,
                                                                    t5,
                                                                    t5 instanceof SQLException
                                                                            ? ((SQLException) t5).getSQLState()
                                                                            : null)) { // 没有原始表需要建表
                                                                String[] tablesqls = createTableSqls(info);
                                                                if (tablesqls == null) { // 没有建表DDL
                                                                    rs.completeExceptionally(t5);
                                                                } else {
                                                                    // 执行一遍建表操作
                                                                    final PgReqUpdate createTableReq =
                                                                            new PgReqUpdate();
                                                                    createTableReq.prepare(tablesqls[0]);
                                                                    writePool()
                                                                            .writeChannel(connRef.get(), createTableReq)
                                                                            .whenComplete((g6, t6) -> {
                                                                                if (t6 == null) { // 建表成功
                                                                                    // 再再执行一遍复制表操作
                                                                                    final PgReqUpdate copyTableReq3 =
                                                                                            new PgReqUpdate();
                                                                                    copyTableReq3.prepare(
                                                                                            getTableCopySql(
                                                                                                    info, newTable));
                                                                                    writePool()
                                                                                            .writeChannel(
                                                                                                    connRef.get(),
                                                                                                    copyTableReq3)
                                                                                            .whenComplete((g7, t7) -> {
                                                                                                if (t7 == null) {
                                                                                                    // 再执行一遍新增操作
                                                                                                    writePool()
                                                                                                            .writeChannel(
                                                                                                                    connRef
                                                                                                                            .get(),
                                                                                                                    reqRef.get()
                                                                                                                            .reuse())
                                                                                                            .whenComplete(
                                                                                                                    (g8,
                                                                                                                            t8) -> {
                                                                                                                        if (t8
                                                                                                                                == null) {
                                                                                                                            rs
                                                                                                                                    .complete(
                                                                                                                                            g8);
                                                                                                                        } else {
                                                                                                                            rs
                                                                                                                                    .completeExceptionally(
                                                                                                                                            t8);
                                                                                                                        }
                                                                                                                    });
                                                                                                } else {
                                                                                                    rs
                                                                                                            .completeExceptionally(
                                                                                                                    t7);
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

    protected <T> CompletableFuture<Integer> executeUpdate(
            final EntityInfo<T> info,
            final String[] sqls,
            final T[] values,
            int fetchSize,
            final boolean insert,
            final Attribute<T, Serializable>[] attrs,
            final Object[][] parameters) {
        final long s = System.currentTimeMillis();
        final PgClient pool = writePool();
        WorkThread workThread = WorkThread.currentWorkThread();
        ObjectRef<PgClientRequest> reqRef = new ObjectRef();
        ObjectRef<ClientConnection> connRef = new ObjectRef();
        Function<PgClientConnection, CompletableFuture<PgResultSet>> futureFunc = conn -> {
            PgClientRequest req;
            if (sqls.length == 1) {
                PgReqUpdate upreq =
                        insert ? conn.pollReqInsert(workThread, info) : conn.pollReqUpdate(workThread, info);
                upreq.prepare(sqls[0], fetchSize, attrs, parameters);
                req = upreq;
            } else {
                req = new PgReqBatch().prepare(sqls);
            }
            reqRef.set(req);
            connRef.set(conn);
            return pool.writeChannel(conn, req);
        };
        if (info == null || (info.getTableStrategy() == null && !autoddl())) {
            return pool.connect().thenCompose(futureFunc).thenApply(g -> {
                slowLog(s, sqls);
                return g.getUpdateEffectCount();
            });
        }
        if (insert) {
            return thenApplyInsertStrategy(info, pool.connect().thenCompose(futureFunc), reqRef, connRef, values)
                    .thenApply(g -> {
                        slowLog(s, sqls);
                        return g.getUpdateEffectCount();
                    });
        }
        return pool.connect()
                .thenCompose(conn -> thenApplyQueryUpdateStrategy(info, conn, futureFunc))
                .thenApply(g -> {
                    slowLog(s, sqls);
                    return g.getUpdateEffectCount();
                });
    }

    // info可以为null,供directQuery
    protected <T> CompletableFuture<PgResultSet> executeQuery(final EntityInfo<T> info, final String sql) {
        final PgClient pool = readPool();
        WorkThread workThread = WorkThread.currentWorkThread();
        return pool.connect()
                .thenCompose(c -> thenApplyQueryUpdateStrategy(info, c, conn -> {
                    PgReqQuery req = conn.pollReqQuery(workThread, info);
                    req.prepare(sql);
                    return pool.writeChannel(conn, req);
                }));
    }

    @Local
    @Override
    public CompletableFuture<int[]> nativeUpdatesAsync(String... sqls) {
        if (sqls.length == 1) {
            return nativeUpdateAsync(sqls[0]).thenApply(v -> new int[] {v});
        }
        final long s = System.currentTimeMillis();
        final PgClient pool = writePool();
        CompletableFuture<PgResultSet> future = pool.connect().thenCompose(conn -> {
            PgReqBatch req = new PgReqBatch();
            return pool.writeChannel(conn, req.prepare(sqls));
        });
        return future.thenApply(g -> {
            slowLog(s, sqls);
            return g.getBatchEffectCounts();
        });
    }

    @Local
    @Override
    public CompletableFuture<Integer> nativeUpdateAsync(String sql) {
        final long s = System.currentTimeMillis();
        final PgClient pool = writePool();
        WorkThread workThread = WorkThread.currentWorkThread();
        CompletableFuture<PgResultSet> future = pool.connect().thenCompose(conn -> {
            PgReqUpdate req = conn.pollReqUpdate(workThread, null);
            return pool.writeChannel(conn, req.prepare(sql));
        });
        return future.thenApply(g -> {
            slowLog(s, sql);
            return g.getUpdateEffectCount();
        });
    }

    @Local
    @Override
    public <V> CompletableFuture<V> nativeQueryAsync(
            String sql, BiConsumer<Object, Object> consumer, Function<DataResultSet, V> handler) {
        final long s = System.currentTimeMillis();
        return executeQuery(null, sql).thenApply((DataResultSet dataset) -> {
            V rs = handler.apply(dataset);
            dataset.close();
            slowLog(s, sql);
            return rs;
        });
    }

    @Local
    @Override
    public CompletableFuture<Integer> nativeUpdateAsync(String sql, Map<String, Object> params) {
        long s = System.currentTimeMillis();
        DataNativeSqlStatement sinfo = super.nativeParse(sql, false, null, params);
        final WorkThread workThread = WorkThread.currentWorkThread();
        PgClient pool = writePool();
        if (!sinfo.isEmptyNamed()) {
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, null);
                Stream<Serializable> pstream = sinfo.getParamNames().stream().map(n -> (Serializable) params.get(n));
                req.prepareParams(
                        PgClientRequest.REQ_TYPE_EXTEND_UPDATE,
                        PgExtendMode.OTHER_NATIVE,
                        sinfo.getNativeSql(),
                        0,
                        sinfo.getParamNames().size(),
                        pstream);
                Function<PgResultSet, Integer> transfer = dataset -> {
                    int rs = dataset.getUpdateEffectCount();
                    conn.offerResultSet(req, dataset);
                    slowLog(s, sinfo.getNativeSql());
                    return rs;
                };
                return pool.writeChannel(conn, transfer, req);
            });
        } else {
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, null);
                req.prepare(PgClientRequest.REQ_TYPE_EXTEND_UPDATE, PgExtendMode.OTHER_NATIVE, sinfo.getNativeSql(), 0);
                Function<PgResultSet, Integer> transfer = dataset -> {
                    int rs = dataset.getUpdateEffectCount();
                    conn.offerResultSet(req, dataset);
                    slowLog(s, sinfo.getNativeSql());
                    return rs;
                };
                return pool.writeChannel(conn, transfer, req);
            });
        }
    }

    @Local
    @Override
    public <V> CompletableFuture<V> nativeQueryAsync(
            String sql,
            BiConsumer<Object, Object> consumer,
            Function<DataResultSet, V> handler,
            Map<String, Object> params) {
        long s = System.currentTimeMillis();
        DataNativeSqlStatement sinfo = super.nativeParse(sql, false, null, params);
        final WorkThread workThread = WorkThread.currentWorkThread();
        PgClient pool = readPool();
        Function<PgResultSet, V> transfer = dataset -> {
            V rs = handler.apply(dataset);
            slowLog(s, sinfo.getNativeSql());
            return rs;
        };
        if (!sinfo.isEmptyNamed()) {
            return pool.connect().thenCompose(conn -> {
                PgReqExtended req = conn.pollReqExtended(workThread, null);
                Stream<Serializable> pstream = sinfo.getParamNames().stream().map(n -> (Serializable) params.get(n));
                req.prepareParams(
                        PgClientRequest.REQ_TYPE_EXTEND_QUERY,
                        PgExtendMode.OTHER_NATIVE,
                        sinfo.getNativeSql(),
                        0,
                        sinfo.getParamNames().size(),
                        pstream);
                return pool.writeChannel(conn, transfer, req);
            });
        } else {
            return pool.connect().thenCompose(conn -> {
                PgReqQuery req = conn.pollReqQuery(workThread, null);
                req.prepare(sinfo.getNativeSql());
                return pool.writeChannel(conn, transfer, req);
            });
        }
    }

    public <V> CompletableFuture<Sheet<V>> nativeQuerySheetAsync(
            Class<V> type, String sql, RowBound round, Map<String, Object> params) {
        long s = System.currentTimeMillis();
        DataNativeSqlStatement sinfo = super.nativeParse(sql, true, round, params);
        final WorkThread workThread = WorkThread.currentWorkThread();
        PgClient pool = readPool();
        final String countSql = sinfo.getNativeCountSql();
        Function<PgResultSet, Long> countTransfer = dataset -> {
            long rs = dataset.next() ? dataset.getLongValue(1) : 0;
            slowLog(s, countSql);
            return rs;
        };
        if (!sinfo.isEmptyNamed()) {
            return pool.connect().thenCompose(conn -> {
                PgReqExtended countReq = conn.pollReqExtended(workThread, null);
                Stream<Serializable> pstream = sinfo.getParamNames().stream().map(n -> (Serializable) params.get(n));
                countReq.prepareParams(
                        PgClientRequest.REQ_TYPE_EXTEND_QUERY,
                        PgExtendMode.OTHER_NATIVE,
                        countSql,
                        0,
                        sinfo.getParamNames().size(),
                        pstream);
                return pool.writeChannel(conn, countTransfer, countReq).thenCompose(total -> {
                    if (total < 1) {
                        return CompletableFuture.completedFuture(new Sheet(total, new ArrayList<>()));
                    } else {
                        long s2 = System.currentTimeMillis();
                        String pageSql = sinfo.getNativePageSql();
                        PgReqExtended req = conn.pollReqExtended(workThread, null);
                        Stream<Serializable> pstream2 =
                                sinfo.getParamNames().stream().map(n -> (Serializable) params.get(n));
                        req.prepareParams(
                                PgClientRequest.REQ_TYPE_EXTEND_QUERY,
                                PgExtendMode.OTHER_NATIVE,
                                pageSql,
                                round == null ? 0 : round.getLimit(),
                                sinfo.getParamNames().size(),
                                pstream2);
                        Function<PgResultSet, Sheet<V>> sheetTransfer = dataset -> {
                            List<V> list = EntityBuilder.getListValue(type, dataset);
                            slowLog(s2, pageSql);
                            return new Sheet<>(total, list);
                        };
                        return pool.writeChannel(conn, sheetTransfer, req);
                    }
                });
            });
        } else {
            return pool.connect().thenCompose(conn -> {
                PgReqQuery countReq = conn.pollReqQuery(workThread, null);
                countReq.prepare(countSql);
                return pool.writeChannel(conn, countTransfer, countReq).thenCompose(total -> {
                    if (total < 1) {
                        return CompletableFuture.completedFuture(new Sheet(total, new ArrayList<>()));
                    } else {
                        long s2 = System.currentTimeMillis();
                        String pageSql = sinfo.getNativePageSql();
                        PgReqQuery req = conn.pollReqQuery(workThread, null);
                        req.prepare(pageSql);
                        Function<PgResultSet, Sheet<V>> sheetTransfer = dataset -> {
                            List<V> list = EntityBuilder.getListValue(type, dataset);
                            slowLog(s2, pageSql);
                            return new Sheet<>(total, list);
                        };
                        return pool.writeChannel(conn, sheetTransfer, req);
                    }
                });
            });
        }
    }
}
