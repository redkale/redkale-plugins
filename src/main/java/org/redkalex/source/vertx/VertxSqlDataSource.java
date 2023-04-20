/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import io.vertx.core.*;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.impl.ListTuple;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.logging.Level;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceType;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 * 只实现了 Mysql 和 Postgresql <br>
 * see https://github.com/eclipse-vertx/vertx-sql-client/blob/master/vertx-pg-client/src/main/java/examples/SqlClientExamples.java
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(DataSource.class)
public class VertxSqlDataSource extends AbstractDataSqlSource {

    protected Vertx vertx;

    protected boolean dollar;

    protected SqlConnectOptions readOptions;

    protected PoolOptions readPoolOptions;

    protected Pool readThreadPool;

    protected SqlConnectOptions writeOptions;

    protected PoolOptions writePoolOptions;

    protected Pool writeThreadPool;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        this.dollar = "postgresql".equalsIgnoreCase(dbtype);
        this.vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(Utility.cpus()).setPreferNativeTransport(true));
        {
            this.readOptions = createSqlOptions(readConfProps);
            int readMaxconns = Math.max(1, Integer.decode(readConfProps.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
            this.readPoolOptions = new PoolOptions().setMaxSize(readMaxconns);
            RedkaleClassLoader.putReflectionClass(readOptions.getClass().getName());
            RedkaleClassLoader.putReflectionPublicConstructors(readOptions.getClass(), readOptions.getClass().getName());
        }
        this.readThreadPool = Pool.pool(vertx, readOptions, readPoolOptions);
        if (readConfProps == writeConfProps) {
            this.writeOptions = readOptions;
            this.writePoolOptions = readPoolOptions;
            this.writeThreadPool = this.readThreadPool;
        } else {
            this.writeOptions = createSqlOptions(writeConfProps);
            int writeMaxconns = Math.max(1, Integer.decode(writeConfProps.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
            this.writePoolOptions = new PoolOptions().setMaxSize(writeMaxconns);
            this.writeThreadPool = Pool.pool(vertx, writeOptions, writePoolOptions);
        }
    }

    @Override
    protected void updateOneResourceChange(Properties newProps, ResourceEvent[] events) {
        Pool oldPool = this.readThreadPool;
        SqlConnectOptions readOpt = createSqlOptions(newProps);
        int readMaxconns = Math.max(1, Integer.decode(newProps.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
        PoolOptions readPoolOpt = new PoolOptions().setMaxSize(readMaxconns);
        this.readThreadPool = Pool.pool(vertx, readOpt, readPoolOpt);
        this.readOptions = readOpt;
        this.readPoolOptions = readPoolOpt;

        this.writeOptions = readOptions;
        this.writePoolOptions = readPoolOptions;
        this.writeThreadPool = this.readThreadPool;
        if (oldPool != null) {
            oldPool.close();
        }
    }

    @Override
    protected void updateReadResourceChange(Properties newReadProps, ResourceEvent[] events) {
        Pool oldPool = this.readThreadPool;
        SqlConnectOptions readOpt = createSqlOptions(newReadProps);
        int readMaxconns = Math.max(1, Integer.decode(newReadProps.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
        PoolOptions readPoolOpt = new PoolOptions().setMaxSize(readMaxconns);
        this.readThreadPool = Pool.pool(vertx, readOpt, readPoolOpt);
        this.readOptions = readOpt;
        this.readPoolOptions = readPoolOpt;
        if (oldPool != null) {
            oldPool.close();
        }
    }

    @Override
    protected void updateWriteResourceChange(Properties newWriteProps, ResourceEvent[] events) {
        Pool oldPool = this.writeThreadPool;
        SqlConnectOptions writeOpt = createSqlOptions(newWriteProps);
        int writeMaxconns = Math.max(1, Integer.decode(newWriteProps.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
        PoolOptions writePoolOpt = new PoolOptions().setMaxSize(writeMaxconns);
        this.writeThreadPool = Pool.pool(vertx, writeOpt, writePoolOpt);
        this.writeOptions = writeOpt;
        this.writePoolOptions = writePoolOpt;
        if (oldPool != null) {
            oldPool.close();
        }
    }

    @Override
    protected int readMaxConns() {
        return readPoolOptions.getMaxSize();
    }

    @Override
    protected int writeMaxConns() {
        return writePoolOptions.getMaxSize();
    }

    protected Pool readPool() {
        return readThreadPool;
    }

    protected Pool writePool() {
        return writeThreadPool;
    }

    @Local
    @Override
    public void close() {
        destroy(null);
    }

    @Override
    public void destroy(AnyValue config) {
        super.destroy(config);
        if (this.vertx != null) {
            this.vertx.close();
        }
        if (this.readThreadPool != null) {
            this.readThreadPool.close();
        }
        if (this.writeThreadPool != null && this.writeThreadPool != this.readThreadPool) {
            this.writeThreadPool.close();
        }
    }

    private SqlConnectOptions createSqlOptions(Properties prop) {
        SqlConnectOptions sqlOptions;
        if ("mysql".equalsIgnoreCase(dbtype())) {
            try {
                Class clazz = Thread.currentThread().getContextClassLoader().loadClass("io.vertx.mysqlclient.MySQLConnectOptions");
                RedkaleClassLoader.putReflectionPublicConstructors(clazz, clazz.getName());
                sqlOptions = (SqlConnectOptions) clazz.getConstructor().newInstance();
            } catch (Exception e) {
                throw new SourceException(e);
            }
        } else if ("postgresql".equalsIgnoreCase(dbtype())) {
            try {
                Class clazz = Thread.currentThread().getContextClassLoader().loadClass("io.vertx.pgclient.PgConnectOptions");
                RedkaleClassLoader.putReflectionPublicConstructors(clazz, clazz.getName());
                sqlOptions = (SqlConnectOptions) clazz.getConstructor().newInstance();
            } catch (Exception e) {
                throw new SourceException(e);
            }
        } else {
            throw new UnsupportedOperationException("dbtype(" + dbtype() + ") not supported yet.");
        }
        String url = prop.getProperty(DATA_SOURCE_URL);
        if (url.startsWith("jdbc:")) {
            url = url.substring("jdbc:".length());
        }
        final URI uri = URI.create(url);
        sqlOptions.setHost(uri.getHost());
        if (uri.getPort() > 0) {
            sqlOptions.setPort(uri.getPort());
        }
        String user = prop.getProperty(DATA_SOURCE_USER);
        if (user != null && !user.trim().isEmpty()) {
            sqlOptions.setUser(user.trim());
        }
        String pwd = prop.getProperty(DATA_SOURCE_PASSWORD);
        if (pwd != null && !pwd.trim().isEmpty()) {
            sqlOptions.setPassword(pwd.trim());
        }
        String path = uri.getPath();
        if (path != null && path.length() > 1) {
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            sqlOptions.setDatabase(path);
        }
        sqlOptions.setCachePreparedStatements("true".equalsIgnoreCase(prop.getProperty("preparecache", "true")));
        String query = uri.getQuery();
        if (query != null && !query.isEmpty()) {
            query = query.replace("&amp;", "&");
            for (String str : query.split("&")) {
                if (str.isEmpty()) {
                    continue;
                }
                int pos = str.indexOf('=');
                if (pos < 1) {
                    continue;
                }
                String key = str.substring(0, pos);
                String val = str.substring(pos + 1);
                sqlOptions.addProperty(key, val);
            }
        }
        return sqlOptions;
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
    protected <T> CompletableFuture<Integer> insertDBAsync(EntityInfo<T> info, T... values) {
        final long s = System.currentTimeMillis();
        final Attribute<T, Serializable>[] attrs = info.getInsertAttributes();
        final List<Tuple> objs = new ArrayList<>(values.length);
        for (T value : values) {
            final ListTuple params = new ListTuple(new ArrayList<>());
            for (Attribute<T, Serializable> attr : attrs) {
                params.addValue(attr.get(value));
            }
            objs.add(params);
        }
        final String sql = dollar ? info.getInsertDollarPrepareSQL(values[0]) : info.getInsertQuestionPrepareSQL(values[0]);
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        writePool().preparedQuery(sql).executeBatch(objs, (AsyncResult<RowSet<Row>> event) -> {
            slowLog(s, sql);
            if (event.failed()) {
                future.completeExceptionally(event.cause());
                return;
            }
            future.complete(event.result().rowCount());
        });
        return future;
    }

    @Override
    protected <T> CompletableFuture<Integer> deleteDBAsync(EntityInfo<T> info, String[] tables, Flipper flipper, FilterNode node, Map<String, List<Serializable>> pkmap, String... sqls) {
        if (info.isLoggable(logger, Level.FINEST)) {
            final String debugsql = flipper == null || flipper.getLimit() <= 0 ? sqls[0] : (sqls[0] + " LIMIT " + flipper.getLimit());
            if (info.isLoggable(logger, Level.FINEST, debugsql)) {
                if (sqls.length == 1) {
                    logger.finest(info.getType().getSimpleName() + " delete sql=" + debugsql);
                } else if (flipper == null || flipper.getLimit() <= 0) {
                    logger.finest(info.getType().getSimpleName() + " delete sqls=" + Arrays.toString(sqls));
                } else {
                    logger.finest(info.getType().getSimpleName() + " limit " + flipper.getLimit() + " delete sqls=" + Arrays.toString(sqls));
                }
            }
        }
        return executeUpdate(info, sqls, null, fetchSize(flipper), false, null, null);
    }

    @Override
    protected <T> CompletableFuture<Integer> clearTableDBAsync(EntityInfo<T> info, final String[] tables, FilterNode node, String... sqls) {
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
    protected <T> CompletableFuture<Integer> createTableDBAsync(EntityInfo<T> info, String copyTableSql, final Serializable pk, String... sqls) {
        if (copyTableSql == null) {
            return executeUpdate(info, sqls, null, 0, false, null, null);
        } else {
            return executeUpdate(info, new String[]{copyTableSql}, null, 0, false, null, null);
        }
    }

    @Override
    protected <T> CompletableFuture<Integer> dropTableDBAsync(EntityInfo<T> info, final String[] tables, FilterNode node, String... sqls) {
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
        final List<Tuple> objs = new ArrayList<>(values.length);
        for (T value : values) {
            final ListTuple params = new ListTuple(new ArrayList<>(attrs.length + 1));
            for (Attribute<T, Serializable> attr : attrs) {
                params.addValue(attr.get(value));
            }
            params.addValue(primary.get(value));   //最后一个是主键
            objs.add(params);
        }
        final String sql = dollar ? info.getUpdateDollarPrepareSQL(values[0]) : info.getUpdateQuestionPrepareSQL(values[0]);
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        writePool().preparedQuery(sql).executeBatch(objs, (AsyncResult<RowSet<Row>> event) -> {
            slowLog(s, sql);
            if (event.failed()) {
                future.completeExceptionally(event.cause());
                return;
            }
            future.complete(event.result().rowCount());
        });
        return future;
    }

    @Override
    protected <T> CompletableFuture<Integer> updateColumnDBAsync(EntityInfo<T> info, Flipper flipper, UpdateSqlInfo sql) {
        if (info.isLoggable(logger, Level.FINEST)) {
            final String debugsql = flipper == null || flipper.getLimit() <= 0 ? sql.sql : (sql.sql + " LIMIT " + flipper.getLimit());
            if (info.isLoggable(logger, Level.FINEST, debugsql)) {
                logger.finest(info.getType().getSimpleName() + " update sql=" + debugsql);
            }
        }
        List<Tuple> objs = null;
        if (sql.blobs != null || sql.tables != null) {
            if (sql.tables == null) {
                objs = List.of(Tuple.wrap(sql.blobs));
            } else {
                objs = new ArrayList<>();
                for (String table : sql.tables) {
                    if (sql.blobs != null) {
                        List w = new ArrayList(sql.blobs);
                        w.add(table);
                        objs.add(Tuple.wrap(w));
                    } else {
                        objs.add(Tuple.of(table));
                    }
                }
            }
        }
        //有params的情况表示 prepareSQL带byte[]的绑定参数
        return executeUpdate(info, new String[]{sql.sql}, null, fetchSize(flipper), false, null, objs);
    }

    @Override
    protected <T, N extends Number> CompletableFuture<Map<String, N>> getNumberMapDBAsync(EntityInfo<T> info, String[] tables, String sql, FilterNode node, FilterFuncColumn... columns) {
        return queryResultSet(info, sql).thenApply((VertxResultSet set) -> {
            final Map map = new HashMap<>();
            if (set.next()) {
                int index = 0;
                for (FilterFuncColumn ffc : columns) {
                    for (String col : ffc.cols()) {
                        Object o = set.getObject(++index);
                        Number rs = ffc.getDefvalue();
                        if (o != null) {
                            rs = (Number) o;
                        }
                        map.put(ffc.col(col), rs);
                    }
                }
            }
            return map;
        });
    }

    @Override
    protected <T> CompletableFuture<Number> getNumberResultDBAsync(EntityInfo<T> info, String[] tables, String sql, FilterFunc func, Number defVal, String column, final FilterNode node) {
        return queryResultSet(info, sql).thenApply((VertxResultSet set) -> {
            Number rs = defVal;
            if (set.next()) {
                Object o = set.getObject(1);
                if (o != null) {
                    rs = (Number) o;
                }
            }
            return rs;
        });
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N>> queryColumnMapDBAsync(EntityInfo<T> info, String[] tables, String sql, String keyColumn, FilterFunc func, String funcColumn, FilterNode node) {
        return queryResultSet(info, sql).thenApply((VertxResultSet set) -> {
            Map<K, N> rs = new LinkedHashMap<>();
            while (set.next()) {
                rs.put((K) set.getObject(1), (N) set.getObject(2));
            }
            return rs;
        });
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K[], N[]>> queryColumnMapDBAsync(EntityInfo<T> info, String[] tables, String sql, final ColumnNode[] funcNodes, final String[] groupByColumns, FilterNode node) {
        return queryResultSet(info, sql).thenApply((VertxResultSet set) -> {
            Map rs = new LinkedHashMap<>();
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
            return rs;
        });
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

        if (selects == null) {
            final String sql = dollar ? info.getFindDollarPrepareSQL(pk) : info.getFindQuestionPrepareSQL(pk);
            return queryPrepareResultSet(info, sql, Tuple.of(pk)).thenApply(rsset -> {
                boolean rs = rsset.next();
                T val = rs ? getEntityValue(info, null, rsset) : null;
                return val;
            });
        }
        String sql = findSql(info, selects, pk);
        if (info.isLoggable(logger, Level.FINEST, sql)) {
            logger.finest(info.getType().getSimpleName() + " find sql=" + sql);
        }
        return queryResultSet(info, sql).thenApply(rsset -> {
            boolean rs = rsset.next();
            T val = rs ? (selects == null ? getEntityValue(info, null, rsset) : getEntityValue(info, selects, rsset)) : null;
            return val;
        });
    }

    @Override
    protected <T> CompletableFuture<T> findDBAsync(EntityInfo<T> info, String[] tables, String sql, boolean onlypk, SelectColumn selects, Serializable pk, FilterNode node) {
        return queryResultSet(info, sql).thenApply(rsset -> {
            boolean rs = rsset.next();
            T val = rs ? (onlypk && selects == null ? getEntityValue(info, null, rsset) : getEntityValue(info, selects, rsset)) : null;
            return val;
        });
    }

    @Override
    protected <T> CompletableFuture<Serializable> findColumnDBAsync(EntityInfo<T> info, final String[] tables, String sql, boolean onlypk, String column, Serializable defValue, Serializable pk, FilterNode node) {
        return queryResultSet(info, sql).thenApply((VertxResultSet set) -> {
            Serializable val = defValue;
            if (set.next()) {
                final Attribute<T, Serializable> attr = info.getAttribute(column);
                val = set.getObject(attr, 1, null);
            }
            return val == null ? defValue : val;
        });
    }

    @Override
    protected <T> CompletableFuture<Boolean> existsDBAsync(EntityInfo<T> info, final String[] tables, String sql, boolean onlypk, Serializable pk, FilterNode node) {
        return queryResultSet(info, sql).thenApply((VertxResultSet set) -> {
            return set.next() ? (((Number) set.getObject(1)).intValue() > 0) : false;
        });
    }

    @Override
    protected <T> CompletableFuture<Sheet<T>> querySheetDBAsync(EntityInfo<T> info, final boolean readcache, boolean needtotal, final boolean distinct, SelectColumn selects, Flipper flipper, FilterNode node) {
        final SelectColumn sels = selects;
        final Map<Class, String> joinTabalis = node == null ? null : getJoinTabalis(node);
        final CharSequence join = node == null ? null : createSQLJoin(node, this, false, joinTabalis, new HashSet<>(), info);
        final CharSequence where = node == null ? null : createSQLExpress(node, info, joinTabalis);
        String[] tables = info.getTables(node);
        String joinAndWhere = (join == null ? "" : join) + ((where == null || where.length() == 0) ? "" : (" WHERE " + where));
        String listsubsql;
        StringBuilder union = new StringBuilder();
        if (tables.length == 1) {
            listsubsql = "SELECT " + (distinct ? "DISTINCT " : "") + info.getQueryColumns("a", selects) + " FROM " + tables[0] + " a" + joinAndWhere;
        } else {
            int b = 0;
            for (String table : tables) {
                if (union.length() > 0) {
                    union.append(" UNION ALL ");
                }
                String tabalis = "t" + (++b);
                union.append("SELECT ").append(info.getQueryColumns(tabalis, selects)).append(" FROM ").append(table).append(" ").append(tabalis).append(joinAndWhere);
            }
            listsubsql = "SELECT " + (distinct ? "DISTINCT " : "") + info.getQueryColumns("a", selects) + " FROM (" + (union) + ") a";
        }
        final String listsql = listsubsql + createSQLOrderby(info, flipper) + (flipper == null || flipper.getLimit() < 1 ? "" : (" LIMIT " + flipper.getLimit() + " OFFSET " + flipper.getOffset()));
        if (readcache && info.isLoggable(logger, Level.FINEST, listsql)) {
            logger.finest(info.getType().getSimpleName() + " query sql=" + listsql);
        }
        if (!needtotal) {
            CompletableFuture<VertxResultSet> listfuture = queryResultSet(info, listsql);
            return listfuture.thenApply((VertxResultSet set) -> {
                final List<T> list = new ArrayList();
                while (set.next()) {
                    list.add(getEntityValue(info, sels, set));
                }
                Sheet sheet = Sheet.asSheet(list);
                return sheet;
            });
        }
        String countsubsql;
        if (tables.length == 1) {
            countsubsql = "SELECT " + (distinct ? "DISTINCT COUNT(" + info.getQueryColumns("a", selects) + ")" : "COUNT(*)") + " FROM " + tables[0] + " a" + joinAndWhere;
        } else {
            countsubsql = "SELECT " + (distinct ? "DISTINCT COUNT(" + info.getQueryColumns("a", selects) + ")" : "COUNT(*)") + " FROM (" + (union) + ") a";
        }
        final String countsql = countsubsql;
        return getNumberResultDBAsync(info, null, countsql, distinct ? FilterFunc.DISTINCTCOUNT : FilterFunc.COUNT, 0, countsql, node).thenCompose(total -> {
            if (total.longValue() <= 0) {
                return CompletableFuture.completedFuture(new Sheet<>(0, new ArrayList()));
            }
            return queryResultSet(info, listsql).thenApply((VertxResultSet set) -> {
                final List<T> list = new ArrayList();
                while (set.next()) {
                    list.add(getEntityValue(info, sels, set));
                }
                return new Sheet(total.longValue(), list);
            });
        });
    }

    private static int fetchSize(Flipper flipper) {
        return flipper == null || flipper.getLimit() <= 0 ? 0 : flipper.getLimit();
    }

    protected <T> CompletableFuture<Integer> executeUpdate(final EntityInfo<T> info, final String[] sqls, final T[] values, int fetchSize, final boolean insert, final Attribute<T, Serializable>[] attrs, final List<Tuple> parameters) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        final long s = System.currentTimeMillis();
        if (sqls.length == 1) {
            if (parameters != null && !parameters.isEmpty()) {
                writePool().preparedQuery(sqls[0]).executeBatch(parameters, (AsyncResult<RowSet<Row>> event) -> {
                    slowLog(s, sqls);
                    if (event.failed()) {
                        future.completeExceptionally(event.cause());
                        return;
                    }
                    future.complete(event.result().rowCount());
                });
            } else {
                writePool().query(sqls[0]).execute((AsyncResult<RowSet<Row>> event) -> {
                    slowLog(s, sqls);
                    if (event.failed()) {
                        future.completeExceptionally(event.cause());
                        return;
                    }
                    future.complete(event.result().rowCount());
                });
            }
        } else {
            final int[] rs = new int[sqls.length];
            writePool().withTransaction(conn -> {
                CompletableFuture[] futures = new CompletableFuture[sqls.length];
                for (int i = 0; i < sqls.length; i++) {
                    final int index = i;
                    futures[i] = conn.query(sqls[i]).execute().map(rset -> {
                        int c = rset.rowCount();
                        rs[index] = c;
                        return c;
                    }).toCompletionStage().toCompletableFuture();
                }
                return io.vertx.core.Future.fromCompletionStage(CompletableFuture.allOf(futures));
            }).toCompletionStage().whenComplete((v, t) -> {
                if (t != null) {
                    future.completeExceptionally(t);
                } else {
                    int c = 0;
                    for (int cc : rs) {
                        c += cc;
                    }
                    future.complete(c);
                }
            });
        }
        return future;
    }

    //info不可以为null
    protected <T> CompletableFuture<VertxResultSet> queryPrepareResultSet(final EntityInfo<T> info, final String sql, Tuple tuple) {
        final long s = System.currentTimeMillis();
        final CompletableFuture<VertxResultSet> future = new CompletableFuture<>();
        readPool().preparedQuery(sql).execute(tuple, newQueryHandler(s, sql, info, future));
        return future;
    }

    //info可以为null,供directQuery
    protected <T> CompletableFuture<VertxResultSet> queryResultSet(final EntityInfo<T> info, final String sql) {
        final long s = System.currentTimeMillis();
        final CompletableFuture<VertxResultSet> future = new CompletableFuture<>();
        readPool().query(sql).execute(newQueryHandler(s, sql, info, future));
        return future;
    }

    protected <T> io.vertx.core.Handler<AsyncResult<RowSet<Row>>> newQueryHandler(long s, String sql, final EntityInfo<T> info, final CompletableFuture<VertxResultSet> future) {
        return (AsyncResult<RowSet<Row>> event) -> {
            slowLog(s, sql);
            if (event.failed()) {
                final Throwable ex = event.cause();
                if (info == null || !isTableNotExist(info, ex)) {
                    future.completeExceptionally(ex);
                } else {  //表不存在
                    if (info.getTableStrategy() == null) {  //没有原始表
                        String[] tablesqls = createTableSqls(info);
                        if (tablesqls == null) { //没有建表DDL
                            future.completeExceptionally(ex);
                        } else {
                            writePool().query(tablesqls[0]).execute((AsyncResult<RowSet<Row>> event2) -> {
                                if (event2.failed()) {
                                    future.completeExceptionally(event2.cause());
                                } else {
                                    future.complete(new VertxResultSet(info, null, null));
                                }
                            });
                        }
                    } else {  //没有分表
                        future.complete(new VertxResultSet(info, null, null));
                    }
                }
            } else {
                future.complete(new VertxResultSet(info, null, event.result()));
            }
        };
    }

    protected <T> boolean isTableNotExist(EntityInfo<T> info, Throwable t) {
        String code = null;
        if ("postgresql".equals(dbtype())) {
            if (t instanceof io.vertx.pgclient.PgException) {
                code = ((io.vertx.pgclient.PgException) t).getCode();
            }
        } else if ("mysql".equals(dbtype())) {
            if (t instanceof io.vertx.mysqlclient.MySQLException) {
                code = ((io.vertx.mysqlclient.MySQLException) t).getSqlState();
            }
        }
        if (code == null) {
            return false;
        }
        return super.isTableNotExist(info, code);
    }

    @Local
    @Override
    public int nativeExecute(String sql) {
        return executeUpdate(null, new String[]{sql}, null, 0, false, null, null).join();
    }

    @Local
    @Override
    public int[] nativeExecute(final String... sqls) {
        final long s = System.currentTimeMillis();
        final int[] rs = new int[sqls.length];
        writePool().withTransaction(conn -> {
            CompletableFuture[] futures = new CompletableFuture[rs.length];
            for (int i = 0; i < rs.length; i++) {
                final int index = i;
                futures[i] = conn.query(sqls[i]).execute().map(rset -> {
                    int c = rset.rowCount();
                    rs[index] = c;
                    return c;
                }).toCompletionStage().toCompletableFuture();
            }
            return io.vertx.core.Future.fromCompletionStage(CompletableFuture.allOf(futures));
        }).toCompletionStage().toCompletableFuture().join();
        slowLog(s, sqls);
        return rs;
    }

    @Local
    @Override
    public <V> V nativeQuery(String sql, Function<DataResultSet, V> handler) {
        final long s = System.currentTimeMillis();
        return queryResultSet(null, sql).thenApply((VertxResultSet set) -> {
            slowLog(s, sql);
            return handler.apply(set);
        }).join();
    }

}
