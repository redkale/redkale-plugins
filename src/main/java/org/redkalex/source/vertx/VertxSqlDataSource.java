/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.vertx;

import io.vertx.core.*;
import io.vertx.core.metrics.MetricsOptions;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.impl.ListTuple;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.logging.Level;
import java.util.stream.Stream;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Nullable;
import org.redkale.annotation.ResourceType;
import org.redkale.inject.ResourceEvent;
import org.redkale.net.WorkThread;
import org.redkale.service.Local;
import org.redkale.source.*;
import static org.redkale.source.DataSources.*;
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

    protected static final PropertyKind<Long> MYSQL_LAST_INSERTED_ID = PropertyKind.create("last-inserted-id", Long.class);

    protected Vertx vertx;

    protected boolean dollar;

    protected SqlConnectOptions readOptions;

    protected PoolOptions readPoolOptions;

    protected Pool readThreadPool;

    protected SqlConnectOptions writeOptions;

    protected PoolOptions writePoolOptions;

    protected Pool writeThreadPool;

    protected boolean pgsql;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        this.dollar = "postgresql".equalsIgnoreCase(dbtype);
        this.pgsql = this.dollar;
        this.vertx = createVertx();
        {
            this.readOptions = createSqlOptions(readConfProps);
            int readMaxconns = Math.max(1, Integer.decode(readConfProps.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
            this.readPoolOptions = createPoolOptions(readMaxconns);
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
            this.writePoolOptions = createPoolOptions(writeMaxconns);
            this.writeThreadPool = Pool.pool(vertx, writeOptions, writePoolOptions);
        }
    }

    protected Vertx createVertx() {
        return Vertx.vertx(new VertxOptions()
            .setEventLoopPoolSize(Utility.cpus())
            .setPreferNativeTransport(true)
            .setDisableTCCL(true)
            .setHAEnabled(false)
            .setBlockedThreadCheckIntervalUnit(TimeUnit.HOURS)
            .setMetricsOptions(new MetricsOptions().setEnabled(false))
        );
    }

    public boolean isPgsql() {
        return pgsql;
    }

    @Override

    protected void updateOneResourceChange(Properties newProps, ResourceEvent[] events) {
        Pool oldPool = this.readThreadPool;
        SqlConnectOptions readOpt = createSqlOptions(newProps);
        int readMaxconns = Math.max(1, Integer.decode(newProps.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
        PoolOptions readPoolOpt = createPoolOptions(readMaxconns);
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
        PoolOptions readPoolOpt = createPoolOptions(readMaxconns);
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
        PoolOptions writePoolOpt = createPoolOptions(writeMaxconns);
        this.writeThreadPool = Pool.pool(vertx, writeOpt, writePoolOpt);
        this.writeOptions = writeOpt;
        this.writePoolOptions = writePoolOpt;
        if (oldPool != null) {
            oldPool.close();
        }
    }

    private PoolOptions createPoolOptions(int maxConns) {
        PoolOptions options = new PoolOptions()
            .setEventLoopSize(Utility.cpus())
            .setMaxSize(maxConns);
        try {
            if ("mysql".equalsIgnoreCase(dbtype())) {
                Class myclass = Class.forName("io.vertx.mysqlclient.impl.MySQLPoolOptions");
                Object myopts = myclass.getConstructor(PoolOptions.class).newInstance(options);
                Method method = myclass.getMethod("setPipelined", boolean.class);
                method.invoke(myopts, true);
                RedkaleClassLoader.putReflectionClass(myclass.getName());
                RedkaleClassLoader.putReflectionPublicConstructors(myclass, myclass.getName());
                RedkaleClassLoader.putReflectionMethod(myclass.getName(), method);
                return (PoolOptions) myopts;
            } else if ("postgresql".equalsIgnoreCase(dbtype())) {
                Class myclass = Class.forName("io.vertx.pgclient.impl.PgPoolOptions");
                Object myopts = myclass.getConstructor(PoolOptions.class).newInstance(options);
                Method method = myclass.getMethod("setPipelined", boolean.class);
                method.invoke(myopts, true);
                RedkaleClassLoader.putReflectionClass(myclass.getName());
                RedkaleClassLoader.putReflectionPublicConstructors(myclass, myclass.getName());
                RedkaleClassLoader.putReflectionMethod(myclass.getName(), method);
                return (PoolOptions) myopts;
            } else {
                return options;
            }
        } catch (Throwable t) {
            return options;
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
                Method method = sqlOptions.getClass().getMethod("setPipeliningLimit", int.class);
                method.invoke(sqlOptions, 100000);
            } catch (Exception e) {
                throw new SourceException(e);
            }
        } else if ("postgresql".equalsIgnoreCase(dbtype())) {
            try {
                Class clazz = Thread.currentThread().getContextClassLoader().loadClass("io.vertx.pgclient.PgConnectOptions");
                RedkaleClassLoader.putReflectionPublicConstructors(clazz, clazz.getName());
                sqlOptions = (SqlConnectOptions) clazz.getConstructor().newInstance();
                Method method = sqlOptions.getClass().getMethod("setPipeliningLimit", int.class);
                method.invoke(sqlOptions, 100000);
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
        return dollar ? ("$" + index) : "?";
    }

    @Override
    protected final boolean isAsync() {
        return true;
    }

    @Override
    protected <T> CompletableFuture<Integer> insertDBAsync(EntityInfo<T> info, T... values) {
        final long s = System.currentTimeMillis();
        final WorkThread workThread = WorkThread.currentWorkThread();
        final Attribute<T, Serializable>[] attrs = info.getInsertAttributes();
        final List<Tuple> objs = new ArrayList<>(values.length);
        for (T value : values) {
            final ListTuple params = new ListTuple(new ArrayList<>());
            for (Attribute<T, Serializable> attr : attrs) {
                params.addValue(attr.get(value));
            }
            objs.add(params);
        }
        String sql0 = dollar ? info.getInsertDollarPrepareSQL(values[0]) : info.getInsertQuestionPrepareSQL(values[0]);
        final String sql = info.isAutoGenerated() && isPgsql() ? (sql0 + " RETURNING " + info.getPrimarySQLColumn()) : sql0;
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        final ObjectRef<Handler<AsyncResult<RowSet<Row>>>> selfHandlerRef = new ObjectRef<>();
        final Handler<AsyncResult<RowSet<Row>>> handler = (AsyncResult<RowSet<Row>> event) -> {
            slowLog(s, sql);
            if (event.failed()) {
                if (!isTableNotExist(info, event.cause())) {
                    completeExceptionally(workThread, future, event.cause());
                    return;
                }
                if (info.getTableStrategy() == null) { //单表模式
                    String[] tableSqls = createTableSqls(info);
                    if (tableSqls == null) { //没有建表DDL
                        completeExceptionally(workThread, future, event.cause());
                        return;
                    }
                    //创建单表结构
                    AtomicInteger createIndex = new AtomicInteger();
                    final ObjectRef<Handler<AsyncResult<RowSet<Row>>>> createHandlerRef = new ObjectRef<>();
                    final Handler<AsyncResult<RowSet<Row>>> createHandler = (AsyncResult<RowSet<Row>> event2) -> {
                        if (event2.failed()) {
                            completeExceptionally(workThread, future, event2.cause());
                        } else if (createIndex.incrementAndGet() < tableSqls.length) {
                            writePool().query(tableSqls[createIndex.get()]).execute(createHandlerRef.get());
                        } else {
                            //重新提交新增记录
                            writePool().preparedQuery(sql).executeBatch(objs, selfHandlerRef.get());
                        }
                    };
                    createHandlerRef.set(createHandler);
                    writePool().query(tableSqls[createIndex.get()]).execute(createHandler);
                } else { //分表模式
                    //执行一遍复制表操作
                    final String copySql = getTableCopySQL(info, info.getTable(values[0]));
                    final ObjectRef<Handler<AsyncResult<RowSet<Row>>>> copySqlHandlerRef = new ObjectRef<>();
                    final Handler<AsyncResult<RowSet<Row>>> copySqlHandler = (AsyncResult<RowSet<Row>> event2) -> {
                        if (event2.failed()) {
                            completeExceptionally(workThread, future, event2.cause());
                        } else {
                            //重新提交新增记录
                            writePool().preparedQuery(sql).executeBatch(objs, selfHandlerRef.get());
                        }
                    };
                    copySqlHandlerRef.set(copySqlHandler);
                    writePool().query(copySql).execute(copySqlHandler);
                }
                return;
            }
            if (info.isAutoGenerated()) {
                int i = -1;
                RowSet<Row> res = event.result();
                final Attribute primary = info.getPrimary();
                final Class primaryType = primary.type();
                if (isPgsql()) {
                    for (RowSet<Row> rows = res; rows != null; rows = rows.next()) {
                        T entity = values[++i];
                        Row row = rows.iterator().next();
                        if (primaryType == int.class || primaryType == Integer.class) {
                            primary.set(entity, row.getInteger(0));
                        } else if (primaryType == long.class || primaryType == long.class) {
                            primary.set(entity, row.getLong(0));
                        } else if (primaryType == String.class) {
                            primary.set(entity, row.getString(0));
                        } else {
                            primary.set(entity, row.get(primaryType, 0));
                        }
                    }
                } else {
                    long firstId = res.property(MYSQL_LAST_INSERTED_ID);
                    for (T entity : values) {
                        long id = firstId + (++i);
                        if (primaryType == int.class || primaryType == Integer.class) {
                            primary.set(entity, (int) id);
                        } else if (primaryType == long.class || primaryType == long.class) {
                            primary.set(entity, id);
                        } else if (primaryType == String.class) {
                            primary.set(entity, String.valueOf(id));
                        } else {
                            primary.set(entity, id);
                        }
                    }
                }
            }
            complete(workThread, future, event.result().rowCount());
        };
        selfHandlerRef.set(handler);
        writePool().preparedQuery(sql).executeBatch(objs, handler);
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
        final WorkThread workThread = WorkThread.currentWorkThread();
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
                completeExceptionally(workThread, future, event.cause());
                return;
            }
            complete(workThread, future, event.result().rowCount());
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
        final WorkThread workThread = WorkThread.currentWorkThread();
        return readResultSet(workThread, info, sql).thenApply((VertxResultSet set) -> {
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
        final WorkThread workThread = WorkThread.currentWorkThread();
        return readResultSet(workThread, info, sql).thenApply((VertxResultSet set) -> {
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
        final WorkThread workThread = WorkThread.currentWorkThread();
        return readResultSet(workThread, info, sql).thenApply((VertxResultSet set) -> {
            Map<K, N> rs = new LinkedHashMap<>();
            while (set.next()) {
                rs.put((K) set.getObject(1), (N) set.getObject(2));
            }
            return rs;
        });
    }

    @Override
    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map<K[], N[]>> queryColumnMapDBAsync(EntityInfo<T> info, String[] tables, String sql, final ColumnNode[] funcNodes, final String[] groupByColumns, FilterNode node) {
        final WorkThread workThread = WorkThread.currentWorkThread();
        return readResultSet(workThread, info, sql).thenApply((VertxResultSet set) -> {
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

        final WorkThread workThread = WorkThread.currentWorkThread();
        if (selects == null) {
            final String sql = dollar ? info.getFindDollarPrepareSQL(pk) : info.getFindQuestionPrepareSQL(pk);
            return readPrepareResultSet(workThread, info, sql, Tuple.of(pk)).thenApply(rsset -> {
                boolean rs = rsset.next();
                T val = rs ? getEntityValue(info, null, rsset) : null;
                return val;
            });
        }
        String sql = findSql(info, selects, pk);
        if (info.isLoggable(logger, Level.FINEST, sql)) {
            logger.finest(info.getType().getSimpleName() + " find sql=" + sql);
        }
        return readResultSet(workThread, info, sql).thenApply(rsset -> {
            boolean rs = rsset.next();
            T val = rs ? getEntityValue(info, selects, rsset) : null;
            return val;
        });
    }

    @Override
    public <D extends Serializable, T> CompletableFuture<List<T>> findsListAsync(final Class<T> clazz, final Stream<D> pks) {
        final EntityInfo<T> info = loadEntityInfo(clazz);
        final EntityCache<T> cache = info.getCache();
        Serializable[] ids = pks.toArray(serialArrayFunc);
        if (cache != null) {
            T[] rs = cache.finds(ids);
            if (cache.isFullLoaded() || rs != null) {
                return CompletableFuture.completedFuture(Arrays.asList(rs));
            }
        }
        if (ids.length == 0) {
            return CompletableFuture.completedFuture(new ArrayList<>());
        }
        final String sql = dollar ? info.getFindDollarPrepareSQL(ids[0]) : info.getFindQuestionPrepareSQL(ids[0]);
        final long s = System.currentTimeMillis();
        final WorkThread workThread = WorkThread.currentWorkThread();
        final PreparedQuery<RowSet<Row>> query = readPool().preparedQuery(sql);
        final T[] array = Creator.newArray(clazz, ids.length);
        final CompletableFuture<List<T>> future = new CompletableFuture<>();
        final AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < ids.length; i++) {
            final int index = i;
            query.execute(Tuple.of(ids[index]), (AsyncResult<RowSet<Row>> event) -> {
                slowLog(s, sql);
                if (event.failed()) {
                    final Throwable ex = event.cause();
                    if (!isTableNotExist(info, ex)) {
                        completeExceptionally(workThread, future, ex);
                    } else {  //表不存在
                        if (info.getTableStrategy() == null) {  //没有原始表
                            String[] tablesqls = createTableSqls(info);
                            if (tablesqls == null) { //没有建表DDL
                                completeExceptionally(workThread, future, ex);
                            } else {
                                array[index] = null;
                                if (count.incrementAndGet() == ids.length) {
                                    complete(workThread, future, Arrays.asList(array));
                                }
                            }
                        } else {  //没有分表
                            array[index] = null;
                            if (count.incrementAndGet() == ids.length) {
                                complete(workThread, future, Arrays.asList(array));
                            }
                        }
                    }
                } else {
                    VertxResultSet vrs = new VertxResultSet(info, null, event.result());
                    if (vrs.next()) {
                        array[index] = getEntityValue(info, null, vrs);
                    } else {
                        array[index] = null;
                    }
                    if (count.incrementAndGet() == ids.length) {
                        complete(workThread, future, Arrays.asList(array));
                    }
                }
            });
        }
        return future;
    }

    @Override
    protected <T> CompletableFuture<T> findDBAsync(EntityInfo<T> info, String[] tables, String sql, boolean onlypk, SelectColumn selects, Serializable pk, FilterNode node) {
        final WorkThread workThread = WorkThread.currentWorkThread();
        return readResultSet(workThread, info, sql).thenApply(rsset -> {
            boolean rs = rsset.next();
            T val = rs ? (onlypk && selects == null ? getEntityValue(info, null, rsset) : getEntityValue(info, selects, rsset)) : null;
            return val;
        });
    }

    @Override
    protected <T> CompletableFuture<Serializable> findColumnDBAsync(EntityInfo<T> info, final String[] tables, String sql, boolean onlypk, String column, Serializable defValue, Serializable pk, FilterNode node) {
        final WorkThread workThread = WorkThread.currentWorkThread();
        return readResultSet(workThread, info, sql).thenApply((VertxResultSet set) -> {
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
        final WorkThread workThread = WorkThread.currentWorkThread();
        return readResultSet(workThread, info, sql).thenApply((VertxResultSet set) -> {
            return set.next() ? (((Number) set.getObject(1)).intValue() > 0) : false;
        });
    }

    @Override
    protected <T> CompletableFuture<Sheet<T>> querySheetDBAsync(EntityInfo<T> info, final boolean readcache, boolean needtotal, final boolean distinct, SelectColumn selects, Flipper flipper, FilterNode node) {
        final SelectColumn sels = selects;
        final Map<Class, String> joinTabalis = node == null ? null : getJoinTabalis(node);
        final CharSequence join = node == null ? null : createSQLJoin(node, this, false, joinTabalis, new HashSet<>(), info);
        final CharSequence where = node == null ? null : createSQLExpress(node, info, joinTabalis);
        final WorkThread workThread = WorkThread.currentWorkThread();
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
        final String listSql = listsubsql + createSQLOrderby(info, flipper) + (flipper == null || flipper.getLimit() < 1 ? "" : (" LIMIT " + flipper.getLimit() + " OFFSET " + flipper.getOffset()));
        if (readcache && info.isLoggable(logger, Level.FINEST, listSql)) {
            logger.finest(info.getType().getSimpleName() + " query sql=" + listSql);
        }
        if (!needtotal) {
            CompletableFuture<VertxResultSet> listfuture = readResultSet(workThread, info, listSql);
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
            return readResultSet(workThread, info, listSql).thenApply((VertxResultSet set) -> {
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
        final WorkThread workThread = WorkThread.currentWorkThread();
        if (sqls.length == 1) {
            if (parameters != null && !parameters.isEmpty()) {
                writePool().preparedQuery(sqls[0]).executeBatch(parameters, (AsyncResult<RowSet<Row>> event) -> {
                    slowLog(s, sqls);
                    if (event.failed()) {
                        completeExceptionally(workThread, future, event.cause());
                        return;
                    }
                    complete(workThread, future, event.result().rowCount());
                });
            } else {
                writePool().query(sqls[0]).execute((AsyncResult<RowSet<Row>> event) -> {
                    slowLog(s, sqls);
                    if (event.failed()) {
                        completeExceptionally(workThread, future, event.cause());
                        return;
                    }
                    complete(workThread, future, event.result().rowCount());
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
                    completeExceptionally(workThread, future, t);
                } else {
                    int c = 0;
                    for (int cc : rs) {
                        c += cc;
                    }
                    complete(workThread, future, c);
                }
            });
        }
        return future;
    }

    //info不可以为null
    protected <T> CompletableFuture<VertxResultSet> readPrepareResultSet(final WorkThread workThread, final EntityInfo<T> info, final String sql, Tuple tuple) {
        final long s = System.currentTimeMillis();
        final CompletableFuture<VertxResultSet> future = new CompletableFuture<>();
        PreparedQuery<RowSet<Row>> query;
        if (workThread != null && workThread.inIO()) {
            query = info.getSubobjectIfAbsent(resourceName() + ":" + sql, n -> readPool().preparedQuery(sql));
        } else {
            query = readPool().preparedQuery(sql);
        }
        query.execute(tuple, newQueryHandler(s, workThread, sql, info, future));
        return future;
    }

    //info可以为null,供directQuery
    protected <T> CompletableFuture<VertxResultSet> readResultSet(final WorkThread workThread, @Nullable EntityInfo<T> info, final String sql) {
        final long s = System.currentTimeMillis();
        final CompletableFuture<VertxResultSet> future = new CompletableFuture<>();
        readPool().query(sql).execute(newQueryHandler(s, workThread, sql, info, future));
        return future;
    }

    protected <T> io.vertx.core.Handler<AsyncResult<RowSet<Row>>> newQueryHandler(long s,
        WorkThread workThread, String sql, final EntityInfo<T> info, final CompletableFuture<VertxResultSet> future) {
        return (AsyncResult<RowSet<Row>> event) -> {
            slowLog(s, sql);
            if (event.failed()) {
                final Throwable ex = event.cause();
                if (info == null || !isTableNotExist(info, ex)) {
                    completeExceptionally(workThread, future, ex);
                } else {  //表不存在
                    if (info.getTableStrategy() == null) {  //没有原始表
                        String[] tablesqls = createTableSqls(info);
                        if (tablesqls == null) { //没有建表DDL
                            completeExceptionally(workThread, future, ex);
                        } else {
                            writePool().query(tablesqls[0]).execute((AsyncResult<RowSet<Row>> event2) -> {
                                if (event2.failed()) {
                                    completeExceptionally(workThread, future, event2.cause());
                                } else {
                                    complete(workThread, future, new VertxResultSet(info, null, null));
                                }
                            });
                        }
                    } else {  //没有分表
                        complete(workThread, future, new VertxResultSet(info, null, null));
                    }
                }
            } else {
                complete(workThread, future, new VertxResultSet(info, null, event.result()));
            }
        };
    }

    protected <T> boolean isTableNotExist(EntityInfo<T> info, Throwable t) {
        String code = null;
        if ("postgresql".equals(dbtype())) {
            if (t.getClass().getName().equals("io.vertx.pgclient.PgException")) {
                code = ((io.vertx.pgclient.PgException) t).getSqlState();
            }
        } else if ("mysql".equals(dbtype())) {
            if (t.getClass().getName().equals("io.vertx.mysqlclient.MySQLException")) {
                code = ((io.vertx.mysqlclient.MySQLException) t).getSqlState();
            }
            if ("42000".equals(code)) {
                return false;
            }
            if ("42S02".equals(code)) {
                return true;
            }
        }
        if (code == null) {
            return false;
        }
        return super.isTableNotExist(info, code);
    }

    @Local
    @Override
    public CompletableFuture<Integer> nativeUpdateAsync(String sql) {
        return nativeUpdatesAsync(new String[]{sql}).thenApply(v -> v[0]);
    }

    @Local
    @Override
    public CompletableFuture<int[]> nativeUpdatesAsync(String... sqls) {
        final long s = System.currentTimeMillis();
        final WorkThread workThread = WorkThread.currentWorkThread();
        final int[] rs = new int[sqls.length];
        CompletableFuture[] futures = new CompletableFuture[rs.length];
        return writePool().withTransaction(conn -> {
            for (int i = 0; i < rs.length; i++) {
                final int index = i;
                futures[i] = conn.query(sqls[i]).execute().map(rset -> {
                    int c = rset.rowCount();
                    rs[index] = c;
                    return c;
                }).toCompletionStage().toCompletableFuture();
            }
            return io.vertx.core.Future.fromCompletionStage(CompletableFuture.allOf(futures));
        }).toCompletionStage().toCompletableFuture()
            .thenApply(v -> {
                slowLog(s, sqls);
                return rs;
            });
    }

    @Local
    @Override
    public <V> CompletableFuture<V> nativeQueryAsync(String sql, BiConsumer<Object, Object> consumer, Function<DataResultSet, V> handler) {
        final long s = System.currentTimeMillis();
        final WorkThread workThread = WorkThread.currentWorkThread();
        return readResultSet(workThread, null, sql).thenApply((VertxResultSet set) -> {
            slowLog(s, sql);
            return handler.apply(set);
        });
    }

    @Local
    @Override
    public CompletableFuture<Integer> nativeUpdateAsync(String sql, Map<String, Object> params) {
        long s = System.currentTimeMillis();
        final WorkThread workThread = WorkThread.currentWorkThread();
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        DataNativeSqlStatement sinfo = super.nativeParse(sql, params);
        if (!sinfo.isEmptyNamed()) {
            writePool().preparedQuery(sinfo.getNativeSql()).execute(tupleParameter(sinfo, params), (AsyncResult<RowSet<Row>> event) -> {
                slowLog(s, sinfo.getNativeSql());
                if (event.failed()) {
                    completeExceptionally(workThread, future, event.cause());
                    return;
                }
                complete(workThread, future, event.result().rowCount());
            });
        } else {
            writePool().query(sinfo.getNativeSql()).execute((AsyncResult<RowSet<Row>> event) -> {
                slowLog(s, sinfo.getNativeSql());
                if (event.failed()) {
                    completeExceptionally(workThread, future, event.cause());
                    return;
                }
                complete(workThread, future, event.result().rowCount());
            });
        }
        return future;
    }

    @Local
    @Override
    public <V> CompletableFuture<V> nativeQueryAsync(String sql, BiConsumer<Object, Object> consumer, Function<DataResultSet, V> handler, Map<String, Object> params) {
        long s = System.currentTimeMillis();
        final WorkThread workThread = WorkThread.currentWorkThread();
        final CompletableFuture<V> future = new CompletableFuture<>();
        DataNativeSqlStatement sinfo = super.nativeParse(sql, params);
        if (!sinfo.isEmptyNamed()) {
            readPool().preparedQuery(sinfo.getNativeSql()).execute(tupleParameter(sinfo, params), (AsyncResult<RowSet<Row>> event) -> {
                slowLog(s, sinfo.getNativeSql());
                if (event.failed()) {
                    completeExceptionally(workThread, future, event.cause());
                } else {
                    complete(workThread, future, handler.apply(new VertxResultSet(null, null, event.result()))
                    );
                }
            }
            );
        } else {
            readPool().preparedQuery(sinfo.getNativeSql()).execute((AsyncResult<RowSet<Row>> event) -> {
                slowLog(s, sinfo.getNativeSql());
                if (event.failed()) {
                    completeExceptionally(workThread, future, event.cause());
                } else {
                    complete(workThread, future, handler.apply(new VertxResultSet(null, null, event.result())));
                }
            }
            );
        }
        return future;
    }

    public <V> CompletableFuture<Sheet<V>> nativeQuerySheetAsync(Class<V> type, String sql, Flipper flipper, Map<String, Object> params) {
        long s = System.currentTimeMillis();
        final WorkThread workThread = WorkThread.currentWorkThread();
        final CompletableFuture<Sheet<V>> future = new CompletableFuture<>();
        DataNativeSqlStatement sinfo = super.nativeParse(sql, params);
        Pool pool = readPool();
        Handler<AsyncResult<RowSet<Row>>> countHandler = (AsyncResult<RowSet<Row>> evt) -> {
            slowLog(s, sinfo.getNativeCountSql());
            if (evt.failed()) {
                completeExceptionally(workThread, future, evt.cause());
            } else {
                long total = 0;
                RowIterator<Row> it = evt.result().iterator();
                if (it.hasNext()) {
                    total = it.next().getLong(0);
                }
                if (total < 1) {
                    complete(workThread, future, new Sheet<>(total, new ArrayList<>()));
                }
                final long count = total;
                String listSql = sinfo.getNativeCountSql()
                    + (flipper == null || flipper.getLimit() < 1 ? "" : (" LIMIT " + flipper.getLimit() + " OFFSET " + flipper.getOffset()));
                Handler<AsyncResult<RowSet<Row>>> listHandler = (AsyncResult<RowSet<Row>> event) -> {
                    slowLog(s, sinfo.getNativeCountSql());
                    if (event.failed()) {
                        completeExceptionally(workThread, future, event.cause());
                    } else {
                        List<V> list = EntityBuilder.getListValue(type, new VertxResultSet(null, null, event.result()));
                        complete(workThread, future, new Sheet<>(count, list));
                    }
                };
                if (!sinfo.isEmptyNamed()) {
                    pool.preparedQuery(listSql).execute(tupleParameter(sinfo, params), listHandler);
                } else {
                    pool.preparedQuery(listSql).execute(listHandler);
                }
            }
        };
        if (!sinfo.isEmptyNamed()) {
            pool.preparedQuery(sinfo.getNativeCountSql()).execute(tupleParameter(sinfo, params), countHandler);
        } else {
            pool.preparedQuery(sinfo.getNativeCountSql()).execute(countHandler);
        }
        return future;
    }

    protected Tuple tupleParameter(DataNativeSqlStatement sinfo, Map<String, Object> params) {
        List<Object> objs = new ArrayList<>();
        for (String name : sinfo.getParamNames()) {
            objs.add(params.get(name));
        }
        return Tuple.from(objs);
    }
}
