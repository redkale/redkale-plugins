/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mongo;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import com.mongodb.client.result.*;
import com.mongodb.reactivestreams.client.*;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import java.util.stream.Stream;
import org.bson.*;
import org.bson.codecs.configuration.*;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.reactivestreams.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceListener;
import org.redkale.annotation.ResourceType;
import org.redkale.service.Local;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 * Mongodb版的DataSource实现 <br>
 * 注意: datasource.url 需要指定为 mongodb:， 例如：mongodb://127.0.0.1:5005
 *
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(DataSource.class)
public class MongodbDriverDataSource extends AbstractDataSource implements java.util.function.Function<Class, EntityInfo> {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected String readdb;

    protected String writedb;

    protected boolean cacheForbidden;

    protected Properties readConfProps;

    protected Properties writeConfProps;

    protected MongoClient readMongoClient;

    protected MongoDatabase readMongoDatabase;

    protected MongoClient writeMongoClient;

    protected MongoDatabase writeMongoDatabase;

    public MongodbDriverDataSource() {
    }

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        this.name = conf.getValue("name", "");
        if (conf.getAnyValue("read") == null) { //没有读写分离
            Properties rwConf = new Properties();
            conf.forEach((k, v) -> rwConf.put(k, decryptProperty(k, v)));
            initProperties(rwConf);
            this.readConfProps = rwConf;
            this.writeConfProps = rwConf;
        } else { //读写分离
            Properties readConf = new Properties();
            Properties writeConf = new Properties();
            conf.getAnyValue("read").forEach((k, v) -> readConf.put(k, decryptProperty(k, v)));
            conf.getAnyValue("write").forEach((k, v) -> writeConf.put(k, decryptProperty(k, v)));
            initProperties(readConf);
            initProperties(writeConf);
            this.readConfProps = readConf;
            this.writeConfProps = writeConf;
        }

        this.cacheForbidden = "NONE".equalsIgnoreCase(readConfProps.getProperty(DATA_SOURCE_CACHEMODE));
        this.readMongoClient = createMongoClient(true, this.readConfProps);
        if (this.readConfProps == this.writeConfProps) {
            this.writeMongoClient = this.readMongoClient;
            this.writedb = this.readdb;
        } else {
            this.writeMongoClient = createMongoClient(false, this.writeConfProps);
        }
    }

    @Override
    @ResourceListener
    public void onResourceChange(ResourceEvent[] events) {
        if (events == null || events.length < 1) {
            return;
        }
        //不支持读写分离模式的动态切换
        if (readConfProps == writeConfProps
            && (events[0].name().startsWith("read.") || events[0].name().startsWith("write."))) {
            throw new SourceException("DataSource(name=" + resourceName() + ") not support to change to read/write separation mode");
        }
        if (readConfProps != writeConfProps
            && (!events[0].name().startsWith("read.") && !events[0].name().startsWith("write."))) {
            throw new SourceException("DataSource(name=" + resourceName() + ") not support to change to non read/write separation mode");
        }

        StringBuilder sb = new StringBuilder();
        if (readConfProps == writeConfProps) {
            List<ResourceEvent> allEvents = new ArrayList<>();
            Properties newProps = new Properties();
            newProps.putAll(this.readConfProps);
            for (ResourceEvent event : events) { //可能需要解密
                String newValue = decryptProperty(event.name(), event.newValue().toString());
                allEvents.add(ResourceEvent.create(event.name(), newValue, event.oldValue()));
                newProps.put(event.name(), newValue);
                sb.append("DataSource(name=").append(resourceName()).append(") change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
            }
            { //更新MongoClient
                MongoClient oldClient = this.readMongoClient;
                this.readMongoClient = createMongoClient(true, newProps);
                this.writeMongoClient = this.readMongoClient;
                this.writedb = this.readdb;
                if (oldClient != null) {
                    oldClient.close();
                }
            }
            for (ResourceEvent event : allEvents) {
                this.readConfProps.put(event.name(), event.newValue());
            }
        } else {
            List<ResourceEvent> readEvents = new ArrayList<>();
            List<ResourceEvent> writeEvents = new ArrayList<>();
            Properties newReadProps = new Properties();
            newReadProps.putAll(this.readConfProps);
            Properties newWriteProps = new Properties();
            newWriteProps.putAll(this.writeConfProps);
            for (ResourceEvent event : events) {
                if (event.name().startsWith("read.")) {
                    String newName = event.name().substring("read.".length());
                    String newValue = decryptProperty(event.name(), event.newValue().toString());
                    readEvents.add(ResourceEvent.create(newName, newValue, event.oldValue()));
                    newReadProps.put(event.name(), newValue);
                } else {
                    String newName = event.name().substring("write.".length());
                    String newValue = decryptProperty(event.name(), event.newValue().toString());
                    writeEvents.add(ResourceEvent.create(newName, newValue, event.oldValue()));
                    newWriteProps.put(event.name(), newValue);
                }
                sb.append("DataSource(name=").append(resourceName()).append(") change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
            }
            if (!readEvents.isEmpty()) { //更新Read MongoClient
                MongoClient oldClient = this.readMongoClient;
                this.readMongoClient = createMongoClient(true, newReadProps);
                if (oldClient != null) {
                    oldClient.close();
                }
            }
            if (!writeEvents.isEmpty()) {//更新Write MongoClient
                MongoClient oldClient = this.writeMongoClient;
                this.writeMongoClient = createMongoClient(false, newReadProps);
                if (oldClient != null) {
                    oldClient.close();
                }
            }
            //更新Properties
            if (!readEvents.isEmpty()) {
                for (ResourceEvent event : readEvents) {
                    this.readConfProps.put(event.name(), event.newValue());
                }
            }
            if (!writeEvents.isEmpty()) {
                for (ResourceEvent event : writeEvents) {
                    this.writeConfProps.put(event.name(), event.newValue());
                }
            }
        }
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
    }

    protected MongoClient createMongoClient(boolean read, Properties prop) {
        int maxconns = Math.max(1, Integer.decode(prop.getProperty(DATA_SOURCE_MAXCONNS, "" + Utility.cpus())));
        // 实体类自动映射
        MongoClientSettings.Builder settingBuilder = MongoClientSettings.builder();
        String url = prop.getProperty(DATA_SOURCE_URL);
        if (url.indexOf('?') < 0) {
            url += "?maxpoolsize=" + maxconns;
        } else if (!url.contains("maxpoolsize=")) {
            url += "&maxpoolsize=" + maxconns;
        }
        ConnectionString cc = new ConnectionString(url);
        if (read && readdb == null) {
            this.readdb = cc.getDatabase();
        }
        if (!read && writedb == null) {
            this.writedb = cc.getDatabase();
        }
        settingBuilder.applyConnectionString(cc);
        CodecRegistry registry = CodecRegistries.fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
            CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        settingBuilder.codecRegistry(registry);
        return MongoClients.create(settingBuilder.build());
    }

    //解密可能存在的加密字段, 可重载
    protected String decryptProperty(String key, String value) {
        return value;
    }

    //可重载
    protected void initProperties(Properties props) {
    }

    @Override
    public void destroy(AnyValue config) {
        super.destroy(config);
        if (this.readMongoClient != null) {
            this.readMongoClient.close();
        }
        if (this.writeMongoClient != null && this.readMongoClient != this.writeMongoClient) {
            this.writeMongoClient.close();
        }
    }

    @Override
    public String toString() {
        if (readConfProps == null) {
            return getClass().getSimpleName() + "{}"; //compileMode模式下会为null
        }
        return getClass().getSimpleName() + "{url=" + readConfProps.getProperty(DATA_SOURCE_URL) + "}";
    }

    @Local
    @Override
    public EntityInfo apply(Class t) {
        return loadEntityInfo(t);
    }

    protected <T> EntityInfo<T> loadEntityInfo(Class<T> clazz) {
        return loadEntityInfo(clazz, this.cacheForbidden, readConfProps, null);
    }

    @Override
    @Local
    public <T> void compile(Class<T> clazz) {
        EntityInfo.compile(clazz, this);
    }

    @Local
    public MongoClient getReadMongoClient() {
        return this.readMongoClient;
    }

    @Local
    public MongoDatabase getReadMongoDatabase() {
        if (this.readMongoDatabase != null) {
            return this.readMongoDatabase;
        }
        this.readMongoDatabase = this.readMongoClient.getDatabase(readdb);
        return this.readMongoDatabase;
    }

    @Local
    public <T> MongoCollection<T> getReadMongoCollection(EntityInfo<T> info) {
        return this.getReadMongoDatabase().getCollection(info.getTable((T) null), info.getType());
    }

    @Local
    public <T> MongoCollection<Document> getReadMongoDocumentCollection(EntityInfo<T> info) {
        return this.getReadMongoDatabase().getCollection(info.getTable((T) null));
    }

    @Local
    public MongoClient getWriteMongoClient() {
        return this.writeMongoClient;
    }

    @Local
    public MongoDatabase getWriteMongoDatabase() {
        if (this.writeMongoDatabase != null) {
            return this.writeMongoDatabase;
        }
        this.writeMongoDatabase = this.readMongoClient.getDatabase(writedb);
        return this.writeMongoDatabase;
    }

    @Local
    public <T> MongoCollection<T> getWriteMongoCollection(EntityInfo<T> info) {
        return this.getWriteMongoDatabase().getCollection(info.getTable((T) null), info.getType());
    }

    @Local
    public <T> MongoCollection<Document> getWriteMongoDocumentCollection(EntityInfo<T> info) {
        return this.getWriteMongoDatabase().getCollection(info.getTable((T) null));
    }

    //可重载此方法以支持特殊数据类型， 例如：Date、Time
    protected <T> Object formatFilterValue(EntityInfo<T> info, Serializable val) {
        return val;
    }

    @Local
    public Bson createSortBson(Flipper flipper) {
        if (flipper == null) {
            return null;
        }
        if (flipper.getSort() == null || flipper.getSort().isEmpty()) {
            return null;
        }
        List<Bson> sorts = new ArrayList<>();
        for (String item : flipper.getSort().split(",")) {
            if (item.isEmpty()) {
                continue;
            }
            String[] sub = item.split("\\s+");
            if (sub.length < 2 || sub[1].equalsIgnoreCase("ASC")) {
                sorts.add(Sorts.ascending(sub[0]));
            } else {
                sorts.add(Sorts.descending(sub[0]));
            }
        }
        if (sorts.isEmpty()) {
            return null;
        }
        if (sorts.size() == 1) {
            return sorts.get(0);
        }
        return Sorts.orderBy(sorts);
    }

    @Local
    public <T> List<Bson> createUpdateBson(EntityInfo<T> info, ColumnValue... values) {
        List<Bson> items = new ArrayList<>(values.length);
        for (ColumnValue colval : values) {
            Bson bson = createUpdateBson(info, colval);
            if (bson != null) {
                items.add(bson);
            }
        }
        return items;
    }

    @Local
    public <T> Bson createUpdateBson(EntityInfo<T> info, ColumnValue colval) {
        String key = colval.getColumn();
        ColumnNode val = colval.getValue2();
        switch (colval.getExpress()) {
            case MOV:// col = val
                return new BsonDocument("$set", new BsonDocument(key, formatToBsonValue(val)));
            case INC:// col = col + val
                return new BsonDocument("$inc", new BsonDocument(key, formatToBsonValue(val)));
            case DEC:// col = col - val
                return new BsonDocument("$inc", new BsonDocument(key, formatToBsonValue(val, true)));
            case MUL:// col = col * val
                return new BsonDocument("$mul", new BsonDocument(key, formatToBsonValue(val)));
            case DIV:// col = col / val
                return new BsonDocument("$set", new BsonDocument(key, new BsonDocument("$divide", new BsonArray(List.of(new BsonString("$" + key), formatToBsonValue(val))))));
            case MOD:// col = col % val
                return new BsonDocument("$set", new BsonDocument(key, new BsonDocument("$mod", new BsonArray(List.of(new BsonString("$" + key), formatToBsonValue(val))))));
            case AND: // col = col & val
                return new BsonDocument("$bit", new BsonDocument(key, new BsonDocument("and", formatToBsonValue(val))));
            case ORR: //col = col | val
                return new BsonDocument("$bit", new BsonDocument(key, new BsonDocument("or", formatToBsonValue(val))));
        }
        return null;
    }

    @Local
    public BsonField createBsonField(FilterFunc func, String fieldName, Serializable column) {
        BsonField bf = null;
        if (column == null || FilterFuncColumn.COLUMN_NULL.equals(column)) {
            column = "_id";
        }
        if (fieldName == null) {
            fieldName = column.toString();
        }
        if (func == FilterFunc.COUNT) {
            bf = Accumulators.sum(fieldName, 1);
        } else if (func == FilterFunc.AVG) {
            bf = Accumulators.avg(fieldName, "$" + column);
        } else if (func == FilterFunc.MAX) {
            bf = Accumulators.max(fieldName, "$" + column);
        } else if (func == FilterFunc.MIN) {
            bf = Accumulators.min(fieldName, "$" + column);
        } else if (func == FilterFunc.SUM) {
            bf = Accumulators.sum(fieldName, "$" + column);
        } else {
            throw new UnsupportedOperationException(FilterFunc.class.getSimpleName() + " " + func + " not supported yet.");
        }
        return bf;
    }

    @Local
    public <T> Bson createFilterBson(EntityInfo<T> info, FilterNode node) {
        return createFilter(info, node, false);
    }

    protected BsonValue formatToBsonValue(ColumnNode node) {
        return formatToBsonValue(node, false);
    }

    protected BsonValue formatToBsonValue(ColumnNode node, boolean dec) {
        if (node == null) {
            return null;
        } else if (node instanceof ColumnNumberNode) {
            Number val = ((ColumnNumberNode) node).getValue();
            BsonNumber bn = null;
            if (val instanceof Number) {
                if (val instanceof Float || val instanceof Double) {
                    double d = ((Number) val).doubleValue();
                    if (dec) {
                        d = -d;
                    }
                    bn = new BsonDouble(d);
                } else if (val instanceof Long) {
                    long d = ((Number) val).longValue();
                    if (dec) {
                        d = -d;
                    }
                    bn = new BsonInt64(d);
                } else {
                    int d = ((Number) val).intValue();
                    if (dec) {
                        d = -d;
                    }
                    bn = new BsonInt32(d);
                }
                return bn;
            }
        } else if (node instanceof ColumnNameNode) {
            return new BsonString("$" + ((ColumnNameNode) node).getColumn());
        }
        throw new IllegalArgumentException("Not supported ColumnValue " + node);
    }

    private <T> Bson createFilter(EntityInfo<T> info, FilterNode node, boolean sub) {
        if (node == null) {
            return null;
        }
        Bson bson = createFilterElement(info, node);
        if (bson == null && node.getNodes() == null) {
            return null;
        }
        List<Bson> items = new ArrayList<>();
        if (bson != null) {
            items.add(bson);
        }
        if (node.getNodes() != null) {
            for (FilterNode item : node.getNodes()) {
                Bson s = createFilter(info, item, true);
                if (s == null) {
                    continue;
                }
                items.add(s);
            }
        }
        if (items.isEmpty()) {
            return null;
        }
        if (items.size() == 1) {
            return items.get(0);
        }
        return node.isOr() ? Filters.or(items.toArray(new Bson[items.size()])) : Filters.and(items.toArray(new Bson[items.size()]));
    }

    private <T> Bson createFilterElement(EntityInfo<T> info, FilterNode node) {
        if (node == null || node.getColumn() == null || node.getColumn().charAt(0) == '#') {
            return null;
        }
        if (node instanceof FilterJoinNode) {
            throw new IllegalArgumentException("Not supported " + FilterJoinNode.class.getSimpleName());
        }
        switch (node.getExpress()) {
            case EQ: {
                return Filters.eq(node.getColumn(), formatFilterValue(info, node.getValue()));
            }
            case IG_EQ: {
                return Filters.regex(node.getColumn(), "/^" + node.getValue() + "$/i");
            }
            case NOT_EQ:
            case IG_NOT_EQ: {
                return Filters.not(Filters.eq(node.getColumn(), node.getValue()));
            }
            case GT: {
                return Filters.gt(node.getColumn(), formatFilterValue(info, node.getValue()));
            }
            case LT: {
                return Filters.lt(node.getColumn(), formatFilterValue(info, node.getValue()));
            }
            case GE: {
                return Filters.gte(node.getColumn(), formatFilterValue(info, node.getValue()));
            }
            case LE: {
                return Filters.lte(node.getColumn(), formatFilterValue(info, node.getValue()));
            }
            case LIKE: {
                return Filters.regex(node.getColumn(), "/" + node.getValue() + "/");
            }
            case NOT_LIKE: {
                return Filters.not(Filters.regex(node.getColumn(), "/" + node.getValue() + "/"));
            }
            case IN: {
                return Filters.in(node.getColumn(), node.getValue() instanceof Collection ? (Collection) node.getValue() : List.of((Object[]) node.getValue()));
            }
            case NOT_IN: {
                return Filters.not(Filters.in(node.getColumn(), node.getValue() instanceof Collection ? (Collection) node.getValue() : (Object[]) node.getValue()));
            }
            case BETWEEN: {
                Range range = (Range) node.getValue();
                if (range.getMax() != null && range.getMax().compareTo(range.getMin()) > 0) {
                    return Filters.and(Filters.gte(node.getColumn(), range.getMin()), Filters.lte(node.getColumn(), range.getMax()));
                } else {
                    return Filters.gte(node.getColumn(), range.getMin());
                }
            }
            case NOT_BETWEEN: {
                Range range = (Range) node.getValue();
                Bson bson;
                if (range.getMax() != null && range.getMax().compareTo(range.getMin()) > 0) {
                    bson = Filters.and(Filters.gte(node.getColumn(), range.getMin()), Filters.lte(node.getColumn(), range.getMax()));
                } else {
                    bson = Filters.gte(node.getColumn(), range.getMin());
                }
                return Filters.not(bson);
            }
            default:
                throw new IllegalArgumentException("Not supported FilterNode " + node);
        }
    }

    @Override
    public String getType() {
        return "mongodb";
    }

    @Override
    public <T> int insert(T... entitys) {
        if (entitys.length == 0) {
            return 0;
        }
        checkEntity("insert", false, entitys);
        return insertAsync(entitys).join();
    }

    @Override
    public <T> CompletableFuture<Integer> insertAsync(T... entitys) {
        if (entitys.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        checkEntity("insert", false, entitys);
        EntityInfo<T> info = loadEntityInfo((Class<T>) entitys[0].getClass());
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<InsertManyResult> future = new ReatorFuture<>();
        collection.insertMany(List.of(entitys)).subscribe(future);
        return future.thenApply(v -> v.getInsertedIds().size());
    }

    @Override
    public <T> int delete(T... entitys) {
        if (entitys.length == 0) {
            return 0;
        }
        checkEntity("delete", false, entitys);
        return deleteAsync(entitys).join();
    }

    @Override
    public <T> CompletableFuture<Integer> deleteAsync(T... entitys) {
        if (entitys.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        checkEntity("delete", false, entitys);
        EntityInfo<T> info = loadEntityInfo((Class<T>) entitys[0].getClass());
        MongoCollection<T> collection = getWriteMongoCollection(info);
        Attribute<T, Serializable> primary = info.getPrimary();
        Object[] ids = new Object[entitys.length];
        for (int i = 0; i < entitys.length; i++) {
            ids[i] = primary.get(entitys[i]);
        }
        ReatorFuture<DeleteResult> future = new ReatorFuture<>();
        collection.deleteMany(Filters.in(primary.field(), ids)).subscribe(future);
        return future.thenApply(v -> (int) v.getDeletedCount());
    }

    @Override
    public <T> int delete(Class<T> clazz, Serializable... pks) {
        if (pks.length == 0) {
            return 0;
        }
        return deleteAsync(clazz, pks).join();
    }

    @Override
    public <T> CompletableFuture<Integer> deleteAsync(Class<T> clazz, Serializable... pks) {
        if (pks.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<DeleteResult> future = new ReatorFuture<>();
        collection.deleteMany(Filters.in(info.getPrimaryField(), pks)).subscribe(future); //deleteOne bug?
        return future.thenApply(v -> (int) v.getDeletedCount());
    }

    @Override
    public <T> int delete(Class<T> clazz, Flipper flipper, FilterNode node) {
        return deleteAsync(clazz, flipper, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> deleteAsync(Class<T> clazz, Flipper flipper, FilterNode node) {
        if (flipper != null) {
            return CompletableFuture.failedFuture(new RuntimeException("delete on Flipper not supported yet."));
        }
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getWriteMongoCollection(info);

        Bson filter = createFilterBson(info, node);
        ReatorFuture<DeleteResult> future = new ReatorFuture<>();
        collection.deleteMany(filter).subscribe(future);
        return future.thenApply(v -> (int) v.getDeletedCount());
    }

    @Override
    public <T> int clearTable(Class<T> clazz, FilterNode node) {
        return clearTableAsync(clazz, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> clearTableAsync(Class<T> clazz, FilterNode node) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<DeleteResult> future = new ReatorFuture<>();
        collection.deleteMany(new BsonDocument()).subscribe(future);
        return future.thenApply(v -> (int) v.getDeletedCount());
    }

    @Override
    public <T> int createTable(Class<T> clazz, Serializable pk) {
        return createTableAsync(clazz, pk).join();
    }

    @Override
    public <T> CompletableFuture<Integer> createTableAsync(Class<T> clazz, Serializable pk) {
        return CompletableFuture.completedFuture(0);
    }

    @Override
    public <T> int dropTable(Class<T> clazz, FilterNode node) {
        return dropTableAsync(clazz, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> dropTableAsync(Class<T> clazz, FilterNode node) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<Void> future = new ReatorFuture<>();
        collection.drop().subscribe(future);
        return future.thenApply(v -> 1);
    }

    @Override
    public <T> int update(T... entitys) {
        return updateAsync(entitys).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateAsync(T... entitys) {
        if (entitys.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        checkEntity("update", false, entitys);
        EntityInfo<T> info = loadEntityInfo((Class<T>) entitys[0].getClass());
        MongoCollection<T> collection = getWriteMongoCollection(info);

        Attribute<T, Serializable> primary = info.getPrimary();
        List<UpdateOneModel<T>> list = new ArrayList<>();
        for (T entity : entitys) {
            Serializable pk = primary.get(entity);
            List<Bson> items = new ArrayList<>();
            for (Attribute<T, Serializable> attr : info.getUpdateAttributes()) {
                items.add(Updates.set(attr.field(), attr.get(entity)));
            }
            list.add(new UpdateOneModel(Filters.eq(info.getPrimaryField(), pk), Updates.combine(items)));
        }
        ReatorFuture<BulkWriteResult> future = new ReatorFuture<>();
        collection.bulkWrite(list).subscribe(future);
        return future.thenApply(v -> (int) v.getModifiedCount());
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, Serializable pk, String column, Serializable value) {
        return updateColumnAsync(clazz, pk, column, value).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(Class<T> clazz, Serializable pk, String column, Serializable value) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<UpdateResult> future = new ReatorFuture<>();
        collection.updateOne(Filters.eq(info.getPrimaryField(), pk), Updates.set(column, value)).subscribe(future);
        return future.thenApply(v -> (int) v.getModifiedCount());
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, String column, Serializable value, FilterNode node) {
        return updateColumnAsync(clazz, column, value, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(Class<T> clazz, String column, Serializable value, FilterNode node) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<UpdateResult> future = new ReatorFuture<>();
        UpdateOptions options = null;
        collection.updateMany(createFilterBson(info, node), Updates.set(column, value), options).subscribe(future);
        return future.thenApply(v -> (int) v.getModifiedCount());
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, Serializable pk, ColumnValue... values) {
        return updateColumnAsync(clazz, pk, values).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(Class<T> clazz, Serializable pk, ColumnValue... values) {
        if (values.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        EntityInfo<T> info = loadEntityInfo(clazz);
        List<Bson> items = createUpdateBson(info, values);
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<UpdateResult> future = new ReatorFuture<>();
        collection.updateOne(Filters.eq(info.getPrimaryField(), pk), Updates.combine(items)).subscribe(future);
        return future.thenApply(v -> (int) v.getModifiedCount());
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, FilterNode node, Flipper flipper, ColumnValue... values) {
        return updateColumnAsync(clazz, node, flipper, values).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(Class<T> clazz, FilterNode node, Flipper flipper, ColumnValue... values) {
        if (values.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        if (flipper != null) {
            return CompletableFuture.failedFuture(new RuntimeException("updateColumn on Flipper not supported yet."));
        }
        EntityInfo<T> info = loadEntityInfo(clazz);
        List<Bson> items = createUpdateBson(info, values);
        if (items == null || items.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<UpdateResult> future = new ReatorFuture<>();
        collection.updateMany(createFilterBson(info, node), Updates.combine(items)).subscribe(future);
        return future.thenApply(v -> (int) v.getModifiedCount());
    }

    @Override
    public <T> int updateColumn(T entity, FilterNode node, SelectColumn selects) {
        return updateColumnAsync(entity, node, selects).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(T entity, FilterNode node, SelectColumn selects) {
        EntityInfo<T> info = loadEntityInfo((Class) entity.getClass());
        List<Bson> items = new ArrayList<>();
        for (Attribute<T, Serializable> attr : info.getUpdateAttributes()) {
            if (selects == null || selects.test(attr.field())) {
                items.add(Updates.set(attr.field(), attr.get(entity)));
            }
        }
        if (items.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        MongoCollection<T> collection = getWriteMongoCollection(info);

        ReatorFuture<UpdateResult> future = new ReatorFuture<>();
        collection.updateMany(createFilterBson(info, node), Updates.combine(items)).subscribe(future);
        return future.thenApply(v -> (int) v.getModifiedCount());
    }

    @Override
    public Number getNumberResult(Class entityClass, FilterFunc func, Number defVal, String column, FilterNode node) {
        return getNumberResultAsync(entityClass, func, defVal, column, node).join();
    }

    @Override
    public CompletableFuture<Number> getNumberResultAsync(Class entityClass, FilterFunc func, Number defVal, String column, FilterNode node) {
        EntityInfo info = loadEntityInfo(entityClass);
        MongoCollection<Document> collection = getReadMongoDocumentCollection(info);
        Bson filter = createFilterBson(info, node);
        if (func == FilterFunc.COUNT) {
            Publisher<Long> publisher = filter == null ? collection.countDocuments() : collection.countDocuments(filter);
            ReatorFuture<Long> future = new ReatorFuture<>();
            publisher.subscribe(future);
            return future.thenApply(v -> v.intValue());
        } else if (func == FilterFunc.DISTINCTCOUNT) {
            String key = column == null ? info.getPrimaryField() : column;
            ReatorFuture<Document> future = new ReatorFuture<>();
            List<Bson> items = new ArrayList<>();
            if (filter != null) {
                items.add(Aggregates.match(filter));
            }
            //[{$group:{_id:"$fieldName"}}, {$group:{_id:1, count:{$sum:1}}}]
            items.add(Aggregates.group("$" + key));
            items.add(Aggregates.group(1, Accumulators.sum("count", 1)));
            collection.aggregate(items).subscribe(future);
            return future.thenApply(v -> v == null ? defVal : (Number) v.get("count"));
        } else {
            ReatorFuture<Document> future = new ReatorFuture<>();
            List<Bson> items = new ArrayList<>();
            if (filter != null) {
                items.add(Aggregates.match(filter));
            }
            BsonField bf = createBsonField(func, column, column);
            items.add(Aggregates.group(null, bf));
            //System.println(items.get(0).toBsonDocument().toJson(JsonWriterSettings.builder().indent(true).build()));
            collection.aggregate(items).subscribe(future);
            return future.thenApply(v -> v == null ? defVal : (Number) v.get(column));
        }
    }

    @Override
    public <N extends Number> Map<String, N> getNumberMap(Class entityClass, FilterNode node, FilterFuncColumn... columns) {
        return (Map) getNumberMapAsync(entityClass, node, columns).join();
    }

    @Override  //等价SQL: SELECT FUNC1{column1}, FUNC2{column2}, ... FROM {table}
    public <N extends Number> CompletableFuture<Map<String, N>> getNumberMapAsync(Class entityClass, FilterNode node, FilterFuncColumn... columns) {
        final EntityInfo info = loadEntityInfo(entityClass);
        final EntityCache cache = info.getCache();
        if (cache != null && (isOnlyCache(info) || cache.isFullLoaded())) {
            final Map map = new HashMap<>();
            if (node == null || isCacheUseable(node, this)) {
                for (FilterFuncColumn ffc : columns) {
                    for (String col : ffc.cols()) {
                        map.put(ffc.col(col), cache.getNumberResult(ffc.getFunc(), ffc.getDefvalue(), col, node));
                    }
                }
                return CompletableFuture.completedFuture(map);
            }
        }
        MongoCollection<Document> collection = getReadMongoDocumentCollection(info);
        Bson filter = createFilterBson(info, node);
        ReatorFuture<Document> future = new ReatorFuture<>();
        List<Bson> items = new ArrayList<>();
        if (filter != null) {
            items.add(Aggregates.match(filter));
        }
        final List<BsonField> fields = new ArrayList<>();
        final List<String> funcols = new ArrayList<>();
        int i = 0;
        for (FilterFuncColumn ffc : columns) {
            for (String col : ffc.cols()) {
                String colname = "_func_col_" + (++i);
                funcols.add(colname);
                fields.add(createBsonField(ffc.getFunc(), colname, ffc.col(col)));
            }
        }
        items.add(Aggregates.group(null, fields));
        items.add(Aggregates.project(Projections.include(funcols)));
        collection.aggregate(items).subscribe(future);
        return future.thenApply(v -> {
            Map rs = new LinkedHashMap();
            int index = 0;
            for (FilterFuncColumn ffc : columns) {
                for (String col : ffc.cols()) {
                    Object o = v.get(funcols.get(index));
                    Number n = ffc.getDefvalue();
                    if (o != null) {
                        n = (Number) o;
                    }
                    rs.put(ffc.col(col), n);
                    ++index;
                }
            }
            return rs;
        });
    }

    @Override
    public <T, K extends Serializable, N extends Number> Map<K, N> queryColumnMap(Class<T> entityClass, String keyColumn, FilterFunc func, String funcColumn, FilterNode node) {
        return (Map) queryColumnMapAsync(entityClass, keyColumn, func, funcColumn, node).join();
    }

    @Override
    public <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N>> queryColumnMapAsync(Class<T> entityClass, String keyColumn, FilterFunc func, String funcColumn, FilterNode node) {
        return (CompletableFuture) queryColumnMapCompose(entityClass, false, false, Utility.ofArray(ColumnFuncNode.create(func, funcColumn)), Utility.ofArray(keyColumn), node);
    }

    @Override
    public <T, K extends Serializable, N extends Number> Map<K, N[]> queryColumnMap(Class<T> entityClass, ColumnNode[] funcNodes, String groupByColumn, FilterNode node) {
        return (Map) queryColumnMapAsync(entityClass, funcNodes, groupByColumn, node).join();
    }

    @Override
    public <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N[]>> queryColumnMapAsync(Class<T> entityClass, ColumnNode[] funcNodes, String groupByColumn, FilterNode node) {
        return (CompletableFuture) queryColumnMapCompose(entityClass, false, true, funcNodes, Utility.ofArray(groupByColumn), node);
    }

    @Override
    public <T, K extends Serializable, N extends Number> Map<K[], N[]> queryColumnMap(Class<T> entityClass, ColumnNode[] funcNodes, String[] groupByColumns, FilterNode node) {
        return (Map) queryColumnMapAsync(entityClass, funcNodes, groupByColumns, node).join();
    }

    @Override  //等价SQL: SELECT col1, col2, FUNC{funcColumn1}, FUNC{funcColumn2} FROM {table} WHERE {filter node} GROUP BY {col1}, {col2} 
    public <T, K extends Serializable, N extends Number> CompletableFuture<Map<K[], N[]>> queryColumnMapAsync(Class<T> entityClass, ColumnNode[] funcNodes, String[] groupByColumns, FilterNode node) {
        return (CompletableFuture) queryColumnMapCompose(entityClass, true, true, funcNodes, groupByColumns, node);
    }

    protected <T, K extends Serializable, N extends Number> CompletableFuture<Map> queryColumnMapCompose(Class<T> entityClass, boolean arrayKey, boolean arrayVal, ColumnNode[] funcNodes, String[] groupByColumns, FilterNode node) {
        final EntityInfo info = loadEntityInfo(entityClass);
        final EntityCache cache = info.getCache();
        if (cache != null && (isOnlyCache(info) || cache.isFullLoaded())) {
            if (node == null || isCacheUseable(node, this)) {
                return CompletableFuture.completedFuture(cache.queryColumnMap(funcNodes, groupByColumns, node));
            }
        }
        MongoCollection<Document> collection = getReadMongoDocumentCollection(info);
        Bson filter = createFilterBson(info, node);
        ReatorListFuture<Document> future = new ReatorListFuture<>();
        List<Bson> items = new ArrayList<>();
        if (filter != null) {
            items.add(Aggregates.match(filter));
        }
        BsonDocument group = new BsonDocument();
        for (String col : groupByColumns) {
            group.put(col, new BsonString("$" + col));
        }
        BsonField[] fields = new BsonField[funcNodes.length];
        final String[] funcols = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            ColumnNode colNode = funcNodes[i];
            String colName = "_func_col_" + (i + 1);
            funcols[i] = colName;
            if (colNode instanceof ColumnFuncNode) {
                ColumnFuncNode cfn = (ColumnFuncNode) colNode;
                if (cfn.getValue2() instanceof ColumnExpNode) {
                    throw new UnsupportedOperationException(ColumnExpNode.class.getSimpleName() + " " + colNode + " not supported yet.");
                }
                fields[i] = createBsonField(cfn.getFunc(), colName, cfn.getValue2());
            } else {
                throw new UnsupportedOperationException(ColumnNode.class.getSimpleName() + " " + colNode + " not supported yet.");
            }
        }
        items.add(Aggregates.group(group, fields));
        items.add(Aggregates.project(Projections.include(Utility.append(groupByColumns, funcols))));
        collection.aggregate(items).subscribe(future);
        return future.thenApply(v -> {
            Map rs = new LinkedHashMap();
            for (Document doc : v) {
                Document id = (Document) doc.get("_id");
                Object keys, vals;
                //key
                if (arrayKey) {
                    Object[] array = new Object[groupByColumns.length];
                    for (int i = 0; i < array.length; i++) {
                        array[i] = id.get(groupByColumns[i]);
                    }
                    keys = array;
                } else {
                    keys = id.get(groupByColumns[0]);
                }
                //value
                if (arrayVal) {
                    Object[] array = new Object[funcols.length];
                    for (int i = 0; i < array.length; i++) {
                        array[i] = doc.get(funcols[i]);
                    }
                    vals = array;
                } else {
                    vals = doc.get(funcols[0]);
                }
                rs.put(keys, vals);
            }
            return rs;
        });
    }

    @Override
    public <T> CompletableFuture<T> findAsync(Class<T> clazz, Serializable pk) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getReadMongoCollection(info);
        ReatorFuture<T> future = new ReatorFuture<>();
        collection.find(Filters.eq(info.getPrimaryField(), pk)).first().subscribe(future);
        return future;
    }

    @Override
    public <T> T find(Class<T> clazz, SelectColumn selects, Serializable pk) {
        return findAsync(clazz, selects, pk).join();
    }

    @Override
    public <T> CompletableFuture<T> findAsync(Class<T> clazz, SelectColumn selects, Serializable pk) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getReadMongoCollection(info);
        ReatorFuture<T> future = new ReatorFuture<>();
        FindPublisher<T> publisher = collection.find(Filters.eq(info.getPrimaryField(), pk));
        if (selects != null) {
            publisher.projection(selects.isExcludable() ? Projections.exclude(selects.getColumns()) : Projections.include(selects.getColumns()));
        }
        publisher.first().subscribe(future);
        return future;
    }

    @Override
    public <T> T find(Class<T> clazz, SelectColumn selects, FilterNode node) {
        return findAsync(clazz, selects, node).join();
    }

    @Override
    public <T> CompletableFuture<T> findAsync(Class<T> clazz, SelectColumn selects, FilterNode node) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getReadMongoCollection(info);
        ReatorFuture<T> future = new ReatorFuture<>();
        FindPublisher<T> publisher = collection.find();
        Bson filter = createFilterBson(info, node);
        if (filter != null) {
            publisher.filter(filter);
        }
        if (selects != null) {
            publisher.projection(selects.isExcludable() ? Projections.exclude(selects.getColumns()) : Projections.include(selects.getColumns()));
        }
        publisher.first().subscribe(future);
        return future;
    }

    @Override
    public <T> T[] finds(Class<T> clazz, final SelectColumn selects, Serializable... pks) {
        return findsAsync(clazz, selects, pks).join();
    }

    @Override
    public <T> CompletableFuture<T[]> findsAsync(Class<T> clazz, final SelectColumn selects, Serializable... pks) {
        final EntityInfo<T> info = loadEntityInfo(clazz);
        if (pks == null || pks.length == 0) {
            return CompletableFuture.completedFuture(info.getArrayer().apply(0));
        }
        final Attribute<T, Serializable> primary = info.getPrimary();
        return queryListAsync(info.getType(), selects, null, FilterNodes.in(info.getPrimarySQLColumn(), pks)).thenApply(list -> {
            T[] rs = info.getArrayer().apply(pks.length);
            for (int i = 0; i < rs.length; i++) {
                T t = null;
                Serializable pk = pks[i];
                for (T item : list) {
                    if (pk.equals(primary.get(item))) {
                        t = item;
                        break;
                    }
                }
                rs[i] = t;
            }
            return rs;
        });
    }

    @Override
    public <D extends Serializable, T> List<T> findsList(Class<T> clazz, Stream<D> pks) {
        return findsListAsync(clazz, pks).join();
    }

    @Override
    public <D extends Serializable, T> CompletableFuture<List<T>> findsListAsync(final Class<T> clazz, final Stream<D> pks) {
        final EntityInfo<T> info = loadEntityInfo(clazz);
        Serializable[] ids = pks.toArray(serialArrayFunc);
        return queryListAsync(info.getType(), null, null, FilterNodes.in(info.getPrimarySQLColumn(), ids));
    }

    @Override
    public <T> Serializable findColumn(Class<T> clazz, String column, Serializable defValue, Serializable pk) {
        return findColumnAsync(clazz, column, defValue, pk).join();
    }

    @Override
    public <T> CompletableFuture<Serializable> findColumnAsync(Class<T> clazz, String column, Serializable defValue, Serializable pk) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getReadMongoCollection(info);
        ReatorFuture<T> future = new ReatorFuture<>();
        FindPublisher<T> publisher = collection.find(Filters.eq(info.getPrimaryField(), pk));
        publisher.projection(Projections.include(column));
        publisher.first().subscribe(future);
        return future.thenApply(v -> v == null ? defValue : info.getAttribute(column).get(v));
    }

    @Override
    public <T> Serializable findColumn(Class<T> clazz, String column, Serializable defValue, FilterNode node) {
        return findColumnAsync(clazz, column, defValue, node).join();
    }

    @Override
    public <T> CompletableFuture<Serializable> findColumnAsync(Class<T> clazz, String column, Serializable defValue, FilterNode node) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getReadMongoCollection(info);
        ReatorFuture<T> future = new ReatorFuture<>();
        FindPublisher<T> publisher = collection.find(createFilterBson(info, node));
        publisher.projection(Projections.include(column));
        publisher.first().subscribe(future);
        return future.thenApply(v -> v == null ? defValue : info.getAttribute(column).get(v));
    }

    @Override
    public <T> boolean exists(Class<T> clazz, Serializable pk) {
        return existsAsync(clazz, pk).join();
    }

    @Override
    public <T> CompletableFuture<Boolean> existsAsync(Class<T> clazz, Serializable pk) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getReadMongoCollection(info);
        ReatorFuture<T> future = new ReatorFuture<>();
        collection.find(Filters.eq(info.getPrimaryField(), pk)).first().subscribe(future);
        return future.thenApply(v -> v != null);
    }

    @Override
    public <T> boolean exists(Class<T> clazz, FilterNode node) {
        return existsAsync(clazz, node).join();
    }

    @Override
    public <T> CompletableFuture<Boolean> existsAsync(Class<T> clazz, FilterNode node) {
        EntityInfo<T> info = loadEntityInfo(clazz);
        MongoCollection<T> collection = getReadMongoCollection(info);
        ReatorFuture<T> future = new ReatorFuture<>();
        collection.find(createFilterElement(info, node)).projection(Projections.include(info.getPrimaryField())).first().subscribe(future);
        return future.thenApply(v -> v != null);
    }

    @Override
    public <T, V extends Serializable> Set<V> queryColumnSet(String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return (Set) queryColumnSetAsync(selectedColumn, clazz, flipper, node).join();
    }

    @Override
    public <T, V extends Serializable> CompletableFuture<Set<V>> queryColumnSetAsync(String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return querySetAsync(clazz, SelectColumn.includes(selectedColumn), flipper, node).thenApply((Set<T> list) -> {
            final Set<V> rs = new LinkedHashSet<>();
            if (list.isEmpty()) {
                return rs;
            }
            final EntityInfo<T> info = loadEntityInfo(clazz);
            final Attribute<T, V> selected = (Attribute<T, V>) info.getAttribute(selectedColumn);
            for (T t : list) {
                rs.add(selected.get(t));
            }
            return rs;
        });
    }

    @Override
    public <T, V extends Serializable> List<V> queryColumnList(String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return (List) queryColumnListAsync(selectedColumn, clazz, flipper, node).join();
    }

    @Override
    public <T, V extends Serializable> CompletableFuture<List<V>> queryColumnListAsync(String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return queryListAsync(clazz, SelectColumn.includes(selectedColumn), flipper, node).thenApply((List<T> list) -> {
            final List<V> rs = new ArrayList<>();
            if (list.isEmpty()) {
                return rs;
            }
            final EntityInfo<T> info = loadEntityInfo(clazz);
            final Attribute<T, V> selected = (Attribute<T, V>) info.getAttribute(selectedColumn);
            for (T t : list) {
                rs.add(selected.get(t));
            }
            return rs;
        });
    }

    @Override
    public <T, V extends Serializable> Sheet<V> queryColumnSheet(String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return (Sheet) queryColumnSheetAsync(selectedColumn, clazz, flipper, node).join();
    }

    @Override
    public <T, V extends Serializable> CompletableFuture<Sheet<V>> queryColumnSheetAsync(String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return querySheetAsync(clazz, SelectColumn.includes(selectedColumn), flipper, node).thenApply((Sheet<T> sheet) -> {
            final Sheet<V> rs = new Sheet<>();
            if (sheet.isEmpty()) {
                return rs;
            }
            rs.setTotal(sheet.getTotal());
            final EntityInfo<T> info = loadEntityInfo(clazz);
            final Attribute<T, V> selected = (Attribute<T, V>) info.getAttribute(selectedColumn);
            final List<V> list = new ArrayList<>();
            for (T t : sheet.getRows()) {
                list.add(selected.get(t));
            }
            rs.setRows(list);
            return rs;
        });
    }

    @Override
    public <K extends Serializable, T> Map<K, T> queryMap(Class<T> clazz, SelectColumn selects, Stream<K> keyStream) {
        return queryMapAsync(clazz, selects, keyStream).join();
    }

    @Override
    public <K extends Serializable, T> CompletableFuture<Map<K, T>> queryMapAsync(Class<T> clazz, SelectColumn selects, Stream<K> keyStream) {
        if (keyStream == null) {
            return CompletableFuture.completedFuture(new LinkedHashMap<>());
        }
        final EntityInfo<T> info = loadEntityInfo(clazz);
        final ArrayList<K> pks = new ArrayList<>();
        keyStream.forEach(k -> pks.add(k));
        final Attribute<T, Serializable> primary = info.getPrimary();
        return queryListAsync(clazz, FilterNodes.in(primary.field(), pks)).thenApply((List<T> rs) -> {
            Map<K, T> map = new LinkedHashMap<>();
            if (rs.isEmpty()) {
                return new LinkedHashMap<>();
            }
            for (T item : rs) {
                map.put((K) primary.get(item), item);
            }
            return map;
        });
    }

    @Override
    public <K extends Serializable, T> Map<K, T> queryMap(Class<T> clazz, SelectColumn selects, FilterNode node) {
        return (Map) queryMapAsync(clazz, selects, node).join();
    }

    @Override
    public <K extends Serializable, T> CompletableFuture<Map<K, T>> queryMapAsync(Class<T> clazz, SelectColumn selects, FilterNode node) {
        return queryListAsync(clazz, selects, node).thenApply((List<T> rs) -> {
            final EntityInfo<T> info = loadEntityInfo(clazz);
            final Attribute<T, Serializable> primary = info.getPrimary();
            Map<K, T> map = new LinkedHashMap<>();
            if (rs.isEmpty()) {
                return new LinkedHashMap<>();
            }
            for (T item : rs) {
                map.put((K) primary.get(item), item);
            }
            return map;
        });
    }

    @Override
    public <T> Set<T> querySet(final Class<T> clazz, final SelectColumn selects, final Flipper flipper, final FilterNode node) {
        return new LinkedHashSet<>(querySheetCompose(true, false, true, clazz, selects, flipper, node).join().list(true));
    }

    @Override
    public <T> CompletableFuture<Set<T>> querySetAsync(final Class<T> clazz, final SelectColumn selects, final Flipper flipper, final FilterNode node) {
        return querySheetCompose(true, false, true, clazz, selects, flipper, node).thenApply((rs) -> new LinkedHashSet<>(rs.list(true)));
    }

    @Override
    public <T> List<T> queryList(final Class<T> clazz, final SelectColumn selects, final Flipper flipper, final FilterNode node) {
        return querySheetCompose(true, false, false, clazz, selects, flipper, node).join().list(true);
    }

    @Override
    public <T> CompletableFuture<List<T>> queryListAsync(final Class<T> clazz, final SelectColumn selects, final Flipper flipper, final FilterNode node) {
        return querySheetCompose(true, false, false, clazz, selects, flipper, node).thenApply((rs) -> rs.list(true));
    }

    @Override
    public <T> Sheet<T> querySheet(final Class<T> clazz, final SelectColumn selects, final Flipper flipper, final FilterNode node) {
        return querySheetCompose(true, true, false, clazz, selects, flipper, node).join();
    }

    @Override
    public <T> CompletableFuture<Sheet<T>> querySheetAsync(final Class<T> clazz, final SelectColumn selects, final Flipper flipper, final FilterNode node) {
        return querySheetCompose(true, true, false, clazz, selects, flipper, node);
    }

    protected <T> CompletableFuture<Sheet<T>> querySheetCompose(final boolean readcache, final boolean needtotal, final boolean distinct, final Class<T> clazz, final SelectColumn selects, final Flipper flipper, final FilterNode node) {
        final EntityInfo<T> info = loadEntityInfo(clazz);
        final EntityCache<T> cache = info.getCache();
        if (readcache && cache != null && cache.isFullLoaded()) {
            if (node == null || isCacheUseable(node, this)) {
                if (info.isLoggable(logger, Level.FINEST, " cache query predicate = ")) {
                    logger.finest(clazz.getSimpleName() + " cache query predicate = " + (node == null ? null : createPredicate(node, cache)));
                }
                return CompletableFuture.completedFuture(cache.querySheet(needtotal, distinct, selects, flipper, node));
            }
        }
        return querySheetCompose(info, readcache, needtotal, distinct, selects, flipper, node);
    }

    protected <T> CompletableFuture<Sheet<T>> querySheetCompose(EntityInfo<T> info, final boolean readcache, boolean needtotal, final boolean distinct, SelectColumn selects, Flipper flipper, FilterNode node) {
        MongoCollection<T> collection = getReadMongoCollection(info);
        final Bson filter = createFilterBson(info, node);
        CompletableFuture<Long> totalFuture;
        if (needtotal) {
            Publisher<Long> publisher = filter == null ? collection.countDocuments() : collection.countDocuments(filter);
            ReatorFuture<Long> future = new ReatorFuture<>();
            publisher.subscribe(future);
            totalFuture = future;
        } else {
            totalFuture = CompletableFuture.completedFuture(-1L);
        }
        return totalFuture.thenCompose(total -> {
            FindPublisher<T> publisher = collection.find();
            if (filter != null) {
                publisher.filter(filter);
            }
            if (selects != null) {
                publisher.projection(selects.isExcludable() ? Projections.exclude(selects.getColumns()) : Projections.include(selects.getColumns()));
            }
            if (flipper != null) {
                if (flipper.getOffset() > 0) {
                    publisher.skip(flipper.getOffset());
                }
                if (flipper.getLimit() > 0) {
                    publisher.limit(flipper.getLimit());
                }
                Bson sortBson = createSortBson(flipper);
                if (sortBson != null) {
                    publisher.sort(sortBson);
                }
            }
            ReatorListFuture<T> future = new ReatorListFuture<>();
            publisher.subscribe(future);
            return future.thenApply(v -> new Sheet<>(total, v));
        });
    }

    @Override
    public void close() throws Exception {
        if (this.readMongoClient != null) {
            this.readMongoClient.close();
        }
        if (this.writeMongoClient != null && this.readMongoClient != this.writeMongoClient) {
            this.writeMongoClient.close();
        }
    }

    public static class ReatorFuture<T> extends CompletableFuture<T> implements Subscriber<T> {

        protected T rs;

        @Override
        public void onSubscribe(Subscription subscription) {
            subscription.request(Integer.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            rs = item;
        }

        @Override
        public void onError(Throwable t) {
            completeExceptionally(t);
        }

        @Override
        public void onComplete() {
            complete(rs);
        }
    }

    public static class ReatorListFuture<T> extends CompletableFuture<List<T>> implements Subscriber<T> {

        protected List<T> rs;

        @Override
        public void onSubscribe(Subscription s) {
            s.request(Integer.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            if (rs == null) {
                rs = new ArrayList<>();
            }
            rs.add(item);
        }

        @Override
        public void onError(Throwable t) {
            completeExceptionally(t);
        }

        @Override
        public void onComplete() {
            complete(rs == null ? new ArrayList<>() : rs);
        }

    }

}
