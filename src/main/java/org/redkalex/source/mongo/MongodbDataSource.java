/*
 *
 */

package org.redkalex.source.mongo;

import com.mongodb.client.model.BsonField;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.io.Serializable;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.redkale.source.AbstractDataSource;
import org.redkale.source.ColumnValue;
import org.redkale.source.EntityInfo;
import org.redkale.source.FilterFunc;
import org.redkale.source.FilterNode;
import org.redkale.source.Flipper;

/**
 *
 * @author zhangjx
 */
public abstract class MongodbDataSource extends AbstractDataSource {

    public abstract MongoClient getReadMongoClient();

    public abstract MongoDatabase getReadMongoDatabase();

    public abstract <T> MongoCollection<T> getReadMongoCollection(EntityInfo<T> info);

    public abstract <T> MongoCollection<Document> getReadMongoDocumentCollection(EntityInfo<T> info);

    public abstract MongoClient getWriteMongoClient();

    public abstract MongoDatabase getWriteMongoDatabase();

    public abstract <T> MongoCollection<T> getWriteMongoCollection(EntityInfo<T> info);

    public abstract <T> MongoCollection<Document> getWriteMongoDocumentCollection(EntityInfo<T> info);

    public abstract Bson createSortBson(Flipper flipper);

    public abstract <T> List<Bson> createUpdateBson(EntityInfo<T> info, ColumnValue... values);

    public abstract <T> Bson createUpdateBson(EntityInfo<T> info, ColumnValue colval);

    public abstract BsonField createBsonField(FilterFunc func, String fieldName, Serializable column);

    public abstract <T> Bson createFilterBson(EntityInfo<T> info, FilterNode node);
}
