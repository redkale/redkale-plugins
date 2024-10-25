/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.net.*;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.IntFunction;
import java.util.logging.*;
import java.util.stream.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.ResourceChanged;
import org.redkale.annotation.ResourceType;
import org.redkale.convert.json.*;
import org.redkale.inject.Resourcable;
import org.redkale.inject.ResourceEvent;
import org.redkale.persistence.Entity;
import org.redkale.service.*;
import org.redkale.source.*;
import static org.redkale.source.DataSources.*;
import org.redkale.util.*;

/**
 * ElasticSearch实现 <br>
 *
 * @author zhangjx
 * @since 2.4.0
 */
@Local
@AutoLoad(false)
@SuppressWarnings("unchecked")
@ResourceType(SearchSource.class)
public final class OpenSearchSource extends AbstractService implements SearchSource, AutoCloseable, Resourcable {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected final IntFunction<Serializable[]> serialArrayFunc = Utility.serialArrayFunc();

    // key: table, 不能是class，因为存在分表的情况
    protected final ConcurrentHashMap<String, Object> checkedIndexClasses = new ConcurrentHashMap();

    protected Properties confProps;

    protected String name;

    protected URI[] uris;

    protected HttpClient httpClient;

    @Override
    public void init(AnyValue config) {
        super.init(config);
        Properties props = new Properties();
        config.forEach((k, v) -> props.put(k, decryptProperty(k, v)));
        initFromProperties(props);
    }

    protected void initFromProperties(Properties props) {
        ProxySelector proxySelector = null;
        Authenticator authenticator = null;
        List<URI> us = new ArrayList<>();
        String url = props.getProperty(DATA_SOURCE_URL);
        if (url.startsWith("search://")) {
            url = url.replace("search://", "http://");
        } else if (url.startsWith("searchs://")) {
            url = url.replace("searchs://", "https://");
        }
        for (String str : url.split(";")) {
            if (str.trim().isEmpty()) {
                continue;
            }
            us.add(URI.create(str.trim()));
        }

        String proxyAddr = props.getProperty(DATA_SOURCE_PROXY_ADDRESS);
        if (proxyAddr != null
                && !proxyAddr.isEmpty()
                && proxyAddr.contains(":")
                && "true".equalsIgnoreCase(props.getProperty(DATA_SOURCE_PROXY_ENABLE, "true"))) {
            String proxyType = props.getProperty(DATA_SOURCE_PROXY_TYPE, "HTTP").toUpperCase();
            int pos = proxyAddr.indexOf(':');
            SocketAddress addr =
                    new InetSocketAddress(proxyAddr.substring(0, pos), Integer.parseInt(proxyAddr.substring(pos + 1)));
            proxySelector = SimpleProxySelector.create(new Proxy(Proxy.Type.valueOf(proxyType), addr));
        }
        String proxyUser = props.getProperty(DATA_SOURCE_PROXY_USER);
        if (proxyUser != null && !proxyUser.isEmpty()) {
            char[] proxyPassword =
                    props.getProperty(DATA_SOURCE_PROXY_PASSWORD, "").toCharArray();
            authenticator = new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(proxyUser, proxyPassword);
                }
            };
        }
        HttpClient.Builder builder = HttpClient.newBuilder();
        if (proxySelector != null) {
            builder = builder.proxy(proxySelector);
        }
        if (authenticator != null) {
            builder = builder.authenticator(authenticator);
        }
        HttpClient c = builder.build();
        this.uris = us.toArray(new URI[us.size()]);
        this.httpClient = c;
        this.confProps = props;
    }

    @ResourceChanged
    public void onResourceChange(ResourceEvent[] events) {
        if (Utility.isEmpty(events)) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        Properties newProps = new Properties();
        newProps.putAll(this.confProps);
        for (ResourceEvent event : events) { // 可能需要解密
            String newValue = decryptProperty(event.name(), event.newValue().toString());
            newProps.put(event.name(), newValue);
            sb.append("DataSource(name=")
                    .append(resourceName())
                    .append(") change '")
                    .append(event.name())
                    .append("' to '")
                    .append(event.coverNewValue())
                    .append("'\r\n");
        }
        initFromProperties(newProps);
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
    }

    // 解密可能存在的加密字段, 可重载
    protected String decryptProperty(String key, String value) {
        return value;
    }

    @Override
    public void destroy(AnyValue config) {
        super.destroy(config);
    }

    @Override
    public void close() throws Exception {}

    @Override
    public String toString() {
        if (confProps == null) {
            return getClass().getSimpleName() + "_" + Objects.hashCode(this) + "{}"; // compileMode模式下会为null
        }
        return getClass().getSimpleName() + "_" + Objects.hashCode(this) + "{url = "
                + confProps.getProperty(DATA_SOURCE_URL) + "}";
    }

    @Override
    @Local
    public <T> void compile(Class<T> clazz) {
        SearchInfo.compile(clazz, this);
        JsonFactory.root().findDecoder(TypeToken.createParameterizedType(null, FindResult.class, clazz));
        JsonFactory.root().findDecoder(TypeToken.createParameterizedType(null, SearchResult.class, clazz));
    }

    @Override
    public String getType() {
        return "search";
    }

    @Override
    public String resourceName() {
        return name;
    }

    // 检查对象是否都是同一个Entity类
    protected <T> void checkEntity(String action, T... entitys) {
        Class clazz = null;
        for (T val : entitys) {
            if (clazz == null) {
                clazz = val.getClass();
                if (clazz.getAnnotation(Entity.class) == null) {
                    throw new SourceException("Entity Class " + clazz + " must be on Annotation @Entity");
                }
            } else if (clazz != val.getClass()) {
                throw new SourceException("DataSource." + action + " must the same Class Entity, but diff is " + clazz
                        + " and " + val.getClass());
            }
        }
    }

    protected CompletableFuture<RetResult<String>> deleteAsync(CharSequence path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uris[0].toString() + path))
                .timeout(Duration.ofMillis(10_000))
                .header("Content-Type", "application/json");
        return httpClient
                .sendAsync(builder.DELETE().build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenApply(resp -> {
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.log(Level.FINEST, path + " delete --> " + resp.body());
                    }
                    return new RetResult(resp.body()).retcode(resp.statusCode());
                });
    }

    protected CompletableFuture<RetResult<String>> getAsync(CharSequence path) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uris[0].toString() + path))
                .timeout(Duration.ofMillis(10_000))
                .header("Content-Type", "application/json");
        return httpClient
                .sendAsync(builder.GET().build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenApply(resp -> {
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.log(Level.FINEST, path + " get --> " + resp.body());
                    }
                    return new RetResult(resp.body()).retcode(resp.statusCode());
                });
    }

    protected <T> CompletableFuture<RetResult<String>> postAsync(
            CharSequence path, SearchInfo<T> info, SearchRequest body) {
        return postAsync(path, convertSearchRequest(info, body));
    }

    protected <T> CompletableFuture<RetResult<String>> postAsync(
            CharSequence path, SearchInfo<T> info, UpdatePart body) {
        return postAsync(path, info.getConvert().convertToBytes(body));
    }

    protected <T> CompletableFuture<RetResult<String>> postEntityAsync(
            CharSequence path, SearchInfo<T> info, T entity) {
        return postAsync(path, info.getConvert().convertToBytes(entity));
    }

    protected CompletableFuture<RetResult<String>> postAsync(CharSequence path, byte[] body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uris[0].toString() + path))
                .timeout(Duration.ofMillis(10_000))
                .header("Content-Type", "application/json");
        HttpRequest.BodyPublisher publisher =
                body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(body);
        return httpClient
                .sendAsync(builder.POST(publisher).build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenApply(resp -> {
                    // SearchSource可能作为logging输出源，故此insert不能打印日志
                    // if (finest) logger.log(Level.FINEST, path + " post: " + (body == null ? null : new String(body,
                    // StandardCharsets.UTF_8)) + " --> " + resp.body());
                    return new RetResult(resp.body()).retcode(resp.statusCode());
                });
    }

    protected <T> CompletableFuture<RetResult<String>> putAsync(
            CharSequence path, SearchInfo<T> info, Map<String, Object> map) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uris[0].toString() + path))
                .timeout(Duration.ofMillis(10_000))
                .header("Content-Type", "application/json");
        return httpClient
                .sendAsync(
                        builder.PUT(HttpRequest.BodyPublishers.ofByteArray(
                                        info.getConvert().convertToBytes(map)))
                                .build(),
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenApply(resp -> {
                    // SearchSource可能作为logging输出源，故此insert不能打印日志
                    // if (finest) logger.log(Level.FINEST, path + " put: " + info.getConvert().convertTo(map) + " --> "
                    // + resp.body());
                    return new RetResult(resp.body()).retcode(resp.statusCode());
                });
    }

    protected <T> CompletableFuture<RetResult<String>> bulkAsync(CharSequence path, CharSequence body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(uris[0].toString() + path))
                .timeout(Duration.ofMillis(10_000))
                .header("Content-Type", "application/x-ndjson");
        return httpClient
                .sendAsync(
                        builder.POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                                .build(),
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenApply(resp -> {
                    // SearchSource可能作为logging输出源，故此insert不能打印日志
                    // if (finest) logger.log(Level.FINEST, path + " bulk: " + body + " --> " + resp.body());
                    return new RetResult(resp.body()).retcode(resp.statusCode());
                });
    }

    protected <T> CharSequence getQueryTable(SearchInfo<T> info, FilterNode node) {
        if (node == null) {
            return info.getTable(node);
        }
        SearchQuery bean = (SearchQuery) node.findValue(SearchQuery.SEARCH_FILTER_NAME);
        if (bean == null || bean.searchClasses() == null) {
            return info.getTable(node);
        }
        StringBuilder sb = new StringBuilder();
        for (Class clazz : bean.searchClasses()) {
            if (clazz == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(SearchInfo.load(clazz).getTable(node));
        }
        return sb.length() > 0 ? sb : info.getTable(node);
    }

    protected <T> SearchRequest createSearchRequest(
            SearchInfo<T> info, SelectColumn selects, Flipper flipper, FilterNode node) {
        if (flipper == null && node == null) {
            return SearchRequest.createMatchAll();
        }
        return new SearchRequest().flipper(flipper).filterNode(info, node);
    }

    protected <T> byte[] convertSearchRequest(final SearchInfo<T> info, SearchRequest bean) {
        if (bean == null) {
            return null;
        }
        return info.getConvert().convertToBytes(bean);
    }

    protected <T> SearchInfo loadSearchInfo(Class<T> clazz) {
        return SearchInfo.load(clazz);
    }

    protected <T> void checkIndexSync(final SearchInfo<T> info, String table0) {
        if (info.isVirtual()) {
            return;
        }
        String table = table0 == null ? info.getOriginTable() : table0;
        checkedIndexClasses.computeIfAbsent(table, c -> {
            final StringBuilder path =
                    new StringBuilder().append('/').append(table).append("/_mapping");
            CompletableFuture future = getAsync(path).thenCompose(resp -> {
                if (resp.getRetcode() == 404) { // 还没有表结构
                    return putAsync("/" + table, info, info.createIndexMap())
                            .thenApply(resp2 -> resp2.getRetcode() != 200 ? null : resp);
                }
                if (resp.getRetcode() != 200) {
                    return null;
                }
                Map<String, SearchMapping> rs =
                        JsonConvert.root().convertFrom(SearchMapping.MAPPING_MAP_TYPE, resp.getResult());
                SearchMapping sm = rs == null ? null : rs.get(table);
                if (sm == null || sm.mappings == null || !sm.mappings.equal(info.getMappingTypes())) {
                    return updateMappingAsync(info.getType(), table)
                            .thenCompose(v -> v == 1 ? CompletableFuture.completedFuture(1) : null);
                }
                return CompletableFuture.completedFuture(1);
            });
            return future == null ? null : future.join();
        });
    }

    protected <T> CompletableFuture<Integer> insertOneAsync(final SearchInfo<T> info, T entity) {
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(info.getTable(entity))
                .append("/_create/")
                .append(info.getPrimary().get(entity));
        return postEntityAsync(path, info, entity).thenApply(resp -> {
            if (resp.getRetcode() != 200 && resp.getRetcode() != 201) {
                throw new SourceException(
                        "insert response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = info.getConvert().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? 0 : rs._shards.successful;
            }
        });
    }

    @Override
    public int batch(final DataBatch batch) {
        return batchAsync(batch).join();
    }

    @Override
    public CompletableFuture<Integer> batchAsync(final DataBatch batch) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> int insert(T... entitys) {
        return insertAsync(entitys).join();
    }

    @Override
    public <T> CompletableFuture<Integer> insertAsync(T... entitys) {
        checkEntity("insert", entitys);
        final SearchInfo<T> info = loadSearchInfo(entitys[0].getClass());
        if (entitys.length == 1) {
            return insertOneAsync(info, entitys[0]);
        }
        final Attribute<T, Serializable> primary = info.getPrimary();
        final Map<String, StringBuilder> tables = new LinkedHashMap<>();
        for (T entity : entitys) {
            String table = info.getTable(entity);
            StringBuilder sb = tables.computeIfAbsent(table, t -> new StringBuilder());
            sb.append("{\"create\":{\"_id\":\"")
                    .append(primary.get(entity))
                    .append("\"}}\n")
                    .append(info.getConvert().convertTo(entity))
                    .append('\n');
        }
        final List<CompletableFuture<Integer>> futures = new ArrayList<>(tables.size());
        tables.forEach((table, sb) -> {
            checkIndexSync(info, table);
            final StringBuilder path =
                    new StringBuilder().append('/').append(table).append("/_bulk");
            futures.add(bulkAsync(path, sb).thenApply(resp -> {
                if (resp.getRetcode() != 200) {
                    throw new SourceException(
                            "insert response code = " + resp.getRetcode() + ", body = " + resp.getResult());
                } else {
                    BulkResult rs = JsonConvert.root().convertFrom(BulkResult.class, resp.getResult());
                    return rs == null ? -1 : rs.successCount();
                }
            }));
        });
        if (futures.size() == 1) {
            return futures.get(0);
        }
        return Utility.allOfFutures(futures).thenApply(list -> {
            int count = 0;
            for (Integer c : list) {
                if (c < 0) {
                    return c; // 存在失败的直接返回，已成功的暂时无法进行回滚
                }
                count += c;
            }
            return count;
        });
    }

    protected <T> CompletableFuture<Integer> deleteOneAsync(final SearchInfo<T> info, Class<T> clazz, Serializable pk) {
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(info.getTable(pk))
                .append("/_doc/")
                .append(pk);
        return deleteAsync(path).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "delete response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
            }
        });
    }

    @Override
    public <T> int delete(T... entitys) {
        return deleteAsync(entitys).join();
    }

    @Override
    public <T> CompletableFuture<Integer> deleteAsync(T... entitys) {
        checkEntity("delete", entitys);
        final SearchInfo<T> info = loadSearchInfo(entitys[0].getClass());
        final Attribute<T, Serializable> primary = info.getPrimary();
        if (entitys.length == 1) {
            return deleteOneAsync(info, (Class) entitys[0].getClass(), primary.get(entitys[0]));
        }
        final Map<String, StringBuilder> tables = new LinkedHashMap<>();
        for (T entity : entitys) {
            String table = info.getTable(entity);
            StringBuilder sb = tables.computeIfAbsent(table, t -> new StringBuilder());
            sb.append("{\"delete\":{\"_id\":\"").append(primary.get(entity)).append("\"}}\n");
        }
        final List<CompletableFuture<Integer>> futures = new ArrayList<>(tables.size());
        tables.forEach((table, sb) -> {
            checkIndexSync(info, table);
            final StringBuilder path =
                    new StringBuilder().append('/').append(table).append("/_bulk");
            futures.add(bulkAsync(path, sb).thenApply(resp -> {
                if (resp.getRetcode() == 404) {
                    return 0;
                }
                if (resp.getRetcode() != 200) {
                    throw new SourceException(
                            "delete response code = " + resp.getRetcode() + ", body = " + resp.getResult());
                } else {
                    BulkResult rs = JsonConvert.root().convertFrom(BulkResult.class, resp.getResult());
                    return rs == null ? -1 : rs.successCount();
                }
            }));
        });
        if (futures.size() == 1) {
            return futures.get(0);
        }
        return Utility.allOfFutures(futures).thenApply(list -> {
            int count = 0;
            for (Integer c : list) {
                if (c < 0) {
                    return c; // 存在失败的直接返回，已成功的暂时无法进行回滚
                }
                count += c;
            }
            return count;
        });
    }

    @Override
    public <T> int delete(Class<T> clazz, Serializable... pks) {
        return deleteAsync(clazz, pks).join();
    }

    @Override
    public <T> CompletableFuture<Integer> deleteAsync(Class<T> clazz, Serializable... pks) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        if (pks.length == 1) {
            return deleteOneAsync(info, clazz, pks[0]);
        }
        final Map<String, StringBuilder> tables = new LinkedHashMap<>();
        for (Serializable pk : pks) {
            String table = info.getTable(pk);
            StringBuilder sb = tables.computeIfAbsent(table, t -> new StringBuilder());
            sb.append("{\"delete\":{\"_id\":\"").append(pk).append("\"}}\n");
        }
        final List<CompletableFuture<Integer>> futures = new ArrayList<>(tables.size());
        tables.forEach((table, sb) -> {
            checkIndexSync(info, table);
            final StringBuilder path =
                    new StringBuilder().append('/').append(table).append("/_bulk");
            futures.add(bulkAsync(path, sb).thenApply(resp -> {
                if (resp.getRetcode() == 404) {
                    return 0;
                }
                if (resp.getRetcode() != 200) {
                    throw new SourceException(
                            "delete response code = " + resp.getRetcode() + ", body = " + resp.getResult());
                } else {
                    BulkResult rs = JsonConvert.root().convertFrom(BulkResult.class, resp.getResult());
                    return rs == null ? -1 : rs.successCount();
                }
            }));
        });
        if (futures.size() == 1) {
            return futures.get(0);
        }
        return Utility.allOfFutures(futures).thenApply(list -> {
            int count = 0;
            for (Integer c : list) {
                if (c < 0) {
                    return c; // 存在失败的直接返回，已成功的暂时无法进行回滚
                }
                count += c;
            }
            return count;
        });
    }

    @Override
    public <T> int delete(Class<T> clazz, FilterNode node) {
        return deleteAsync(clazz, (Flipper) null, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> deleteAsync(Class<T> clazz, FilterNode node) {
        return deleteAsync(clazz, (Flipper) null, node);
    }

    @Override
    public <T> int delete(Class<T> clazz, Flipper flipper, FilterNode node) {
        return deleteAsync(clazz, flipper, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> deleteAsync(Class<T> clazz, Flipper flipper, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(getQueryTable(info, node))
                .append("/_delete_by_query");
        return postAsync(path, info, createSearchRequest(info, null, flipper, node))
                .thenApply(resp -> {
                    if (resp.getRetcode() == 404) {
                        return 0;
                    }
                    if (resp.getRetcode() != 200) {
                        throw new SourceException(
                                "delete response code = " + resp.getRetcode() + ", body = " + resp.getResult());
                    } else {
                        ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                        return rs == null || rs._shards == null ? -1 : rs._shards.successful;
                    }
                });
    }

    @Override
    public <T> int clearTable(Class<T> clazz) {
        return clearTableAsync(clazz, (FilterNode) null).join();
    }

    @Override
    public <T> CompletableFuture<Integer> clearTableAsync(Class<T> clazz) {
        return clearTableAsync(clazz, (FilterNode) null);
    }

    @Override
    public <T> int clearTable(Class<T> clazz, FilterNode node) {
        return clearTableAsync(clazz, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> clearTableAsync(Class<T> clazz, FilterNode node) {
        //        final SearchInfo<T> info = loadSearchInfo(clazz);
        //        final String path = "/" + info.getTable() + "/_delete_by_query";
        //
        // //{"took":24,"timed_out":false,"total":3,"deleted":3,"batches":1,"version_conflicts":0,"noops":0,"retries":{"bulk":0,"search":0},"throttled_millis":0,"requests_per_second":-1,"throttled_until_millis":0,"failures":[]}
        //        return postAsync(path, HttpRequest.BodyPublishers.ofByteArray(BYTES_QUERY_MATCH_ALL)).thenApply(resp
        // -> {
        //            if (resp.getRetcode() != 200) {
        //                throw new SourceException("clearTable response code = " + resp.getRetcode() + ", body = " +
        // resp.getResult());
        //            } else {
        //                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
        //                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
        //            }
        //        });
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path = new StringBuilder().append("/").append(getQueryTable(info, node));
        return deleteAsync(path).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "clearTable response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else { // {"acknowledged" : true}
                Map<String, String> rs =
                        JsonConvert.root().convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, resp.getResult());
                return rs == null || !"true".equals(rs.get("acknowledged")) ? -1 : 1;
            }
        });
    }

    @Override
    public <T> int createTable(final Class<T> clazz, final Serializable pk) {
        return createTableAsync(clazz, pk).join();
    }

    @Override
    public <T> CompletableFuture<Integer> createTableAsync(final Class<T> clazz, final Serializable pk) {
        return CompletableFuture.completedFuture(0);
    }

    @Override
    public <T> int dropTable(Class<T> clazz) {
        return dropTableAsync(clazz, (FilterNode) null).join();
    }

    @Override
    public <T> CompletableFuture<Integer> dropTableAsync(Class<T> clazz) {
        return dropTableAsync(clazz, (FilterNode) null);
    }

    @Override
    public <T> int dropTable(Class<T> clazz, FilterNode node) {
        return dropTableAsync(clazz, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> dropTableAsync(Class<T> clazz, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path = new StringBuilder().append('/').append(getQueryTable(info, node));
        return deleteAsync(path).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "dropTable response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else { // {"acknowledged" : true}
                checkedIndexClasses.remove(clazz);
                Map<String, String> rs =
                        JsonConvert.root().convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, resp.getResult());
                return rs == null || !"true".equals(rs.get("acknowledged")) ? -1 : 1;
            }
        });
    }

    protected <T> CompletableFuture<Integer> updateOneAsync(final SearchInfo<T> info, T entity) {
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(info.getTable(entity))
                .append('/')
                .append(info.getPrimary().get(entity))
                .append("/_update");
        return postEntityAsync(path, info, entity).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "update response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
            }
        });
    }

    @Override
    public <T> int update(T... entitys) {
        return updateAsync(entitys).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateAsync(T... entitys) {
        checkEntity("update", entitys);
        final SearchInfo<T> info = loadSearchInfo(entitys[0].getClass());
        if (entitys.length == 1) {
            return updateOneAsync(info, entitys[0]);
        }
        final Attribute<T, Serializable> primary = info.getPrimary();
        final Map<String, StringBuilder> tables = new LinkedHashMap<>();
        for (T entity : entitys) {
            String table = info.getTable(entity);
            StringBuilder sb = tables.computeIfAbsent(table, t -> new StringBuilder());
            sb.append("{\"update\":{\"_id\":\"")
                    .append(primary.get(entity))
                    .append("\"}}\n")
                    .append(info.getConvert().convertTo(entity))
                    .append('\n');
        }
        final List<CompletableFuture<Integer>> futures = new ArrayList<>(tables.size());
        tables.forEach((table, sb) -> {
            final StringBuilder path =
                    new StringBuilder().append('/').append(table).append("/_bulk");
            futures.add(bulkAsync(path, sb).thenApply(resp -> {
                if (resp.getRetcode() == 404) {
                    return 0;
                }
                if (resp.getRetcode() != 200) {
                    throw new SourceException(
                            "update response code = " + resp.getRetcode() + ", body = " + resp.getResult());
                } else {
                    BulkResult rs = JsonConvert.root().convertFrom(BulkResult.class, resp.getResult());
                    return rs == null ? -1 : rs.successCount();
                }
            }));
        });
        if (futures.size() == 1) {
            return futures.get(0);
        }
        return Utility.allOfFutures(futures).thenApply(list -> {
            int count = 0;
            for (Integer c : list) {
                if (c < 0) {
                    return c; // 存在失败的直接返回，已成功的暂时无法进行回滚
                }
                count += c;
            }
            return count;
        });
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, Serializable pk, String column, Serializable value) {
        return updateColumnAsync(clazz, pk, column, value).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(
            Class<T> clazz, Serializable pk, String column, Serializable value) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(info.getTable(pk))
                .append('/')
                .append(pk)
                .append("/_update");
        return postAsync(path, info, new UpdatePart(info, column, value)).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "update response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
            }
        });
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, String column, Serializable value, FilterNode node) {
        return updateColumnAsync(clazz, column, value, node).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(
            Class<T> clazz, String column, Serializable value, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path =
                new StringBuilder().append('/').append(info.getTable(node)).append("/_update_by_query");
        return postAsync(path, info, new UpdatePart(info, column, value)).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "update response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
            }
        });
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, Serializable pk, ColumnValue... values) {
        if (values.length == 0) {
            return 0;
        }
        return updateColumnAsync(clazz, pk, values).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(Class<T> clazz, Serializable pk, ColumnValue... values) {
        if (values.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(info.getTable(pk))
                .append('/')
                .append(pk)
                .append("/_update");
        return postAsync(path, info, new UpdatePart(info, values)).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "updateColumn response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
            }
        });
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, FilterNode node, ColumnValue... values) {
        return updateColumnAsync(clazz, node, (Flipper) null, values).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(Class<T> clazz, FilterNode node, ColumnValue... values) {
        return updateColumnAsync(clazz, node, (Flipper) null, values);
    }

    @Override
    public <T> int updateColumn(Class<T> clazz, FilterNode node, Flipper flipper, ColumnValue... values) {
        return updateColumnAsync(clazz, node, flipper, values).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(
            Class<T> clazz, FilterNode node, Flipper flipper, ColumnValue... values) {
        if (values.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path =
                new StringBuilder().append('/').append(info.getTable(node)).append("/_update_by_query");
        SearchRequest bean = createSearchRequest(info, null, flipper, node);
        if (bean == null) {
            bean = new SearchRequest();
        }
        bean.script = new UpdatePart(info, values).script;
        return postAsync(path, info, bean).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "updateColumn response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
            }
        });
    }

    @Override
    public <T> int updateColumn(T entity, String... columns) {
        if (columns.length == 0) {
            return 0;
        }
        return updateColumnAsync(entity, columns).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(T entity, String... columns) {
        if (columns.length == 0) {
            return CompletableFuture.completedFuture(0);
        }
        final SearchInfo<T> info = loadSearchInfo(entity.getClass());
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(info.getTable(entity))
                .append('/')
                .append(info.getPrimary().get(entity))
                .append("/_update");
        final Map<String, Serializable> map = new LinkedHashMap<>();
        for (String col : columns) {
            Attribute<T, Serializable> attr = info.getUpdateAttribute(col);
            map.put(col, attr.get(entity));
        }
        return postAsync(path, info, new UpdatePart(info, map)).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "updateColumn response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
            }
        });
    }

    @Override
    public <T> int updateColumn(T entity, FilterNode node, String... columns) {
        return updateColumnAsync(entity, node, SelectColumn.includes(columns)).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(T entity, FilterNode node, String... columns) {
        return updateColumnAsync(entity, node, SelectColumn.includes(columns));
    }

    @Override
    public <T> int updateColumn(T entity, FilterNode node, SelectColumn selects) {
        return updateColumnAsync(entity, node, selects).join();
    }

    @Override
    public <T> CompletableFuture<Integer> updateColumnAsync(T entity, FilterNode node, SelectColumn selects) {
        if (entity == null) {
            return CompletableFuture.completedFuture(0);
        }
        final SearchInfo<T> info = loadSearchInfo(entity.getClass());
        final StringBuilder path =
                new StringBuilder().append('/').append(info.getTable(node)).append("/_update_by_query");
        SearchRequest bean = createSearchRequest(info, null, null, node);
        if (bean == null) {
            bean = new SearchRequest();
        }
        bean.script = new UpdatePart(info, entity, selects);
        return postAsync(path, info, bean).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return 0;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "updateColumn response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                ActionResult rs = JsonConvert.root().convertFrom(ActionResult.class, resp.getResult());
                return rs == null || rs._shards == null ? -1 : rs._shards.successful;
            }
        });
    }

    @Override
    public Number getNumberResult(Class entityClass, FilterFunc func, Number defVal, String column, FilterNode node) {
        return getNumberResultAsync(entityClass, func, defVal, column, node).join();
    }

    @Override
    public CompletableFuture<Number> getNumberResultAsync(
            Class entityClass, FilterFunc func, Number defVal, String column, FilterNode node) {
        final SearchInfo<?> info = loadSearchInfo(entityClass);
        final Attribute keyAttr = (Attribute) info.getAttribute(column);
        if (keyAttr == null) {
            return CompletableFuture.failedFuture(
                    new RuntimeException("not found column " + column + " in " + entityClass));
        }
        final StringBuilder path =
                new StringBuilder().append('/').append(info.getTable(node)).append("/_search?_source=false");
        SearchRequest bean = createSearchRequest(info, null, null, node);
        SearchRequest.QueryFilterItem aggs = new SearchRequest.QueryFilterItem();
        String funcesname = func == FilterFunc.COUNT
                ? "value_count"
                : (func == FilterFunc.DISTINCTCOUNT
                        ? "cardinality"
                        : func.name().toLowerCase()); // func_count:为BucketItem类其中的字段名
        String field = column == null ? info.getPrimary().field() : column;
        aggs.put("func_count", Utility.ofMap(funcesname, Utility.ofMap("field", field)));
        bean.aggs = aggs;
        bean.size = 0;
        return postAsync(path, info, bean).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return defVal;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "getNumberResult response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                SearchResult rs = JsonConvert.root().convertFrom(info.getSearchResultType(), resp.getResult());
                if (rs == null || rs.timed_out || rs.aggregations == null) {
                    return defVal;
                }
                SearchResult.Aggregations aggrs = (SearchResult.Aggregations) rs.aggregations.get("func_count");
                if (aggrs == null) {
                    return defVal;
                }
                Number d = aggrs.value;
                if (func == FilterFunc.COUNT || func == FilterFunc.DISTINCTCOUNT) {
                    d = d.intValue();
                } else if (keyAttr != null) {
                    if (keyAttr != null && (func == FilterFunc.MIN || func == FilterFunc.MAX)) {
                        if (keyAttr.type() == short.class) {
                            d = d.shortValue();
                        } else if (keyAttr.type() == int.class || keyAttr.type() == char.class) {
                            d = d.intValue();
                        } else if (keyAttr.type() == long.class) {
                            d = d.longValue();
                        } else if (keyAttr.type() == float.class) {
                            d = d.floatValue();
                        }
                    }
                }
                return d;
            }
        });
    }

    @Override
    public <N extends Number> Map<String, N> getNumberMap(
            Class entityClass, FilterNode node, FilterFuncColumn... columns) {
        return (Map) getNumberMapAsync(entityClass, node, columns).join();
    }

    @Override
    public <N extends Number> CompletableFuture<Map<String, N>> getNumberMapAsync(
            Class entityClass, FilterNode node, FilterFuncColumn... columns) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T, K extends Serializable, N extends Number> Map<K, N> queryColumnMap(
            Class<T> entityClass, String keyColumn, FilterFunc func, String funcColumn, FilterNode node) {
        return (Map) queryColumnMapAsync(entityClass, keyColumn, func, funcColumn, node)
                .join();
    }

    @Override
    public <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N>> queryColumnMapAsync(
            Class<T> entityClass, String keyColumn, FilterFunc func, String funcColumn, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(entityClass);
        final Attribute<T, Serializable> keyAttr = (Attribute) info.getAttribute(keyColumn);
        final Attribute<T, Serializable> funcAttr =
                funcColumn == null ? null : (Attribute) info.getAttribute(funcColumn);
        if (keyAttr == null) {
            return CompletableFuture.failedFuture(
                    new RuntimeException("not found column " + keyColumn + " in " + entityClass));
        }
        if (funcColumn != null && funcAttr == null) {
            return CompletableFuture.failedFuture(
                    new RuntimeException("not found column " + funcColumn + " in " + entityClass));
        }
        final StringBuilder path =
                new StringBuilder().append('/').append(info.getTable(node)).append("/_search?_source=false");
        SearchRequest bean = createSearchRequest(info, null, null, node);
        SearchRequest.QueryFilterItem aggs = new SearchRequest.QueryFilterItem();
        String funcesname = func == FilterFunc.COUNT
                ? "value_count"
                : (func == FilterFunc.DISTINCTCOUNT
                        ? "cardinality"
                        : func.name().toLowerCase()); // func_count:为BucketItem类其中的字段名
        String field = funcColumn == null ? info.getPrimary().field() : funcColumn;
        aggs.put(
                "key_" + keyColumn,
                Utility.ofMap(
                        "terms",
                        Utility.ofMap("field", keyColumn),
                        "aggs",
                        Utility.ofMap("func_count", Utility.ofMap(funcesname, Utility.ofMap("field", field)))));
        bean.aggs = aggs;
        bean.size = 0;
        return postAsync(path, info, bean).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return new HashMap();
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "queryColumnMap response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                SearchResult<T> rs = JsonConvert.root().convertFrom(info.getSearchResultType(), resp.getResult());
                if (rs == null || rs.timed_out || rs.aggregations == null) {
                    return new HashMap();
                }
                SearchResult.Aggregations aggrs = rs.aggregations.get("key_" + keyColumn);
                if (aggrs == null || aggrs.buckets == null) {
                    return new HashMap();
                }
                final HashMap map = new HashMap();
                final Type kt = keyAttr.genericType();
                final JsonConvert convert = info.getConvert();
                for (SearchResult.BucketItem item : aggrs.buckets) {
                    if (item == null || item.key == null) {
                        continue;
                    }
                    Number d = item.funcCount();
                    if (func == FilterFunc.COUNT || func == FilterFunc.DISTINCTCOUNT) {
                        d = d.intValue();
                    } else if (funcAttr != null) {
                        if (funcAttr != null && (func == FilterFunc.MIN || func == FilterFunc.MAX)) {
                            if (funcAttr.type() == short.class) {
                                d = d.shortValue();
                            } else if (funcAttr.type() == int.class || funcAttr.type() == char.class) {
                                d = d.intValue();
                            } else if (funcAttr.type() == long.class) {
                                d = d.longValue();
                            } else if (funcAttr.type() == float.class) {
                                d = d.floatValue();
                            }
                        }
                    }
                    map.put(kt == String.class ? item.key : convert.convertFrom(kt, item.key), d);
                }
                return map;
            }
        });
    }

    @Override
    public <T, K extends Serializable, N extends Number> Map<K, N[]> queryColumnMap(
            Class<T> entityClass, ColumnNode[] funcNodes, String groupByColumn, FilterNode node) {
        return (Map)
                queryColumnMapAsync(entityClass, funcNodes, groupByColumn, node).join();
    }

    @Override
    public <T, K extends Serializable, N extends Number> CompletableFuture<Map<K, N[]>> queryColumnMapAsync(
            Class<T> entityClass, ColumnNode[] funcNodes, String groupByColumn, FilterNode node) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T, K extends Serializable, N extends Number> Map<K[], N[]> queryColumnMap(
            Class<T> entityClass, ColumnNode[] funcNodes, String[] groupByColumns, FilterNode node) {
        return (Map) queryColumnMapAsync(entityClass, funcNodes, groupByColumns, node)
                .join();
    }

    @Override
    public <T, K extends Serializable, N extends Number> CompletableFuture<Map<K[], N[]>> queryColumnMapAsync(
            Class<T> entityClass, ColumnNode[] funcNodes, String[] groupByColumns, FilterNode node) {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T> CompletableFuture<T> findAsync(Class<T> clazz, Serializable pk) {
        return findAsync(clazz, (SelectColumn) null, pk);
    }

    @Override
    public <T> T find(Class<T> clazz, SelectColumn selects, Serializable pk) {
        return findAsync(clazz, selects, pk).join();
    }

    @Override
    public <T> CompletableFuture<T> findAsync(Class<T> clazz, SelectColumn selects, Serializable pk) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(info.getTable(pk))
                .append("/_doc/")
                .append(pk);
        if (selects != null) {
            path.append("?_source")
                    .append(selects.isExcludable() ? "_excludes=" : "_includes=")
                    .append(Utility.joining(selects.getColumns(), ','));
        }
        return getAsync(path).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return null;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException("find response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                FindResult<T> rs = JsonConvert.root().convertFrom(info.getFindResultType(), resp.getResult());
                return rs == null || !rs.found ? null : rs._source;
            }
        });
    }

    @Override
    public <T> T find(Class<T> clazz, String column, Serializable colval) {
        return findAsync(clazz, column, colval).join();
    }

    @Override
    public <T> CompletableFuture<T> findAsync(Class<T> clazz, String column, Serializable colval) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final String table = info.getTableStrategy() == null
                ? info.getOriginTable()
                : info.getTable(FilterNodes.create(column, colval));
        final StringBuilder path = new StringBuilder().append('/').append(table).append("/_search");
        SearchRequest bean = new SearchRequest();
        bean.query = new SearchRequest.Query();
        bean.query.term = new SearchRequest.QueryFilterItem();
        bean.query.term.put(column, colval);
        bean.size = 1;
        return postAsync(path, info, bean).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return null;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException("find response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                SearchResult<T> rs = JsonConvert.root().convertFrom(info.getSearchResultType(), resp.getResult());
                if (rs == null || rs.timed_out || rs.hits == null) {
                    return null;
                }
                return rs.hits.hits != null && rs.hits.hits.length == 1 ? rs.hits.hits[0]._source : null;
            }
        });
    }

    @Override
    public <T> T find(Class<T> clazz, SelectColumn selects, FilterNode node) {
        return findAsync(clazz, selects, node).join();
    }

    @Override
    public <T> CompletableFuture<T> findAsync(Class<T> clazz, SelectColumn selects, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path =
                new StringBuilder().append('/').append(info.getTable(node)).append("/_search");
        if (selects != null) {
            path.append("?_source")
                    .append(selects.isExcludable() ? "_excludes=" : "_includes=")
                    .append(Utility.joining(selects.getColumns(), ','));
        }
        return postAsync(path, info, createSearchRequest(info, selects, null, node))
                .thenApply(resp -> {
                    if (resp.getRetcode() == 404) {
                        return null;
                    }
                    if (resp.getRetcode() != 200) {
                        throw new SourceException(
                                "find response code = " + resp.getRetcode() + ", body = " + resp.getResult());
                    } else {
                        SearchResult<T> rs =
                                JsonConvert.root().convertFrom(info.getSearchResultType(), resp.getResult());
                        if (rs == null || rs.timed_out || rs.hits == null) {
                            return null;
                        }
                        return rs.hits.hits != null && rs.hits.hits.length == 1 ? rs.hits.hits[0]._source : null;
                    }
                });
    }

    @Override
    public <T> T[] finds(Class<T> clazz, final SelectColumn selects, Serializable... pks) {
        return findsAsync(clazz, selects, pks).join();
    }

    @Override
    public <T> CompletableFuture<T[]> findsAsync(Class<T> clazz, final SelectColumn selects, Serializable... pks) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        if (pks == null || pks.length == 0) {
            return CompletableFuture.completedFuture(info.getArrayer().apply(0));
        }
        final Attribute<T, Serializable> primary = info.getPrimary();
        return queryListAsync(info.getType(), selects, null, FilterNodes.in(primary.field(), pks))
                .thenApply(list -> {
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
    public <D extends Serializable, T> CompletableFuture<List<T>> findsListAsync(
            final Class<T> clazz, final Stream<D> pks) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        Serializable[] ids = pks.toArray(serialArrayFunc);
        return queryListAsync(
                info.getType(), null, null, FilterNodes.in(info.getPrimary().field(), ids));
    }

    @Override
    public <T> Serializable findColumn(Class<T> clazz, String column, Serializable pk) {
        return findColumnAsync(clazz, column, (Serializable) null, pk).join();
    }

    @Override
    public <T> CompletableFuture<Serializable> findColumnAsync(Class<T> clazz, String column, Serializable pk) {
        return findColumnAsync(clazz, column, (Serializable) null, pk);
    }

    @Override
    public <T> Serializable findColumn(Class<T> clazz, String column, Serializable defValue, Serializable pk) {
        return findColumnAsync(clazz, column, defValue, pk).join();
    }

    @Override
    public <T> CompletableFuture<Serializable> findColumnAsync(
            Class<T> clazz, String column, Serializable defValue, Serializable pk) {
        return findAsync(clazz, SelectColumn.includes(column), pk).thenApply(v -> {
            if (v == null) {
                return defValue;
            }
            final SearchInfo<T> info = loadSearchInfo(clazz);
            Serializable s = info.getAttribute(column).get(v);
            return s == null ? defValue : s;
        });
    }

    @Override
    public <T> Serializable findColumn(Class<T> clazz, String column, Serializable defValue, FilterNode node) {
        return findColumnAsync(clazz, column, defValue, node).join();
    }

    @Override
    public <T> CompletableFuture<Serializable> findColumnAsync(
            Class<T> clazz, String column, Serializable defValue, FilterNode node) {
        return findAsync(clazz, SelectColumn.includes(column), node).thenApply(v -> {
            if (v == null) {
                return defValue;
            }
            final SearchInfo<T> info = loadSearchInfo(clazz);
            Serializable s = info.getAttribute(column).get(v);
            return s == null ? defValue : s;
        });
    }

    @Override
    public <T> boolean exists(Class<T> clazz, Serializable pk) {
        return existsAsync(clazz, pk).join();
    }

    @Override
    public <T> CompletableFuture<Boolean> existsAsync(Class<T> clazz, Serializable pk) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(info.getTable(pk))
                .append("/_doc/")
                .append(pk)
                .append("?_source=false");
        return getAsync(path).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return false;
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException(
                        "exists response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                FindResult<T> rs = JsonConvert.root().convertFrom(info.getFindResultType(), resp.getResult());
                return rs != null && rs.found;
            }
        });
    }

    @Override
    public <T> boolean exists(Class<T> clazz, FilterNode node) {
        return existsAsync(clazz, node).join();
    }

    @Override
    public <T> CompletableFuture<Boolean> existsAsync(Class<T> clazz, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path =
                new StringBuilder().append('/').append(info.getTable(node)).append("/_search?_source=false");
        return postAsync(path, info, createSearchRequest(info, null, null, node))
                .thenApply(resp -> {
                    if (resp.getRetcode() == 404) {
                        return false;
                    }
                    if (resp.getRetcode() != 200) {
                        throw new SourceException(
                                "exists response code = " + resp.getRetcode() + ", body = " + resp.getResult());
                    } else {
                        SearchResult<T> rs =
                                JsonConvert.root().convertFrom(info.getSearchResultType(), resp.getResult());
                        if (rs == null || rs.timed_out || rs.hits == null) {
                            return false;
                        }
                        return rs.hits.hits != null && rs.hits.hits.length == 1 && rs.hits.hits[0]._id != null;
                    }
                });
    }

    @Override
    public <T, V extends Serializable> Set<V> queryColumnSet(
            String selectedColumn, Class<T> clazz, String column, Serializable colval) {
        return (Set) queryColumnSetAsync(selectedColumn, clazz, (Flipper) null, FilterNodes.create(column, colval))
                .join();
    }

    @Override
    public <T, V extends Serializable> CompletableFuture<Set<V>> queryColumnSetAsync(
            String selectedColumn, Class<T> clazz, String column, Serializable colval) {
        return queryColumnSetAsync(selectedColumn, clazz, (Flipper) null, FilterNodes.create(column, colval));
    }

    @Override
    public <T, V extends Serializable> Set<V> queryColumnSet(
            String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return (Set) queryColumnSetAsync(selectedColumn, clazz, flipper, node).join();
    }

    @Override
    public <T, V extends Serializable> CompletableFuture<Set<V>> queryColumnSetAsync(
            String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final Attribute<T, V> attr = (Attribute) info.getAttribute(selectedColumn);
        if (attr == null) {
            return CompletableFuture.failedFuture(
                    new RuntimeException("not found column " + selectedColumn + " in " + clazz));
        }
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(getQueryTable(info, node))
                .append("/_search?_source_includes=")
                .append(selectedColumn);
        SearchRequest bean = createSearchRequest(info, null, flipper, node);
        SearchRequest.QueryFilterItem aggs = new SearchRequest.QueryFilterItem();
        aggs.put("unique", Utility.ofMap("terms", Utility.ofMap("field", selectedColumn)));
        bean.aggs = aggs;
        bean.size = 0; // 不需要返回_source
        return postAsync(path, info, bean).thenApply(resp -> {
            if (resp.getRetcode() == 404) {
                return new HashSet();
            }
            if (resp.getRetcode() != 200) {
                throw new SourceException("find response code = " + resp.getRetcode() + ", body = " + resp.getResult());
            } else {
                SearchResult<T> rs = JsonConvert.root().convertFrom(info.getSearchResultType(), resp.getResult());
                if (rs == null || rs.timed_out || rs.aggregations == null) {
                    return new HashSet();
                }
                SearchResult.Aggregations aggrs = rs.aggregations.get("unique");
                if (aggrs == null) {
                    return new HashSet();
                }
                return aggrs.forEachCount(info.getConvert(), attr.genericType(), new HashSet());
            }
        });
    }

    @Override
    public <T, V extends Serializable> List<V> queryColumnList(
            String selectedColumn, Class<T> clazz, String column, Serializable colval) {
        return (List)
                queryColumnListAsync(selectedColumn, clazz, column, colval).join();
    }

    @Override
    public <T, V extends Serializable> CompletableFuture<List<V>> queryColumnListAsync(
            String selectedColumn, Class<T> clazz, String column, Serializable colval) {
        return queryColumnListAsync(selectedColumn, clazz, (Flipper) null, FilterNodes.create(column, colval));
    }

    @Override
    public <T, V extends Serializable> List<V> queryColumnList(
            String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return (List) queryColumnListAsync(selectedColumn, clazz, flipper, node).join();
    }

    @Override
    public <T, V extends Serializable> CompletableFuture<List<V>> queryColumnListAsync(
            String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final Attribute<T, V> attr = (Attribute) info.getAttribute(selectedColumn);
        if (attr == null) {
            return CompletableFuture.failedFuture(
                    new RuntimeException("not found column " + selectedColumn + " in " + clazz));
        }
        CompletableFuture<List<T>> future = queryListAsync(clazz, SelectColumn.includes(selectedColumn), flipper, node);
        return future.thenApply(v -> v.stream().map(t -> attr.get(t)).collect(Collectors.toList()));
    }

    @Override
    public <T, V extends Serializable> Sheet<V> queryColumnSheet(
            String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        return (Sheet)
                queryColumnSheetAsync(selectedColumn, clazz, flipper, node).join();
    }

    @Override
    public <T, V extends Serializable> CompletableFuture<Sheet<V>> queryColumnSheetAsync(
            String selectedColumn, Class<T> clazz, Flipper flipper, FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final Attribute<T, V> attr = (Attribute) info.getAttribute(selectedColumn);
        if (attr == null) {
            return CompletableFuture.failedFuture(
                    new RuntimeException("not found column " + selectedColumn + " in " + clazz));
        }
        CompletableFuture<Sheet<T>> future =
                querySheetAsync(clazz, SelectColumn.includes(selectedColumn), flipper, node);
        return future.thenApply(v -> v.isEmpty()
                ? (Sheet) v
                : new Sheet<>(v.getTotal(), v.stream().map(t -> attr.get(t)).collect(Collectors.toList())));
    }

    @Override
    public <K extends Serializable, T> Map<K, T> queryMap(Class<T> clazz, SelectColumn selects, Stream<K> keyStream) {
        return queryMapAsync(clazz, selects, keyStream).join();
    }

    @Override
    public <K extends Serializable, T> CompletableFuture<Map<K, T>> queryMapAsync(
            Class<T> clazz, SelectColumn selects, Stream<K> keyStream) {
        if (keyStream == null) {
            return CompletableFuture.completedFuture(new LinkedHashMap<>());
        }
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final ArrayList<K> pks = new ArrayList<>();
        keyStream.forEach(k -> pks.add(k));
        final Attribute<T, Serializable> primary = info.getPrimary();
        return queryListAsync(clazz, FilterNodes.create(primary.field(), pks)).thenApply((List<T> rs) -> {
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
    public <K extends Serializable, T> CompletableFuture<Map<K, T>> queryMapAsync(
            Class<T> clazz, SelectColumn selects, FilterNode node) {
        return queryListAsync(clazz, selects, node).thenApply((List<T> rs) -> {
            final SearchInfo<T> info = loadSearchInfo(clazz);
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
    public <T> Set<T> querySet(Class<T> clazz, Flipper flipper, String column, Serializable colval) {
        return querySetAsync(clazz, (SelectColumn) null, flipper, FilterNodes.create(column, colval))
                .join();
    }

    @Override
    public <T> CompletableFuture<Set<T>> querySetAsync(
            Class<T> clazz, Flipper flipper, String column, Serializable colval) {
        return querySetAsync(clazz, (SelectColumn) null, flipper, FilterNodes.create(column, colval));
    }

    @Override
    public <T> Set<T> querySet(Class<T> clazz, SelectColumn selects, Flipper flipper, FilterNode node) {
        return querySetAsync(clazz, selects, flipper, node).join();
    }

    @Override
    public <T> CompletableFuture<Set<T>> querySetAsync(
            Class<T> clazz, SelectColumn selects, Flipper flipper, FilterNode node) {
        return querySheetAsync(false, true, clazz, selects, flipper, node)
                .thenApply(s -> new LinkedHashSet(s.list(true)));
    }

    @Override
    public <T> List<T> queryList(Class<T> clazz, Flipper flipper, String column, Serializable colval) {
        return queryListAsync(clazz, (SelectColumn) null, flipper, FilterNodes.create(column, colval))
                .join();
    }

    @Override
    public <T> CompletableFuture<List<T>> queryListAsync(
            Class<T> clazz, Flipper flipper, String column, Serializable colval) {
        return queryListAsync(clazz, (SelectColumn) null, flipper, FilterNodes.create(column, colval));
    }

    @Override
    public <T> List<T> queryList(Class<T> clazz, SelectColumn selects, Flipper flipper, FilterNode node) {
        return queryListAsync(clazz, selects, flipper, node).join();
    }

    @Override
    public <T> CompletableFuture<List<T>> queryListAsync(
            Class<T> clazz, SelectColumn selects, Flipper flipper, FilterNode node) {
        return querySheetAsync(false, false, clazz, selects, flipper, node).thenApply(s -> s.list(true));
    }

    @Override
    public <T> Sheet<T> querySheet(Class<T> clazz, SelectColumn selects, Flipper flipper, FilterNode node) {
        return querySheetAsync(clazz, selects, flipper, node).join();
    }

    @Override
    public <T> CompletableFuture<Sheet<T>> querySheetAsync(
            Class<T> clazz, SelectColumn selects, Flipper flipper, FilterNode node) {
        return querySheetAsync(true, false, clazz, selects, flipper, node);
    }

    protected <T> CompletableFuture<Sheet<T>> querySheetAsync(
            boolean needtotal,
            boolean distinct,
            Class<T> clazz,
            SelectColumn selects,
            Flipper flipper,
            FilterNode node) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        final StringBuilder path = new StringBuilder()
                .append('/')
                .append(getQueryTable(info, node))
                .append("/_search");
        if (selects != null) {
            path.append("?_source")
                    .append(selects.isExcludable() ? "_excludes=" : "_includes=")
                    .append(Utility.joining(selects.getColumns(), ','));
        }
        return postAsync(path, info, createSearchRequest(info, selects, flipper, node))
                .thenApply(resp -> {
                    if (resp.getRetcode() == 404) {
                        return new Sheet();
                    }
                    if (resp.getRetcode() != 200) {
                        throw new SourceException(
                                "find response code = " + resp.getRetcode() + ", body = " + resp.getResult());
                    } else {
                        SearchResult<T> rs =
                                JsonConvert.root().convertFrom(info.getSearchResultType(), resp.getResult());
                        if (rs == null || rs.timed_out || rs.hits == null) {
                            return new Sheet();
                        }
                        HitResult<T> hits = rs.hits;
                        return new Sheet<T>(hits.total.value, hits.list(info));
                    }
                });
    }

    @Override
    public <T> int updateMapping(final Class<T> clazz, String table) {
        return updateMappingAsync(clazz, table).join();
    }

    /**
     *
     *
     * <blockquote>
     *
     * <pre>
     * {
     *   "settings": {
     *      "analysis": {
     *          "analyzer": {
     *              "html_ik_max_word": {
     *                  "type": "custom",
     *                  "tokenizer": "ik_max_word",
     *                  "char_filter": ["html_strip"]
     *              },
     *              "html_standard": {
     *                  "type": "custom",
     *                  "tokenizer": "standard",
     *                  "char_filter": ["html_strip"]
     *              }
     *          }
     *      }
     *   }
     * }
     * </pre>
     *
     * </blockquote>
     *
     * @param <T> T
     * @param clazz Class
     * @param table0 String
     * @return int
     */
    @Override
    public <T> CompletableFuture<Integer> updateMappingAsync(final Class<T> clazz, String table0) {
        final SearchInfo<T> info = loadSearchInfo(clazz);
        String table = table0 == null ? info.getOriginTable() : table0;
        final StringBuilder path = new StringBuilder().append('/').append(table).append("/_mapping");
        // 更新setting时，必须先关闭索引，再更新， 再打开索引。 更新setting是为了增加自定义的分词器
        return postAsync("/" + table + "/_close?wait_for_active_shards=0", (byte[]) null)
                .thenCompose(c -> {
                    CompletableFuture<RetResult<String>> updateFuture;
                    if (!info.getCustomAnalyzerMap().isEmpty()) {
                        updateFuture = putAsync(
                                        "/" + table + "/_settings",
                                        info,
                                        Utility.ofMap(
                                                "analysis", Utility.ofMap("analyzer", info.getCustomAnalyzerMap())))
                                .thenCompose(cv ->
                                        putAsync(path, info, Utility.ofMap("properties", info.getMappingTypes())));
                    } else {
                        updateFuture = putAsync(path, info, Utility.ofMap("properties", info.getMappingTypes()));
                    }
                    return updateFuture.thenCompose(resp -> {
                        if (resp.getRetcode() == 404) {
                            return postAsync("/" + table + "/_open", (byte[]) null)
                                    .thenApply(cv -> -1);
                        }
                        if (resp.getRetcode() != 200) {
                            return postAsync("/" + table + "/_open", (byte[]) null)
                                    .thenApply(cv -> {
                                        throw new SourceException("updateMapping response code = " + resp.getRetcode()
                                                + ", body = " + resp.getResult());
                                    });
                        } else { // {"acknowledged":true}
                            Map<String, String> rs = JsonConvert.root()
                                    .convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, resp.getResult());
                            int val = rs == null || !"true".equals(rs.get("acknowledged")) ? -1 : 1;
                            return postAsync("/" + table + "/_open", (byte[]) null)
                                    .thenApply(cv -> val);
                        }
                    });
                });
    }
}
