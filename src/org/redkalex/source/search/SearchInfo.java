/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import javax.persistence.*;
import org.redkale.convert.json.*;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 * Search Entity操作类
 *
 * <p>
 * 详情见: https://redkale.org
 *
 * @since 2.4.0
 *
 * @author zhangjx
 * @param <T> Search Entity类的泛型
 */
@SuppressWarnings("unchecked")
public final class SearchInfo<T> {

    //全局静态资源
    private static final ConcurrentHashMap<Class, SearchInfo> entityInfos = new ConcurrentHashMap<>();

    //日志
    private static final Logger logger = Logger.getLogger(SearchInfo.class.getSimpleName());

    //Entity类名
    private final Class<T> type;

    //类对应的数据表名, 如果是VirtualEntity 类， 则该字段为null
    private final String table;

    //JsonConvert
    private final JsonConvert jsonConvert;

    //Entity构建器
    private final Creator<T> creator;

    //Entity构建器参数
    private final String[] constructorParameters;

    //Entity构建器参数Attribute
    private final Attribute<T, Serializable>[] constructorAttributes;

    //Entity构建器参数Attribute
    private final Attribute<T, Serializable>[] unconstructorAttributes;

    //主键
    private final Attribute<T, Serializable> primary;

    //用于存储绑定在EntityInfo上的对象
    private final ConcurrentHashMap<String, Object> subobjectMap = new ConcurrentHashMap<>();

    //key是field的name， 不是sql字段。
    //存放所有与数据库对应的字段， 包括主键
    private final HashMap<String, Attribute<T, Serializable>> attributeMap = new HashMap<>();

    //存放所有与数据库对应的字段， 包括主键
    private final Attribute<T, Serializable>[] attributes;

    //key是field的name， value是Column的别名，即数据库表的字段名
    //只有field.name 与 Column.name不同才存放在aliasmap里.
    private final Map<String, String> aliasmap;

    //所有可更新字段，即排除了主键字段和标记为&#064;Column(updatable=false)的字段
    private final Map<String, Attribute<T, Serializable>> updateAttributeMap = new HashMap<>();

    //数据库中所有字段
    private final Attribute<T, Serializable>[] queryAttributes;

    //数据库中所有可新增字段
    private final Attribute<T, Serializable>[] insertAttributes;

    //数据库中所有可更新字段
    private final Attribute<T, Serializable>[] updateAttributes;

    //存放highlight虚拟主键字段
    private final Attribute<T, String> highlightAttributeId;

    //存放highlight虚拟索引字段
    private final Attribute<T, String> highlightAttributeIndex;

    //存放highlight虚拟字段
    private final HashMap<String, Attribute<T, String>> highlightAttributeMap = new HashMap<>();

    //存放html定制的analyzer
    private final HashMap<String, Map> customAnalyzerMap = new HashMap<>();

    private final Map<String, Map> mappingTypes;

    //日志级别，从LogLevel获取
    private final int logLevel;

    private final boolean virtual;

    //日志控制
    private final Map<Integer, String[]> excludeLogLevels;

    private final Type findResultType;

    private final Type searchResultType;

    public static <T> SearchInfo<T> load(Class<T> clazz) {
        return load(clazz, null);
    }

    public static <T> SearchInfo<T> load(Class<T> clazz, final Properties conf) {
        SearchInfo rs = entityInfos.get(clazz);
        if (rs != null) return rs;
        synchronized (entityInfos) {
            rs = entityInfos.get(clazz);
            if (rs == null) {
                rs = new SearchInfo(clazz, conf);
                entityInfos.put(clazz, rs);
            }
            return rs;
        }
    }

    /**
     * 构造函数
     *
     * @param type Entity类
     * @param conf 配置信息
     */
    private SearchInfo(Class<T> type, Properties conf) {
        this.type = type;
        this.virtual = type.getAnnotation(VirtualEntity.class) != null;
        this.findResultType = TypeToken.createParameterizedType(null, FindResult.class, type);
        this.searchResultType = TypeToken.createParameterizedType(null, SearchResult.class, type);
        //---------------------------------------------
        LogLevel ll = type.getAnnotation(LogLevel.class);
        this.logLevel = ll == null ? Integer.MIN_VALUE : Level.parse(ll.value()).intValue();
        Map<Integer, HashSet<String>> logmap = new HashMap<>();
        for (LogExcludeLevel lel : type.getAnnotationsByType(LogExcludeLevel.class)) {
            for (String onelevel : lel.levels()) {
                int level = Level.parse(onelevel).intValue();
                HashSet<String> set = logmap.get(level);
                if (set == null) {
                    set = new HashSet<>();
                    logmap.put(level, set);
                }
                for (String key : lel.keys()) {
                    set.add(key);
                }
            }
        }
        if (logmap.isEmpty()) {
            this.excludeLogLevels = null;
        } else {
            this.excludeLogLevels = new HashMap<>();
            logmap.forEach((l, set) -> excludeLogLevels.put(l, set.toArray(new String[set.size()])));
        }
        //---------------------------------------------
        Table t = type.getAnnotation(Table.class);
        this.table = (t == null || t.name().isEmpty()) ? type.getSimpleName().toLowerCase() : t.name();

        this.creator = Creator.create(type);
        ConstructorParameters cp = null;
        try {
            cp = this.creator.getClass().getMethod("create", Object[].class).getAnnotation(ConstructorParameters.class);
        } catch (Exception e) {
            logger.log(Level.SEVERE, type + " cannot find ConstructorParameters Creator", e);
        }
        this.constructorParameters = (cp == null || cp.value().length < 1) ? null : cp.value();
        Attribute idAttr0 = null;
        Map<String, String> aliasmap0 = null;
        Class cltmp = type;
        Set<String> fields = new HashSet<>();
        List<Attribute<T, Serializable>> queryattrs = new ArrayList<>();
        List<String> insertcols = new ArrayList<>();
        List<Attribute<T, Serializable>> insertattrs = new ArrayList<>();
        List<String> updatecols = new ArrayList<>();
        List<Attribute<T, Serializable>> updateattrs = new ArrayList<>();
        Map<String, Map> mappings = new LinkedHashMap<>();
        JsonFactory factory = JsonFactory.root();
        Attribute<T, String> highlightAttrId = null;
        Attribute<T, String> highlightAttrIndex = null;
        do {
            for (Field field : cltmp.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) continue;
                if (Modifier.isFinal(field.getModifiers())) continue;
                if (field.getAnnotation(Transient.class) != null) continue;
                if (fields.contains(field.getName())) continue;
                final String fieldname = field.getName();
                final Column col = field.getAnnotation(Column.class);
                int strlen = col == null || col.length() < 1 ? 255 : col.length();
                final String sqlfield = col == null || col.name().isEmpty() ? fieldname : col.name();
                if (!fieldname.equals(sqlfield)) {
                    if (aliasmap0 == null) aliasmap0 = new HashMap<>();
                    aliasmap0.put(fieldname, sqlfield);
                }
                Attribute attr;
                try {
                    attr = Attribute.create(type, cltmp, field);
                } catch (RuntimeException e) {
                    continue;
                }
                boolean text = false;
                SearchColumn sc = field.getAnnotation(SearchColumn.class);
                if (sc != null) {
                    if (!sc.highlight().isEmpty()) {
                        if (!sc.ignore()) throw new RuntimeException("@SearchColumn.ignore must be true when highlight is not empty on field(" + field + ")");
                        if (field.getType() != String.class) throw new RuntimeException("@SearchColumn.ignore must be on String field(" + field + ")");
                        if (SearchColumn.HighLights.HIGHLIGHT_NAME_ID.equals(sc.highlight())) {
                            highlightAttrId = attr;
                        } else if (SearchColumn.HighLights.HIGHLIGHT_NAME_INDEX.equals(sc.highlight())) {
                            highlightAttrIndex = attr;
                        } else {
                            highlightAttributeMap.put(sc.highlight(), attr);
                        }
                    }
                    text = sc.text();
                    if (sc.ignore()) {
                        if (factory == JsonFactory.root()) factory = JsonFactory.create();
                        factory.register(type, true, fieldname);
                        continue;
                    }
                }
                if (field.getAnnotation(javax.persistence.Id.class) != null && idAttr0 == null) {
                    idAttr0 = attr;
                    insertcols.add(sqlfield);
                    insertattrs.add(attr);
                } else {
                    if (col == null || col.insertable()) {
                        insertcols.add(sqlfield);
                        insertattrs.add(attr);
                    }
                    if (col == null || col.updatable()) {
                        updatecols.add(sqlfield);
                        updateattrs.add(attr);
                        updateAttributeMap.put(fieldname, attr);
                    }
                }
                if (attr.type() == boolean.class || attr.type() == Boolean.class) {
                    mappings.put(attr.field(), Utility.ofMap("type", "boolean"));
                } else if (attr.type() == float.class || attr.type() == Float.class) {
                    mappings.put(attr.field(), Utility.ofMap("type", "double"));
                } else if (attr.type() == double.class || attr.type() == Double.class) {
                    mappings.put(attr.field(), Utility.ofMap("type", "double"));
                } else if (attr.type().isPrimitive() || Number.class.isAssignableFrom(attr.type())) {
                    mappings.put(attr.field(), Utility.ofMap("type", "long"));
                } else if (CharSequence.class.isAssignableFrom(attr.type())) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    if (sc != null && !sc.options().isEmpty()) {
                        m.put("index_options", sc.options());
                    }
                    if (sc != null && (sc.html() || !sc.analyzer().isEmpty())) {
                        String analyzer = (sc.html() ? "html_" : "") + sc.analyzer();
                        if ("html_".equals(analyzer)) analyzer = "html_standard";
                        if (analyzer.startsWith("html_")) {
                            customAnalyzerMap.put(analyzer, Utility.ofMap("type", "custom",
                                "tokenizer", analyzer.replace("html_", ""),
                                "char_filter", new String[]{"html_strip"}));
                        }
                        m.put("analyzer", analyzer);
                    }
                    if (sc != null && (sc.html() || !sc.searchAnalyzer().isEmpty())) {
                        String searchAnalyzer = (sc.html() ? "html_" : "") + sc.searchAnalyzer();
                        if ("html_".equals(searchAnalyzer)) searchAnalyzer = "html_standard";
                        if (searchAnalyzer.startsWith("html_")) {
                            customAnalyzerMap.put(searchAnalyzer, Utility.ofMap("type", "custom",
                                "tokenizer", searchAnalyzer.replace("html_", ""),
                                "char_filter", new String[]{"html_strip"}));
                        }
                        m.put("search_analyzer", searchAnalyzer);
                    }
                    if (text) {
                        m.put("type", "text");
                    } else {
                        m.put("type", "keyword");
                        m.put("ignore_above", strlen);
                    }
                    mappings.put(attr.field(), m);
                } else {
                    mappings.put(attr.field(), Utility.ofMap("type", "object"));
                }
                queryattrs.add(attr);
                fields.add(fieldname);
                attributeMap.put(fieldname, attr);
            }
        } while ((cltmp = cltmp.getSuperclass()) != Object.class);
        if (idAttr0 == null) throw new RuntimeException(type.getName() + " have no primary column by @javax.persistence.Id");
        this.jsonConvert = factory.getConvert();

        this.primary = idAttr0;
        this.aliasmap = aliasmap0;
        this.highlightAttributeId = highlightAttrId;
        this.highlightAttributeIndex = highlightAttrIndex;
        this.attributes = attributeMap.values().toArray(new Attribute[attributeMap.size()]);
        this.queryAttributes = queryattrs.toArray(new Attribute[queryattrs.size()]);
        this.insertAttributes = insertattrs.toArray(new Attribute[insertattrs.size()]);
        this.updateAttributes = updateattrs.toArray(new Attribute[updateattrs.size()]);
        this.mappingTypes = mappings;
        if (this.constructorParameters == null) {
            this.constructorAttributes = null;
            this.unconstructorAttributes = null;
        } else {
            this.constructorAttributes = new Attribute[this.constructorParameters.length];
            List<Attribute<T, Serializable>> unconstructorAttrs = new ArrayList<>();
            for (Attribute<T, Serializable> attr : queryAttributes) {
                int pos = -1;
                for (int i = 0; i < this.constructorParameters.length; i++) {
                    if (attr.field().equals(this.constructorParameters[i])) {
                        pos = i;
                        break;
                    }
                }
                if (pos >= 0) {
                    this.constructorAttributes[pos] = attr;
                } else {
                    unconstructorAttrs.add(attr);
                }
            }
            this.unconstructorAttributes = unconstructorAttrs.toArray(new Attribute[unconstructorAttrs.size()]);
        }
    }

    /**
     * 根据主键值获取Entity的表名
     *
     *
     * @return String
     */
    public String getTable() {
        return table;
    }

    public Class<T> getType() {
        return type;
    }

    /**
     * 获取主键字段的Attribute
     *
     * @return Attribute
     */
    public Attribute<T, Serializable> getPrimary() {
        return this.primary;
    }

    /**
     * 根据Entity字段名获取字段的Attribute
     *
     * @param fieldname Class字段名
     *
     * @return Attribute
     */
    public Attribute<T, Serializable> getAttribute(String fieldname) {
        if (fieldname == null) return null;
        return this.attributeMap.get(fieldname);
    }

    /**
     * 根据Entity字段名获取可更新字段的Attribute
     *
     * @param fieldname Class字段名
     *
     * @return Attribute
     */
    public Attribute<T, Serializable> getUpdateAttribute(String fieldname) {
        return this.updateAttributeMap.get(fieldname);
    }

    public Type getFindResultType() {
        return findResultType;
    }

    public Type getSearchResultType() {
        return searchResultType;
    }

    public Attribute<T, Serializable>[] getAttributes() {
        return attributes;
    }

    public Attribute<T, Serializable>[] getUpdateAttributes() {
        return updateAttributes;
    }

    public Map<String, Map> getMappingTypes() {
        return mappingTypes;
    }

    public Map<String, Map> getCustomAnalyzerMap() {
        return customAnalyzerMap;
    }

    public Map<String, Object> createIndexMap() {
        Map<String, Object> map = Utility.ofMap("mappings", Utility.ofMap("properties", getMappingTypes()));
        if (!customAnalyzerMap.isEmpty()) {
            map.put("settings", Utility.ofMap("analysis", Utility.ofMap("analyzer", customAnalyzerMap)));
        }
        return map;
    }

    public JsonConvert getConvert() {
        return jsonConvert;
    }

    /**
     * 获取主键字段的表字段名
     *
     * @return String
     */
    public String getPrimarySQLColumn() {
        return getSQLColumn(this.primary.field());
    }

    /**
     * 根据field字段名获取数据库对应的字段名
     *
     * @param fieldname 字段名
     *
     * @return String
     */
    public String getSQLColumn(String fieldname) {
        return this.aliasmap == null ? fieldname : aliasmap.getOrDefault(fieldname, fieldname);
    }

    public Attribute<T, String> getHighlightAttribute(String fieldname) {
        return this.highlightAttributeMap.get(fieldname);
    }

    public Attribute<T, String> getHighlightAttributeId() {
        return highlightAttributeId;
    }

    public Attribute<T, String> getHighlightAttributeIndex() {
        return highlightAttributeIndex;
    }

    public boolean isVirtual() {
        return virtual;
    }

}
