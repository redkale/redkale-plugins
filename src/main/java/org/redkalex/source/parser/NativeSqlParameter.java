/*
 *
 */
package org.redkalex.source.parser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.Attribute;

/**
 *
 * @author zhangjx
 */
@SuppressWarnings("unchecked")
public class NativeSqlParameter {

    private static final ConcurrentHashMap<String, Attribute> attrCache = new ConcurrentHashMap<>();

    //${xxx}、$${xxx}参数名
    private final String dollarName;

    //jdbc参数名 :argxxx
    private final String jdbcName;

    //是否必需 
    private final boolean required;

    //${xxx}参数名按.分隔
    private final String[] dollars;

    public NativeSqlParameter(String dollarName, String jdbcName, boolean required) {
        this.dollarName = dollarName;
        this.jdbcName = jdbcName;
        this.required = required;
        this.dollars = dollarName.split("\\.");
    }

    public Object getParamValue(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }
        String[] subs = dollars;
        Object val = params.get(subs[0]);
        if (val == null || subs.length == 1) {
            return val;
        }
        for (int i = 1; i < subs.length; i++) {
            String fieldName = subs[i];
            Class clz = val.getClass();
            Attribute attr = attrCache.computeIfAbsent(clz.getName() + ":" + fieldName, k -> Attribute.create(clz, fieldName));
            val = attr.get(val);
            if (val == null) {
                return val;
            }
        }
        return val;
    }

    public String getDollarName() {
        return dollarName;
    }

    public String getJdbcName() {
        return jdbcName;
    }

    public boolean isRequired() {
        return required;
    }

    public String[] getDollars() {
        return dollars;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

}
