/*
 *
 */
package org.redkalex.source.parser;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.Attribute;
import org.redkale.util.Utility;

/** @author zhangjx */
@SuppressWarnings("unchecked")
public class NativeSqlParameter {

    private static final ConcurrentHashMap<String, Attribute> attrCache = new ConcurrentHashMap<>();

    // #{xx.xx}、##{xx.xx}参数名
    private final String numsignName;

    // jdbc参数名 :xx_xx
    private final String jdbcName;

    // #{xxx}参数名按.分隔
    private final String[] numsigns;

    // 默认值
    private Object defaultVal;

    // 是否必需
    boolean required;

    public NativeSqlParameter(String numsignName, String jdbcName, boolean required, String defaultValStr) {
        this.numsignName = numsignName;
        this.jdbcName = jdbcName;
        this.required = required;
        this.numsigns = numsignName.split("\\.");
        this.defaultVal = parseDefaultVal(defaultValStr);
    }

    private Object parseDefaultVal(final String defaultValStr) {
        if (defaultValStr == null) {
            return null;
        }
        String lowerStr = defaultValStr.toLowerCase();
        if (lowerStr.startsWith("(int)")) {
            return Integer.parseInt(lowerStr.substring("(int)".length()).trim());
        } else if (lowerStr.startsWith("(integer)")) {
            return Integer.parseInt(lowerStr.substring("(integer)".length()).trim());
        } else if (lowerStr.startsWith("(long)")) {
            return Long.parseLong(lowerStr.substring("(long)".length()).trim());
        } else if (lowerStr.startsWith("(short)")) {
            return Short.parseShort(lowerStr.substring("(short)".length()).trim());
        } else if (lowerStr.startsWith("(float)")) {
            return Float.parseFloat(lowerStr.substring("(float)".length()).trim());
        } else if (lowerStr.startsWith("(double)")) {
            return Double.parseDouble(lowerStr.substring("(double)".length()).trim());
        } else if (lowerStr.startsWith("(string)")) {
            return defaultValStr.substring("(string)".length()).trim();
        } else {
            return defaultValStr;
        }
    }

    public Object getParamValue(Map<String, Object> params) {
        if (Utility.isEmpty(params)) {
            return defaultVal;
        }
        String[] subs = numsigns;
        Object val = params.get(subs[0]);
        if (val == null) {
            return defaultVal;
        }
        if (subs.length == 1) {
            return val;
        }
        for (int i = 1; i < subs.length; i++) {
            String fieldName = subs[i];
            Class clz = val.getClass();
            Attribute attr =
                    attrCache.computeIfAbsent(clz.getName() + ":" + fieldName, k -> Attribute.create(clz, fieldName));
            val = attr.get(val);
            if (val == null) {
                return defaultVal;
            }
        }
        return val;
    }

    NativeSqlParameter require(boolean required) {
        this.required = required;
        return this;
    }

    public String getNumsignName() {
        return numsignName;
    }

    public String getJdbcName() {
        return jdbcName;
    }

    public boolean isRequired() {
        return required;
    }

    public String[] getNumsigns() {
        return numsigns;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
