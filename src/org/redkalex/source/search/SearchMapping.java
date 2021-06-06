/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.lang.reflect.Type;
import java.util.Map;
import org.redkale.util.TypeToken;

/**
 *
 * @author zhangjx
 */
public class SearchMapping extends BaseBean {

    public static final Type MAPPING_MAP_TYPE = new TypeToken<Map<String, SearchMapping>>() {
    }.getType();

    public SearchProperties mappings;

    public static class SearchProperties extends BaseBean {

        public Map<String, MappingItem> properties;

        public boolean equal(Map<String, Map> ms) {
            if (properties == null) return false;
            if (ms == null) return false;
            if (properties.size() != ms.size()) return false;
            for (Map.Entry<String, MappingItem> en : properties.entrySet()) {
                Map infomap = ms.get(en.getKey());
                if (infomap == null) return false;
                Object infoanalyzer = infomap.get("analyzer");
                if (en.getValue().analyzer == null && infoanalyzer != null) return false;
                if (en.getValue().analyzer != null && infoanalyzer == null) return false;
                if (en.getValue().analyzer != null && !en.getValue().analyzer.equals(infoanalyzer)) return false;
            }
            return true;
        }
    }

    public static class MappingItem extends BaseBean {

        public String type;

        public String analyzer;

        public Integer ignore_above;

    }

}
