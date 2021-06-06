/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.util.*;
import org.redkale.util.Attribute;

/**
 *
 * @author zhangjx
 * @param <T> T
 */
public class HitResult<T> extends BaseBean {

    public HitTotal total;

    public Float max_score;  //返回值会有null

    public HitEntity<T>[] hits;

    public List<T> list(final SearchInfo<T> info) {
        List<T> vals = new ArrayList();
        if (hits == null || hits.length < 1) return vals;
        Attribute<T, String> highlightAttrId = info.getHighlightAttributeId();
        Attribute<T, String> highlightAttrIndex = info.getHighlightAttributeIndex();
        for (HitEntity<T> item : hits) {
            T obj = item._source;
            vals.add(obj);
            if (highlightAttrId != null) highlightAttrId.set(obj, item._id);
            if (highlightAttrIndex != null) highlightAttrIndex.set(obj, item._index);
            if (obj != null && item.highlight != null) {
                item.highlight.forEach((t, v) -> {
                    Attribute<T, String> attr = info.getHighlightAttribute(t);
                    if (attr != null) attr.set(obj, v[0]);
                });
            }
        }
        return vals;
    }

    public static class HitTotal extends BaseBean {

        public int value;

        public String relation;

    }
}
