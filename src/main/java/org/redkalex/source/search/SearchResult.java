/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.lang.reflect.Type;
import java.util.*;
import org.redkale.convert.json.JsonConvert;

/**
 * @author zhangjx
 * @param <T> T
 */
public class SearchResult<T> extends BaseBean {

	public int took; // 操作耗时

	public boolean timed_out;

	public ShardResult _shards;

	public HitResult<T> hits;

	public Map<String, Aggregations> aggregations;

	public static class Aggregations extends BaseBean {

		public int doc_count_error_upper_bound;

		public int sum_other_doc_count;

		public BucketItem[] buckets;

		public double value; // 等同func_count.value  简化方式

		public <T extends Collection> T forEachCount(JsonConvert convert, Type itemType, T c) {
			if (buckets == null) return c;
			for (BucketItem item : buckets) {
				if (item == null || item.key == null) continue;
				c.add(itemType == String.class ? item.key : convert.convertFrom(itemType, item.key));
			}
			return c;
		}
	}

	public static class BucketItem extends BaseBean {

		public String key;

		public int doc_count;

		public BucketItemCount count;

		public BucketItemFuncCount func_count;

		public double funcCount() {
			return func_count == null ? 0.0 : func_count.value;
		}
	}

	public static class BucketItemCount extends BaseBean {

		public int value;
	}

	public static class BucketItemFuncCount extends BaseBean {

		public double value;
	}
}
