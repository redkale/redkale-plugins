/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import java.io.Serializable;
import java.util.*;
import org.redkale.convert.ConvertColumn;
import org.redkale.convert.json.JsonConvert;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 * 查询条过滤件
 *
 * @author zhangjx
 */
public class SearchRequest extends BaseBean {

	private static final StringWrapper WRAPPER_EMPTY = new StringWrapper("{}");

	@ConvertColumn(index = 1)
	public Query query; // 查询条件

	@ConvertColumn(index = 2)
	public QueryFilterItem highlight;

	@ConvertColumn(index = 3)
	public HashMap<String, Object> source; // 更新内容

	@ConvertColumn(index = 4)
	public Object script;

	@ConvertColumn(index = 5)
	public List<Map<String, SortOrder>> sort;

	@ConvertColumn(index = 6)
	public QueryFilterItem aggs;

	@ConvertColumn(index = 7)
	public Integer from; // 翻页偏移量

	@ConvertColumn(index = 8)
	public Integer size; // 翻页条数

	public static SearchRequest createMatchAll() {
		SearchRequest bean = new SearchRequest();
		bean.query = new Query();
		bean.query.match_all = Collections.EMPTY_MAP;
		return bean;
	}

	private static QueryFilterItem filterNodeElement(FilterNode node) {
		if (node == null || node.getColumn() == null || node.getColumn().charAt(0) == '#') return null;
		if (node instanceof FilterJoinNode)
			throw new IllegalArgumentException("Not supported " + FilterJoinNode.class.getSimpleName());
		switch (node.getExpress()) {
			case EQ:
			case IG_EQ: {
				return new QueryFilterItem("term", new QueryFilterItem(node.getColumn(), node.getValue()));
			}
			case NE:
			case IG_NE: {
				return new QueryFilterItem(
						"bool",
						new QueryFilterItem(
								"must_not",
								new QueryFilterItem("term", new QueryFilterItem(node.getColumn(), node.getValue()))));
			}
			case GT: {
				return new QueryFilterItem(
						"range", new QueryFilterItem(node.getColumn(), Utility.ofMap("gt", node.getValue())));
			}
			case LT: {
				return new QueryFilterItem(
						"range", new QueryFilterItem(node.getColumn(), Utility.ofMap("lt", node.getValue())));
			}
			case GE: {
				return new QueryFilterItem(
						"range", new QueryFilterItem(node.getColumn(), Utility.ofMap("gte", node.getValue())));
			}
			case LE: {
				return new QueryFilterItem(
						"range", new QueryFilterItem(node.getColumn(), Utility.ofMap("lte", node.getValue())));
			}
			case LIKE: {
				return new QueryFilterItem("match_phrase", new QueryFilterItem(node.getColumn(), node.getValue()));
			}
			case NOT_LIKE: {
				return new QueryFilterItem(
						"bool",
						new QueryFilterItem(
								"must_not",
								new QueryFilterItem(
										"match_phrase", new QueryFilterItem(node.getColumn(), node.getValue()))));
			}
			case IN: {
				return new QueryFilterItem("terms", new QueryFilterItem(node.getColumn(), node.getValue()));
			}
			case NOT_IN: {
				return new QueryFilterItem(
						"bool",
						new QueryFilterItem(
								"must_not",
								new QueryFilterItem("terms", new QueryFilterItem(node.getColumn(), node.getValue()))));
			}
			case BETWEEN: {
				Range range = (Range) node.getValue();
				LinkedHashMap rangeval = new LinkedHashMap();
				rangeval.put("gte", range.getMin());
				if (range.getMax() != null && range.getMax().compareTo(range.getMin()) > 0) {
					rangeval.put("lte", range.getMax());
				}
				return new QueryFilterItem("range", new QueryFilterItem(node.getColumn(), rangeval));
			}
			case NOT_BETWEEN: {
				Range range = (Range) node.getValue();
				LinkedHashMap rangeval = new LinkedHashMap();
				rangeval.put("gte", range.getMin());
				if (range.getMax() != null && range.getMax().compareTo(range.getMin()) > 0) {
					rangeval.put("lte", range.getMax());
				}
				return new QueryFilterItem(
						"bool",
						new QueryFilterItem(
								"must_not",
								new QueryFilterItem("range", new QueryFilterItem(node.getColumn(), rangeval))));
			}
			default:
				throw new IllegalArgumentException("Not supported FilterNode " + node);
		}
	}

	private static QueryFilterItem filterNodeBool(FilterNode node, boolean sub) {
		if (node == null) return null;
		QueryFilterItem element = filterNodeElement(node);
		if (element == null && node.getNodes() == null) return null;
		QueryFilterItem bool = new QueryFilterItem();
		List<QueryFilterItem> items = new ArrayList<>();
		if (element != null) items.add(element);
		if (node.getNodes() != null) {
			for (FilterNode item : node.getNodes()) {
				QueryFilterItem s = filterNodeBool(item, true);
				if (s == null) continue;
				if (s.containsKey("must")
						|| s.containsKey("should")
						|| s.containsKey("match")
						|| s.containsKey("match_phrase")
						|| s.containsKey("multi_match")) {
					items.add(new QueryFilterItem("bool", s));
				} else {
					items.add(s);
				}
			}
		}
		if (items.isEmpty()) return null;
		if (sub && items.size() == 1) return items.get(0);
		if (node.isOr()) {
			bool.put("should", items);
		} else {
			bool.put("must", items);
		}
		return bool;
	}

	public SearchRequest filterNode(SearchInfo info, FilterNode node) {
		if (node == null) return this;
		this.query = new Query();
		this.query.bool = filterNodeBool(node, false);
		SearchQuery search = (SearchQuery) node.findValue(SearchQuery.SEARCH_FILTER_NAME);
		if (search != null
				&& search.searchKeyword() != null
				&& !search.searchKeyword().isEmpty()) {
			String[] fs = search.searchFields();
			QueryFilterItem match = new QueryFilterItem("query", search.searchKeyword(), "fields", fs);
			if (search.searchAnalyzer() != null && !search.searchAnalyzer().isEmpty()) {
				match.put("analyzer", search.searchAnalyzer());
			}
			Map<String, Object> extras = search.extras();
			if (extras != null) match.putAll(extras);
			if (this.query.bool == null) {
				this.query.multi_match = match;
			} else {
				List<QueryFilterItem> must = (List) this.query.bool.get("must");
				if (must == null) {
					must = new ArrayList<>();
					this.query.bool.put("must", must);
				}
				must.add(new QueryFilterItem("multi_match", match));
			}
			if (search.highlight() != null) {
				SearchQuery.SearchHighlight hlbean = search.highlight();
				QueryFilterItem hl = new QueryFilterItem();
				if (hlbean.preTag() != null && !hlbean.preTag().isEmpty()) {
					hl.put("pre_tags", new String[] {hlbean.preTag()});
					hl.put("post_tags", new String[] {hlbean.postTag()});
				}
				if (hlbean.boundaryLocale() != null && !hlbean.boundaryLocale().isEmpty()) {
					hl.put("boundary_scanner_locale", hlbean.boundaryLocale());
				}
				if (hlbean.fragmentSize() > 0) hl.put("fragment_size", hlbean.fragmentSize());
				if (hlbean.fragmentCount() >= 0) hl.put("number_of_fragments", hlbean.fragmentCount());
				Map<String, Object> hlextras = hlbean.extras();
				if (hlextras != null) hl.putAll(hlextras);
				QueryFilterItem fields = new QueryFilterItem();
				for (String field : fs) {
					fields.put(field, WRAPPER_EMPTY);
				}
				hl.put("fields", fields);
				this.highlight = hl;
			}
		}
		return this;
	}

	public SearchRequest flipper(Flipper flipper) {
		if (flipper == null) return this;
		if (flipper.getOffset() > 0) this.from = flipper.getOffset();
		if (flipper.getLimit() > 0) this.size = flipper.getLimit();
		if (flipper.getSort() != null && !flipper.getSort().isEmpty()) {
			List<Map<String, SortOrder>> list = new ArrayList<>();
			for (String item : flipper.getSort().split(",")) {
				if (item.isEmpty()) continue;
				String[] sub = item.split("\\s+");
				if (sub.length < 2 || sub[1].equalsIgnoreCase("ASC")) {
					list.add(Utility.ofMap(sub[0], new SortOrder("asc")));
				} else {
					list.add(Utility.ofMap(sub[0], new SortOrder("desc")));
				}
			}
			if (!list.isEmpty()) this.sort = list;
		}
		return this;
	}

	public static class SortOrder extends BaseBean {

		public String order;

		public SortOrder() {}

		public SortOrder(String o) {
			this.order = o;
		}
	}

	public static class Query extends BaseBean {

		@ConvertColumn(index = 1)
		public QueryFilterItem bool;

		@ConvertColumn(index = 2)
		public Object match_all;

		@ConvertColumn(index = 3)
		public QueryIds ids;

		@ConvertColumn(index = 4)
		public QueryFilterItem term;

		@ConvertColumn(index = 5)
		public QueryFilterItem multi_match;
	}

	public static class QueryBool extends BaseBean {

		@ConvertColumn(index = 1)
		public List<QueryFilterItem> must;

		@ConvertColumn(index = 2)
		public List<QueryFilterItem> should;

		@ConvertColumn(index = 3)
		public List<QueryFilterItem> must_not;

		public List<QueryFilterItem> must() {
			if (this.must == null) this.must = new ArrayList<>();
			return this.must;
		}

		public List<QueryFilterItem> must_not() {
			if (this.must_not == null) this.must_not = new ArrayList<>();
			return this.must_not;
		}

		public List<QueryFilterItem> should() {
			if (this.should == null) this.should = new ArrayList<>();
			return this.should;
		}
	}

	public static class QueryFilterItem extends LinkedHashMap<String, Object> {

		public QueryFilterItem() {}

		public QueryFilterItem(String key, Serializable val) {
			put(key, val);
		}

		public QueryFilterItem(Object... items) {
			putAll(Utility.ofMap(items));
		}

		@Override
		public String toString() {
			return JsonConvert.root().convertTo(this);
		}
	}

	public static class QueryIds extends BaseBean {

		public String[] values;

		public QueryIds() {}

		public QueryIds(String... ids) {
			this.values = ids;
		}
	}
}
