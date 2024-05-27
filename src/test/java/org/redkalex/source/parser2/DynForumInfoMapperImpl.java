/*
 *
 */
package org.redkalex.source.parser2;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.redkale.source.DataSqlSource;
import org.redkale.source.Flipper;
import org.redkale.util.Sheet;
import org.redkale.util.Utility;

/** @author zhangjx */
public class DynForumInfoMapperImpl implements ForumInfoMapper {

	private DataSqlSource _source;

	private Class _type;

	@Override
	public ForumResult findForumResultOne(ForumBean bean) {
		return dataSource()
				.nativeQueryOne(
						ForumResult.class,
						"SELECT f.forum_groupid, s.forum_section_color FROM forum_info f, forum_section s WHERE f.forumid = s.forumid",
						(Map) Utility.ofMap("bean", bean));
	}

	@Override
	public CompletableFuture<ForumResult> findForumResultOneAsync(ForumBean bean) {
		return dataSource()
				.nativeQueryOneAsync(
						ForumResult.class,
						"SELECT f.forum_groupid, s.forum_section_color FROM forum_info f, forum_section s WHERE f.forumid = s.forumid",
						(Map) Utility.ofMap("bean", bean));
	}

	@Override
	public List<ForumResult> queryForumResultList(ForumBean bean) {
		return dataSource()
				.nativeQueryList(
						ForumResult.class,
						"SELECT f.forum_groupid, s.forum_section_color FROM forum_info f, forum_section s WHERE f.forumid = s.forumid",
						(Map) Utility.ofMap("bean", bean));
	}

	@Override
	public CompletableFuture<List<ForumResult>> queryForumResultListAsync(ForumBean bean) {
		return dataSource()
				.nativeQueryListAsync(
						ForumResult.class,
						"SELECT f.forum_groupid, s.forum_section_color FROM forum_info f, forum_section s WHERE f.forumid = s.forumid",
						(Map) Utility.ofMap("bean", bean));
	}

	@Override
	public Sheet<ForumResult> queryForumResultSheet(ForumBean bean, Flipper flipper) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public CompletableFuture<Sheet<ForumResult>> queryForumResultSheetAsync(ForumBean bean, Flipper flipper) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Map<String, String> queryForumResultMap(ForumBean bean) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public CompletableFuture<Map<String, String>> queryForumResultMapAsync(ForumBean bean) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public int updateForumResult(ForumBean bean) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Integer updateForumResult2(ForumBean bean) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public CompletableFuture<Integer> updateForumResultAsync(ForumBean bean) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public DataSqlSource dataSource() {
		return _source;
	}

	@Override
	public Class<ForumInfo> entityType() {
		return _type;
	}
}
