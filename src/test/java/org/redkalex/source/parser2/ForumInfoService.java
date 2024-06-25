/*
 *
 */
package org.redkalex.source.parser2;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.redkale.annotation.Resource;
import org.redkale.service.AbstractService;
import org.redkale.source.DataSqlSource;

/**
 *
 * @author zhangjx
 */
public class ForumInfoService extends AbstractService {

    //查询单个记录的sql
    private static final String findOneSql = "SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}";

    //查询列表记录的sql
    private static final String queryListSql = "SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}";

    @Resource
    private DataSqlSource source;

    public ForumResult findForumResultOne(ForumBean bean) {
        return source.nativeQueryOne(ForumResult.class, findOneSql, Map.of("bean", bean));
    }

    public CompletableFuture<List<ForumResult>> queryForumResultListAsync(ForumBean bean) {
        return source.nativeQueryListAsync(ForumResult.class, queryListSql, Map.of("bean", bean));
    }
}
