/*
 *
 */
package org.redkalex.source.mapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.redkale.annotation.Param;
import org.redkale.persistence.Sql;
import org.redkale.source.RowBound;
import org.redkale.util.Sheet;

/** @author zhangjx */
public interface ForumInfoMapper extends BaseMapper<ForumInfo> {

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = #{bean.forumSectionid} AND "
            + "f.forumid = #{bean.forumid} AND s.forum_section_color = #{bean.forumSectionColor}")
    public ForumResult findForumResult(ForumBean bean);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = #{bean.forumSectionid} AND "
            + "f.forumid = #{bean.forumid} AND s.forum_section_color = #{bean.forumSectionColor}")
    public CompletableFuture<ForumResult> findForumResultAsync(ForumBean bean);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = #{bean.forumSectionid} AND "
            + "f.forumid = #{bean.forumid} AND s.forum_section_color = #{bean.forumSectionColor}")
    public List<ForumResult> queryForumResult(@Param("bean") ForumBean bean0);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = #{bean.forumSectionid} AND "
            + "f.forumid = #{bean.forumid} AND s.forum_section_color = #{bean.forumSectionColor}")
    public CompletableFuture<List<ForumResult>> queryForumResultAsync(ForumBean bean);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = #{bean.forumSectionid} AND "
            + "f.forumid = #{bean.forumid} AND s.forum_section_color = #{bean.forumSectionColor}")
    public Sheet<ForumResult> queryForumResultSheet(ForumBean bean, RowBound round);
}
