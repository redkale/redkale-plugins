/*
 *
 */
package org.redkalex.source.parser2;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.redkale.persistence.Sql;
import org.redkale.source.Flipper;
import org.redkale.util.Sheet;

/** @author zhangjx */
public interface ForumInfoMapper extends BaseMapper<ForumInfo> {

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}")
    public ForumResult findForumResultOne(ForumBean bean);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}")
    public CompletableFuture<ForumResult> findForumResultOneAsync(ForumBean bean);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}")
    public List<ForumResult> queryForumResultList(ForumBean bean);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}")
    public CompletableFuture<List<ForumResult>> queryForumResultListAsync(ForumBean bean);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}")
    public Sheet<ForumResult> queryForumResultSheet(ForumBean bean, Flipper flipper);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}")
    public CompletableFuture<Sheet<ForumResult>> queryForumResultSheetAsync(ForumBean bean, Flipper flipper);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}")
    public Map<String, String> queryForumResultMap(ForumBean bean);

    @Sql("SELECT f.forum_groupid, s.forum_section_color "
            + "FROM forum_info f, forum_section s "
            + " WHERE f.forumid = s.forumid AND "
            + "s.forum_sectionid = ${bean.forumSectionid} AND "
            + "f.forumid = ${bean.forumid} AND s.forum_section_color = ${bean.forumSectionColor}")
    public CompletableFuture<Map<String, String>> queryForumResultMapAsync(ForumBean bean);

    @Sql("UPDATE forum_section s "
            + " SET s.forum_sectionid = '' "
            + " WHERE s.forum_section_color = ${bean.forumSectionColor}")
    public int updateForumResult(ForumBean bean);

    @Sql("UPDATE forum_section s "
            + " SET s.forum_sectionid = '' "
            + " WHERE s.forum_section_color = ${bean.forumSectionColor}")
    public Integer updateForumResult2(ForumBean bean);

    @Sql("UPDATE forum_section s "
            + " SET s.forum_sectionid = '' "
            + " WHERE s.forum_section_color = ${bean.forumSectionColor}")
    public CompletableFuture<Integer> updateForumResultAsync(ForumBean bean);
}
