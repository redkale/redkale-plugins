/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import org.redkale.convert.json.JsonConvert;
import org.redkale.persistence.*;

/** @author zhangjx */
public class TestComment {

    @Id
    @Column(length = 64, comment = "评论ID= userid+'-'+createTime")
    private String commentid = "";

    @Column(length = 64, comment = "动态ID")
    private String postid = "";

    @Column(comment = "用户ID")
    private int userid;

    @SearchColumn(text = true, options = "offsets", analyzer = "ik_max_word")
    @Column(length = 255, comment = "评论标题")
    private String title = "";

    @SearchColumn(text = true, options = "offsets", analyzer = "ik_max_word")
    @Column(comment = "评论内容")
    private String content = "";

    @Column(updatable = false, comment = "生成时间，单位毫秒")
    private long createTime;

    public String getCommentid() {
        return commentid;
    }

    public void setCommentid(String commentid) {
        this.commentid = commentid;
    }

    public String getPostid() {
        return postid;
    }

    public void setPostid(String postid) {
        this.postid = postid;
    }

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
