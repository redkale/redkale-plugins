/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.search;

import org.redkale.convert.json.JsonConvert;
import org.redkale.persistence.Column;
import org.redkale.source.*;

/** @author zhangjx */
public class TestPostBean implements FilterBean {

    @FilterColumn(comment = "用户ID")
    private int userid;

    @FilterColumn(comment = "文章标题")
    private String title = "";

    @FilterColumn(express = FilterExpress.LIKE, comment = "文章公开内容")
    private String pubContent = "";

    @Column(updatable = false, comment = "生成时间，单位毫秒")
    private Range.LongRange createTime;

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

    public String getPubContent() {
        return pubContent;
    }

    public void setPubContent(String pubContent) {
        this.pubContent = pubContent;
    }

    public Range.LongRange getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Range.LongRange createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
