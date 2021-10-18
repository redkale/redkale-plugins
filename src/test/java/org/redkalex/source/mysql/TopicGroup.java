/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import javax.persistence.*;

/**
 *
 * @author zhangjx
 */
@Entity
public class TopicGroup {
    
    @Id
    @Column(length = 32, comment = "话题分类的id； 例如: girl")
    private String topicGroupid = "";

    @Column(length = 64, comment = "话题分类的名称,不带#的； 例如: 美女")
    private String topicGroupName = "";

    @Column(comment = "话题数量")
    private int topicCount;

    @Column(comment = "排序顺序，值小靠前")
    private int display = 1000;
    
    public void setTopicGroupid(String topicGroupid) {
        this.topicGroupid = topicGroupid;
    }

    public String getTopicGroupid() {
        return this.topicGroupid;
    }

    public void setTopicGroupName(String topicGroupName) {
        this.topicGroupName = topicGroupName;
    }

    public String getTopicGroupName() {
        return this.topicGroupName;
    }

    public void setTopicCount(int topicCount) {
        this.topicCount = topicCount;
    }

    public int getTopicCount() {
        return this.topicCount;
    }

    public int getDisplay() {
        return display;
    }

    public void setDisplay(int display) {
        this.display = display;
    }

}
