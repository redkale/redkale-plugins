package org.redkalex.source.search;

import org.redkale.convert.json.JsonConvert;
import org.redkale.persistence.*;
import org.redkale.util.Times;

/** @author zhangjx */
@Table(comment = "文章记录表")
public class TestPost {

	@Id
	@Column(length = 128, comment = "文章ID= type[0]+user36id+'-'+create36time")
	private String postid = "";

	@Column(comment = "用户ID")
	private int userid;

	@SearchColumn(text = true, options = "offsets", analyzer = "ik_max_word")
	@Column(length = 255, comment = "文章标题")
	private String title = "";

	@SearchColumn(text = true, options = "offsets", analyzer = "ik_max_word", html = true)
	@Column(comment = "文章公开内容")
	private String pubContent = "";

	@Column(comment = "文章隐藏内容")
	private String hideContent = "";

	@Column(comment = "文章类型: 动态:words; 音乐:music;视频:video;文章:blog;帖子:forum;")
	private String type = "words";

	@Column(comment = "查看权限")
	private long power = 1;

	@Column(updatable = false, comment = "生成时间，单位毫秒")
	private long createTime;

	@SearchColumn(highlight = "pubContent", ignore = true)
	private String highlightContent;

	@SearchColumn(highlight = "title", ignore = true)
	private String highlightTitle;

	public static String createId(String type, int userid, long createTime) {
		return type.charAt(0) + Integer.toString(userid, 36) + "-" + Times.format36time(createTime);
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

	public String getPubContent() {
		return pubContent;
	}

	public void setPubContent(String pubContent) {
		this.pubContent = pubContent;
	}

	public String getHideContent() {
		return hideContent;
	}

	public void setHideContent(String hideContent) {
		this.hideContent = hideContent;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getPower() {
		return power;
	}

	public void setPower(long power) {
		this.power = power;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public String getHighlightContent() {
		return highlightContent;
	}

	public void setHighlightContent(String highlightContent) {
		this.highlightContent = highlightContent;
	}

	public String getHighlightTitle() {
		return highlightTitle;
	}

	public void setHighlightTitle(String highlightTitle) {
		this.highlightTitle = highlightTitle;
	}

	@Override
	public String toString() {
		return JsonConvert.root().convertTo(this);
	}
}
