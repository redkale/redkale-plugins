/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.redkalex.source.mysql;

import java.util.regex.Pattern;
import javax.persistence.*;
import org.redkale.convert.ConvertDisabled;
import org.redkale.source.SearchColumn;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class PostRecord {

    public static final Pattern REGX_EMOJI = Pattern.compile("\\[(s-\\d+)-(\\d+)\\]");

    private static final int READ_MORE_LIMIT = 100; //多于100个字需要显示查看全文

    //---------------------- 表情 ---------------------------------------
    public static final int POST_ESCAPEDFLAG_EMOJI = 1; //包含表情转义 

    public static final int POST_ESCAPEDFLAG_FILE = 2; //包含附件转义

    public static final int POST_ESCAPEDFLAG_VIDEO = 4; //包含视频转义

    public static final int POST_ESCAPEDFLAG_MUSIC = 8; //包含音频转义

    //---------------------- 查看 ---------------------------------------
    public static final int POST_POWER_PUBLIC = 1; //公开查看

    public static final int POST_POWER_PAY = 2; //付费查看

    public static final int POST_POWER_PWD = 4; //密码查看

    public static final int POST_POWER_SECRET = 8; //私密内容

    public static final int POST_POWER_VIP = 16; //VIP查看

    public static final int POST_POWER_LOGIN = 32; //登录查看

    public static final int POST_POWER_COMMENT = 64; //回复查看

    public static final int POST_POWER_VERIFY = 128; //认证查看

    public static final int POST_POWER_FOLLOW = 256; //粉丝查看

    //---------------------- 类型 ---------------------------------------
    public static final String POST_TYPE_WORDS = "words"; //动态

    public static final String POST_TYPE_MUSIC = "music"; //音乐

    public static final String POST_TYPE_VIDEO = "video"; //视频

    public static final String POST_TYPE_BLOG = "blog"; //文章 single

    public static final String POST_TYPE_FORUM = "forum"; //帖子 bbs

    //---------------------- 评论 ---------------------------------------
    public static final short POST_COMMENTFLAG_OPEN = 10; //允许评论

    public static final short POST_COMMENTFLAG_CLOSE = 20; //关闭评论

    //---------------------- 精品 ---------------------------------------
    public static final short POST_NICEFLAG_NO = 10; //普通

    public static final short POST_NICEFLAG_OK = 20; //精品
    //---------------------- 置顶 ---------------------------------------

    public static final short POST_TOPFLAG_NO = 10; //普通

    public static final short POST_TOPFLAG_OK = 20; //置顶
    //---------------------- 悬赏 ---------------------------------------

    public static final short POST_REWARDFLAG_NO = 10; //普通

    public static final short POST_REWARDFLAG_OK = 20; //悬赏

    @Id
    @Column(length = 128, comment = "文章ID= type[0]+user36id+'-'+create36time")
    private String postid = "";

    @Column(comment = "用户ID")
    private int userid;

    @SearchColumn(text = true, options = "offsets", analyzer = "ik_max_word")
    @Column(length = 255, comment = "文章标题")
    private String title = "";

    @SearchColumn(text = true, options = "offsets", analyzer = "ik_max_word", html = true)
    @Column(length = 65535, nullable = false, comment = "文章公开内容") //length=65535视为TEXT类型
    private String pubContent = "";

    @Column(length = 65535, nullable = false, comment = "文章隐藏内容") //length=65535视为TEXT类型
    private String hideContent = "";

    @Column(comment = "公开内容是否包含特殊处理，位运算; 1: 包含表情转义; 2:包含附件转义;4:包含视频转义;8:包含音频转义;")
    private int pubEscapedFlag;

    @Column(comment = "隐藏内容是否包含特殊处理，位运算; 1: 包含表情转义; 2:包含附件转义;4:包含视频转义;8:包含音频转义;")
    private int hideEscapedFlag;

    @SearchColumn(ignore = true)
    @Column(comment = "图片url,多个用;分割")
    private String imgUrls = "";

    @SearchColumn(ignore = true)
    @Column(comment = "用户ID")
    private int imgCount;

    @Column(length = 128, comment = "父文章ID")
    private String parentid = "";

    @Column(length = 64, comment = "论坛ID")
    private String forumid = "";

    @Column(length = 64, comment = "论坛小版块ID")
    private String forumSectionid = "";

    @Column(comment = "状态: 10:正常的;20:待审核; 30:审核不过;80:删除;")
    private short status;

    @Column(comment = "文章类型: 动态:words; 音乐:music;视频:video;文章:blog;帖子:forum;")
    private String type = POST_TYPE_WORDS;

    @Column(comment = "查看权限; 位运算;  1:公开查看;2:登录查看;4:回复查看;8:密码查看;16:付费查看;32:粉丝查看;64:认证查看;128:VIP查看;256:私密内容")
    private long power = POST_POWER_PUBLIC;

    @SearchColumn(ignore = true)
    @Column(length = 128, comment = "文章查看所需密码;仅限于密码查看")
    private String seePwd = "";

    @SearchColumn(ignore = true)
    @Column(comment = "文章查看所需金币数;仅限于付费查看")
    private int seeCoin;

    @Column(length = 128, comment = "文章关联的对象ID，例如帖子类型的文章需要属于某个板块")
    private String refObjid = "";

    @Column(length = 128, comment = "话题，多个用#隔开，首尾必须有#，例如: #视频#时尚#")
    private String topicNames = "";

    @SearchColumn(ignore = true)
    @Column(comment = "前多少张图片免费看")
    private int freeImgCount;

    @SearchColumn(ignore = true)
    @Column(length = 2048, comment = "封面URL")
    private String coverImgUrl = "";

    @SearchColumn(ignore = true)
    @Column(length = 2048, comment = "视频或音频URL")
    private String videoUrl = "";

    @Column(comment = "精品标记; 10:普通;20:精品;")
    private short niceFlag = POST_NICEFLAG_NO;

    @Column(comment = "置顶标记; 10:普通;20:置顶;")
    private short topFlag = POST_TOPFLAG_NO;

    @Column(comment = "悬赏标记; 10:普通;20:悬赏; ")
    private short rewardFlag = POST_REWARDFLAG_NO;

    @SearchColumn(ignore = true)
    @Column(comment = "悬赏金币数")
    private int rewardCoin;

    @SearchColumn(ignore = true)
    @Column(length = 64, comment = "悬赏采纳的评论ID")
    private String rewardCommentid;

    @SearchColumn(ignore = true)
    @Column(comment = "评论权限; 10:允许评论; 20:关闭评论;")
    private short commentFlag = POST_COMMENTFLAG_OPEN;

    @SearchColumn(ignore = true)
    @Column(comment = "评论数")
    private long commentCount;

    @SearchColumn(ignore = true)
    @Column(comment = "最后评论人")
    private int lastCommentUserid;

    @SearchColumn(ignore = true)
    @Column(comment = "最后评论时间")
    private long lastCommentTime;

    @SearchColumn(ignore = true)
    @Column(comment = "输入密码成功查看次数")
    private long pwdCount;

    @SearchColumn(ignore = true)
    @Column(comment = "付费数")
    private long payCount;

    @SearchColumn(ignore = true)
    @Column(comment = "关注数")
    private long likeCount;

    @SearchColumn(ignore = true)
    @Column(comment = "推广分享次数")
    private long shareCount;

    @SearchColumn(ignore = true)
    @Column(comment = "阅读次数")
    private long visitCount;

    @Column(length = 32, comment = "发布时的客户端IP")
    private String clientAddr = "";

    @Column(length = 32, comment = "发布时的所在城市")
    private String clientCity = "";

    @Column(length = 32, comment = "发布时的终端: web_pc;web_mobile;app_android;app_ios;")
    private String clientAgent = "";

    @Column(length = 10240, comment = "扩展字段，map结构")
    private String metas = "";

    @Column(updatable = false, comment = "生成时间，单位毫秒")
    private long createTime;

    public static String createId(String type, int userid, long createTime) {
        //forum 开头的 f 会特殊处理， 以后新加的type不能以f开头
        return type.charAt(0) + Integer.toString(userid, 36) + "-" + Utility.format36time(createTime);
    }

    public boolean pubNeedReadMore() {
        String content = this.pubContent;
        if (content == null || content.isEmpty()) return false;
        if (content.length() > READ_MORE_LIMIT) return true;
        int space = 0;
        int size = Math.min(content.length(), READ_MORE_LIMIT);
        for (int i = 0; i < size; i++) {
            if (content.charAt(i) == '\n') space++;
        }
        return space > 4;
    }

    public boolean hideNeedReadMore() {
        String content = this.hideContent;
        if (content == null || content.isEmpty()) return false;
        if (content.length() > READ_MORE_LIMIT) return true;
        int space = 0;
        int size = Math.min(content.length(), READ_MORE_LIMIT);
        for (int i = 0; i < size; i++) {
            if (content.charAt(i) == '\n') space++;
        }
        return space > 4;
    }

    public void increPubEscapedFlag(int flag) {
        this.pubEscapedFlag |= flag;
    }

    public void increHideEscapedFlag(int flag) {
        this.hideEscapedFlag |= flag;
    }

    //是否容许评论
    public boolean canComment() {
        return this.commentFlag == POST_COMMENTFLAG_OPEN;
    }

    @ConvertDisabled
    public boolean flagNice() {
        return this.niceFlag == POST_NICEFLAG_OK;
    }

    @ConvertDisabled
    public boolean flagTop() {
        return this.topFlag == POST_TOPFLAG_OK;
    }

    @ConvertDisabled
    public boolean flagReward() {
        return this.rewardFlag == POST_REWARDFLAG_OK;
    }

    @ConvertDisabled //悬赏是否已解决
    public boolean rewardOver() {
        return this.rewardCommentid != null && !this.rewardCommentid.isEmpty();
    }

    @ConvertDisabled
    public boolean powerPay() {
        return this.power == POST_POWER_PAY;
    }

    @ConvertDisabled
    public boolean powerComment() {
        return this.power == POST_POWER_COMMENT;
    }

    public boolean emptyForumid() {
        return this.forumid == null || this.forumid.isEmpty();
    }

    public boolean emptyForumSectionid() {
        return this.forumSectionid == null || this.forumSectionid.isEmpty();
    }

    public boolean emptyTopicNames() {
        return this.topicNames == null || this.topicNames.isEmpty();
    }

    public boolean emptyImgUrls() {
        return this.imgUrls == null || this.imgUrls.isEmpty();
    }

    public boolean emptyTitle() {
        return this.title == null || this.title.isEmpty();
    }

    public boolean emptyPubContent() {
        return this.pubContent == null || this.pubContent.isEmpty();
    }

    public boolean mustHideContent() {
        return !POST_TYPE_VIDEO.equals(type) && !POST_TYPE_MUSIC.equals(type);
    }

    public boolean emptyHideContent() {
        return this.hideContent == null || this.hideContent.isEmpty();
    }

    public boolean emptySeePwd() {
        return this.seePwd == null || this.seePwd.isEmpty();
    }

    public String[] imageUrlArray() {
        String imgs = this.imgUrls;
        if (imgs == null || imgs.isEmpty()) return new String[0];
        return imgs.split(";");
    }

    public String[] topicNameArray() {
        String tns = this.topicNames;
        if (tns == null || tns.isEmpty()) return new String[0];
        if (tns.charAt(0) == '#') tns = tns.substring(1);
        if (tns.charAt(tns.length() - 1) == '#') tns = tns.substring(0, tns.length() - 1);
        return tns.split("#");
    }

    public synchronized void increVisitCount() {
        this.visitCount++;
    }

    public synchronized void increCommentCount() {
        this.commentCount++;
    }

    public synchronized void increLikeCount() {
        this.likeCount++;
    }

    public void setPostid(String postid) {
        this.postid = postid;
    }

    public String getPostid() {
        return this.postid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public int getUserid() {
        return this.userid;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return this.title;
    }

    public void setPubContent(String pubContent) {
        this.pubContent = pubContent;
    }

    public String getPubContent() {
        return this.pubContent;
    }

    public String getHideContent() {
        return hideContent;
    }

    public void setHideContent(String hideContent) {
        this.hideContent = hideContent;
    }

    public String getImgUrls() {
        return imgUrls;
    }

    public void setImgUrls(String imgUrls) {
        this.imgUrls = imgUrls;
    }

    public int getImgCount() {
        return imgCount;
    }

    public void setImgCount(int imgCount) {
        this.imgCount = imgCount;
    }

    public String getParentid() {
        return parentid;
    }

    public void setParentid(String parentid) {
        this.parentid = parentid;
    }

    public void setStatus(short status) {
        this.status = status;
    }

    public short getStatus() {
        return this.status;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return this.type;
    }

    public void setPower(long power) {
        this.power = power;
    }

    public long getPower() {
        return this.power;
    }

    public void setSeePwd(String seePwd) {
        this.seePwd = seePwd;
    }

    public String getSeePwd() {
        return this.seePwd;
    }

    public void setSeeCoin(int seeCoin) {
        this.seeCoin = seeCoin;
    }

    public int getSeeCoin() {
        return this.seeCoin;
    }

    public String getRefObjid() {
        return refObjid;
    }

    public void setRefObjid(String refObjid) {
        this.refObjid = refObjid;
    }

    public String getTopicNames() {
        return topicNames;
    }

    public void setTopicNames(String topicNames) {
        this.topicNames = topicNames;
    }

    public void setFreeImgCount(int freeImgCount) {
        this.freeImgCount = freeImgCount;
    }

    public int getFreeImgCount() {
        return this.freeImgCount;
    }

    public void setCoverImgUrl(String coverImgUrl) {
        this.coverImgUrl = coverImgUrl;
    }

    public String getCoverImgUrl() {
        return this.coverImgUrl;
    }

    public void setVideoUrl(String videoUrl) {
        this.videoUrl = videoUrl;
    }

    public String getVideoUrl() {
        return this.videoUrl;
    }

    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public String getClientAddr() {
        return this.clientAddr;
    }

    public void setClientCity(String clientCity) {
        this.clientCity = clientCity;
    }

    public String getClientCity() {
        return this.clientCity;
    }

    public void setClientAgent(String clientAgent) {
        this.clientAgent = clientAgent;
    }

    public String getClientAgent() {
        return this.clientAgent;
    }

    public void setCommentFlag(short commentFlag) {
        this.commentFlag = commentFlag;
    }

    public short getCommentFlag() {
        return this.commentFlag;
    }

    public void setCommentCount(long commentCount) {
        this.commentCount = commentCount;
    }

    public long getCommentCount() {
        return this.commentCount;
    }

    public int getLastCommentUserid() {
        return lastCommentUserid;
    }

    public void setLastCommentUserid(int lastCommentUserid) {
        this.lastCommentUserid = lastCommentUserid;
    }

    public void setLastCommentTime(long lastCommentTime) {
        this.lastCommentTime = lastCommentTime;
    }

    public long getLastCommentTime() {
        return this.lastCommentTime;
    }

    public long getPwdCount() {
        return pwdCount;
    }

    public void setPwdCount(long pwdCount) {
        this.pwdCount = pwdCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    public void setLikeCount(long likeCount) {
        this.likeCount = likeCount;
    }

    public long getLikeCount() {
        return this.likeCount;
    }

    public void setShareCount(long shareCount) {
        this.shareCount = shareCount;
    }

    public long getShareCount() {
        return this.shareCount;
    }

    public void setVisitCount(long visitCount) {
        this.visitCount = visitCount;
    }

    public long getVisitCount() {
        return this.visitCount;
    }

    public void setMetas(String metas) {
        this.metas = metas;
    }

    public String getMetas() {
        return this.metas;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getCreateTime() {
        return this.createTime;
    }

    public String getForumid() {
        return forumid;
    }

    public void setForumid(String forumid) {
        this.forumid = forumid;
    }

    public String getForumSectionid() {
        return forumSectionid;
    }

    public void setForumSectionid(String forumSectionid) {
        this.forumSectionid = forumSectionid;
    }

    public short getNiceFlag() {
        return niceFlag;
    }

    public void setNiceFlag(short niceFlag) {
        this.niceFlag = niceFlag;
    }

    public short getTopFlag() {
        return topFlag;
    }

    public void setTopFlag(short topFlag) {
        this.topFlag = topFlag;
    }

    public short getRewardFlag() {
        return rewardFlag;
    }

    public void setRewardFlag(short rewardFlag) {
        this.rewardFlag = rewardFlag;
    }

    public String getRewardCommentid() {
        return rewardCommentid;
    }

    public void setRewardCommentid(String rewardCommentid) {
        this.rewardCommentid = rewardCommentid;
    }

    public int getRewardCoin() {
        return rewardCoin;
    }

    public void setRewardCoin(int rewardCoin) {
        this.rewardCoin = rewardCoin;
    }

    public int getPubEscapedFlag() {
        return pubEscapedFlag;
    }

    public void setPubEscapedFlag(int pubEscapedFlag) {
        this.pubEscapedFlag = pubEscapedFlag;
    }

    public int getHideEscapedFlag() {
        return hideEscapedFlag;
    }

    public void setHideEscapedFlag(int hideEscapedFlag) {
        this.hideEscapedFlag = hideEscapedFlag;
    }

}
