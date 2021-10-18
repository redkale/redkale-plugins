package org.redkalex.source.mysql;

import java.io.Serializable;
import javax.persistence.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.source.*;
import org.redkale.util.ConstructorParameters;

/**
 *
 * @author zhangjx
 */
@Entity
//@DistributeTable(strategy = SmsRecord.TableStrategy.class)
public class SmsRecord {

    @Id
    @Column(length = 64, comment = "")
    private String smsid = "";

    @Column(comment = "短信类型;")
    private short smsType;

    @Column(comment = "验证码类型")
    private short codeType;

    @Column(comment = "状态;;")
    private short status;

    @Column(comment = "群发的短信条数")
    private int smsCount;

    @Column(comment = "群发的手机号码数")
    private int mobCount = 1;

    @Column(length = 32, comment = "手机号码")
    private String mobile = "";

    @Column(length = 2048, comment = "群发的手机号码集合")
    private String mobiles = "";

    @Column(length = 1024, comment = "短信内容")
    private String content = "";

    @Column(length = 1024, comment = "返回结果")
    private String resultDesc = "";

    @Column(updatable = false, comment = "生成时间，单位毫秒")
    private long createTime;

    @Transient
    @Column(comment = "用户ID")
    private long userid;//用户ID

    @ConstructorParameters({"codeType", "mobile", "content"})
    public SmsRecord(short smstype, String mobile, String content) {
        this.codeType = smstype;
        this.status = 1;
        this.mobile = mobile;
        this.content = content;
        this.createTime = System.currentTimeMillis();
    }

    public void setSmsid(String smsid) {
        this.smsid = smsid == null ? "" : smsid;
    }

    public String getSmsid() {
        return this.smsid;
    }

    public short getSmsType() {
        return smsType;
    }

    public void setSmsType(short smsType) {
        this.smsType = smsType;
    }

    public void setCodeType(short codeType) {
        this.codeType = codeType;
    }

    public short getCodeType() {
        return this.codeType;
    }

    public void setStatus(short status) {
        this.status = status;
    }

    public short getStatus() {
        return this.status;
    }

    public int getSmsCount() {
        return smsCount;
    }

    public void setSmsCount(int smsCount) {
        this.smsCount = smsCount;
    }

    public int getMobCount() {
        return mobCount;
    }

    public void setMobCount(int mobCount) {
        this.mobCount = mobCount;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getMobile() {
        return this.mobile;
    }

    public String getMobiles() {
        return mobiles;
    }

    public void setMobiles(String mobiles) {
        this.mobiles = mobiles == null ? "" : mobiles.trim();
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getContent() {
        return this.content;
    }

    public void setResultDesc(String resultDesc) {
        this.resultDesc = resultDesc;
    }

    public String getResultDesc() {
        return this.resultDesc;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public long getCreateTime() {
        return this.createTime;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

    public static class TableStrategy implements DistributeTableStrategy<SmsRecord> {

        private static final String format = "%1$tY%1$tm";

        @Override
        public String getTable(String table, FilterNode node) {
            Object time = node.findValue("createTime");
            if (time == null) time = node.findValue("#createTime");
            if (time instanceof Long) return getSingleTable(table, (Long) time);
            Range.LongRange createTime = (Range.LongRange) time;
            return getSingleTable(table, createTime.getMin());
        }

        @Override
        public String getTable(String table, SmsRecord bean) {
            return getSingleTable(table, bean.getCreateTime());
        }

        private String getSingleTable(String table, long createTime) {
            int pos = table.indexOf('.');
            return "" + table.substring(pos + 1) + "_" + String.format(format, createTime); //"notice."
        }

        @Override
        public String getTable(String table, Serializable primary) {
            String id = (String) primary;
            return getSingleTable(table, Long.parseLong(id.substring(0, id.indexOf('-')), 36));
        }
    }
}
