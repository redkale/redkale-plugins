/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import javax.persistence.*;
import org.redkale.convert.*;
import org.redkale.convert.json.JsonConvert;

/**
 *
 * @author zhangjx
 */
@Entity
public class DutyRewardInfo {
    
   
    //连续签到
    public static final int DUTY_TYPE_SERIE = 10;

    //累计签到
    public static final int DUTY_TYPE_TOTAL = 20;

    @Id
    @Column(comment = "签到奖励ID， 从1001开始")
    @ConvertColumn(index = 1)
    private int dutyRewardid = 1001;

    @Column(comment = "签到序号，从1开始")
    @ConvertColumn(index = 2)
    private int dutyIndex;

    @Column(length = 4096, nullable = false, comment = "签到领取的复合商品, GoodsItem[]数组")
    @ConvertColumn(index = 3)
    private GoodsItem[] goodsItems;

    @Column(length = 1024, comment = "备注")
    @ConvertColumn(index = 5)
    private String remark = "";

    @Column(updatable = false, comment = "生成时间，单位毫秒")
    @ConvertColumn(index = 6)
    private long createTime;

    public void setDutyRewardid(int dutyRewardid) {
        this.dutyRewardid = dutyRewardid;
    }

    public int getDutyRewardid() {
        return this.dutyRewardid;
    }

    public void setDutyIndex(int dutyIndex) {
        this.dutyIndex = dutyIndex;
    }

    public int getDutyIndex() {
        return this.dutyIndex;
    }

    public void setGoodsItems(GoodsItem[] goodsItems) {
        this.goodsItems = goodsItems;
    }

    public GoodsItem[] getGoodsItems() {
        return this.goodsItems;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    @ConvertColumn(ignore = true, type = ConvertType.PROTOBUF_JSON)
    public String getRemark() {
        return this.remark;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @ConvertColumn(ignore = true, type = ConvertType.PROTOBUF_JSON)
    public long getCreateTime() {
        return this.createTime;
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

}
