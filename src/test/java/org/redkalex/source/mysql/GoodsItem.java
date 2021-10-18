/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.mysql;

import javax.persistence.*;
import org.redkale.convert.ConvertColumn;
import org.redkale.source.Range;

/**
 *
 * @author zhangjx
 */
public class GoodsItem {
    
    public static final short EXPIRE_TYPE_PART = 1; //时间段过期， 比如有效期7天

    public static final short EXPIRE_TYPE_END = 2; //时间点过期， 比如2020-12-12

    @Id
    @ConvertColumn(index = 1)
    @Column(comment = "商品ID")
    protected int goodsid;

    @ConvertColumn(index = 2)
    @Column(comment = "商品数量")
    protected long count;

    @ConvertColumn(index = 3)
    @Column(comment = "商品数量范围, 比count优先级高")
    protected Range.LongRange counts;

    @ConvertColumn(index = 4)
    @Column(comment = "商品过期秒数*10+过期类别，为0表示不过期, 允许为null可以减少存储量")
    protected Long expires;

    public int getGoodsid() {
        return goodsid;
    }

    public void setGoodsid(int goodsid) {
        this.goodsid = goodsid;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public Range.LongRange getCounts() {
        return counts;
    }

    public void setCounts(Range.LongRange counts) {
        this.counts = counts;
    }

    public Long getExpires() {
        return expires;
    }

    public void setExpires(Long expires) {
        this.expires = expires;
    }
    
}
