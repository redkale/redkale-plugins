/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.convert.ConvertDisabled;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.RedkaleException;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayRequest {

    protected String appid = ""; // APP账号ID

    protected short payType; // 支付类型;

    protected short payWay; // 支付渠道;

    protected short subPayType; // 子支付类型;

    protected String payno = ""; // 自己的订单号

    public PayRequest() {}

    public PayRequest(String appid, short payType, short payWay, String payno) {
        this.appid = appid;
        this.payType = payType;
        this.payWay = payWay;
        this.payno = payno;
    }

    public void checkVaild() {
        if (this.payType < 1) {
            throw new RedkaleException("payType is illegal");
        }
        if (this.payWay < 1) {
            throw new RedkaleException("payWay is illegal");
        }
        // 只有一个支付配置时无需提供appid
        // if (this.payType != Pays.PAYTYPE_UNION && (this.appid == null || this.appid.isEmpty())) throw new
        // RedkaleException("appid is illegal");
        if (this.payno == null || this.payno.isEmpty()) {
            throw new RedkaleException("payno is illegal");
        }
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

    @Deprecated
    @ConvertDisabled
    public short getPaytype() {
        return payType;
    }

    @Deprecated
    @ConvertDisabled
    public void setPaytype(short payType) {
        this.payType = payType;
    }

    @Deprecated
    @ConvertDisabled
    public short getSubpaytype() {
        return subPayType;
    }

    @Deprecated
    @ConvertDisabled
    public void setSubpaytype(short subPayType) {
        this.subPayType = subPayType;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public short getPayType() {
        return payType;
    }

    public void setPayType(short payType) {
        this.payType = payType;
    }

    public short getSubPayType() {
        return subPayType;
    }

    public void setSubPayType(short subPayType) {
        this.subPayType = subPayType;
    }

    public short getPayWay() {
        return payWay;
    }

    public void setPayWay(short payWay) {
        this.payWay = payWay;
    }

    public String getPayno() {
        return payno;
    }

    public void setPayno(String payno) {
        if (payno != null) {
            this.payno = payno.trim();
        }
    }
}
