/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.convert.ConvertDisabled;
import org.redkale.convert.json.JsonFactory;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayNotifyResponse extends PayResponse {

    protected short payType; // 支付类型;

    protected String payno = ""; // 自己的订单号

    protected long payedMoney = -1; // 到账的金额，单位：分

    protected long refundedMoney = -1; // 退款的金额，单位：分

    protected String thirdPayno = ""; // 第三方的支付流水号

    protected String notifyText = ""; // 返回的文本信息, 常见场景是success字符串

    @Override
    public PayNotifyResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayNotifyResponse retinfo(String retinfo) {
        if (retinfo != null) this.retinfo = retinfo;
        return this;
    }

    public PayNotifyResponse notifytext(String notifytext) {
        this.notifyText = notifytext == null ? "" : notifytext;
        return this;
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }

    public String getNotifyText() {
        return notifyText;
    }

    public void setNotifyText(String notifyText) {
        this.notifyText = notifyText == null ? "" : notifyText;
    }

    public short getPayType() {
        return payType;
    }

    public void setPayType(short payType) {
        this.payType = payType;
    }

    public String getPayno() {
        return payno;
    }

    public void setPayno(String payno) {
        this.payno = payno;
    }

    public long getPayedMoney() {
        return payedMoney;
    }

    public void setPayedMoney(long payedMoney) {
        this.payedMoney = payedMoney;
    }

    public String getThirdPayno() {
        return thirdPayno;
    }

    public void setThirdPayno(String thirdPayno) {
        this.thirdPayno = thirdPayno;
    }

    public long getRefundedMoney() {
        return refundedMoney;
    }

    public void setRefundedMoney(long refundedMoney) {
        this.refundedMoney = refundedMoney;
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
    public String getThirdpayno() {
        return thirdPayno;
    }

    @Deprecated
    @ConvertDisabled
    public void setThirdpayno(String thirdPayno) {
        this.thirdPayno = thirdPayno;
    }

    @Deprecated
    @ConvertDisabled
    public String getNotifytext() {
        return notifyText;
    }

    @Deprecated
    @ConvertDisabled
    public void setNotifytext(String notifyText) {
        this.notifyText = notifyText == null ? "" : notifyText;
    }

    @Deprecated
    @ConvertDisabled
    public long getPayedmoney() {
        return payedMoney;
    }

    @Deprecated
    @ConvertDisabled
    public void setPayedmoney(long payedMoney) {
        this.payedMoney = payedMoney;
    }
}
