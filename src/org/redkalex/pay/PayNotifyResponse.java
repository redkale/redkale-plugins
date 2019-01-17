/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.convert.json.JsonFactory;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayNotifyResponse extends PayResponse {

    protected short paytype; //支付类型; 

    protected String payno = ""; //自己的订单号

    protected String thirdpayno = ""; //第三方的支付流水号

    protected String notifytext = ""; //返回的文本信息, 常见场景是success字符串

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
        this.notifytext = notifytext == null ? "" : notifytext;
        return this;
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }

    public String getNotifytext() {
        return notifytext;
    }

    public void setNotifytext(String notifytext) {
        this.notifytext = notifytext == null ? "" : notifytext;
    }

    public short getPaytype() {
        return paytype;
    }

    public void setPaytype(short paytype) {
        this.paytype = paytype;
    }

    public String getPayno() {
        return payno;
    }

    public void setPayno(String payno) {
        this.payno = payno;
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }

}
