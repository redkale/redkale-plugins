/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.convert.json.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayRequest {

    protected String appid = ""; //APP账号ID

    protected short paytype; //支付类型; 

    protected String payno = ""; //自己的订单号

    public PayRequest() {
    }

    public PayRequest(String appid, short paytype, String payno) {
        this.appid = appid;
        this.paytype = paytype;
        this.payno = payno;
    }

    public void checkVaild() {
        if (this.paytype < 1) throw new RuntimeException("paytype is illegal");
        //只有一个支付配置时无需提供appid
        //if (this.paytype != Pays.PAYTYPE_UNION && (this.appid == null || this.appid.isEmpty())) throw new RuntimeException("appid is illegal");
        if (this.payno == null || this.payno.isEmpty()) throw new RuntimeException("payno is illegal");
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
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
        if (payno != null) {
            this.payno = payno.trim();
        }
    }

}
