/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayQueryResponse extends PayResponse {

    private short paystatus;

    private long payedmoney;

    private String thirdpayno = ""; //第三方的支付流水号

    @Override
    public PayQueryResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayQueryResponse retinfo(String retinfo) {
        if (retinfo != null) this.retinfo = retinfo;
        return this;
    }

    @Override
    public PayQueryResponse result(Map<String, String> result) {
        this.setResult(result);
        return this;
    }

    public long getPayedmoney() {
        return payedmoney;
    }

    public void setPayedmoney(long payedmoney) {
        this.payedmoney = payedmoney;
    }

    public short getPaystatus() {
        return paystatus;
    }

    public void setPaystatus(short paystatus) {
        this.paystatus = paystatus;
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }
}
