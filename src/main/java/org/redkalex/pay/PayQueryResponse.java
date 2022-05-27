/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;
import org.redkale.convert.ConvertDisabled;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayQueryResponse extends PayResponse {

    protected short payStatus;

    protected long payedMoney;

    protected String thirdPayno = ""; //第三方的支付流水号

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

    public long getPayedMoney() {
        return payedMoney;
    }

    public void setPayedMoney(long payedMoney) {
        this.payedMoney = payedMoney;
    }

    public short getPayStatus() {
        return payStatus;
    }

    public void setPayStatus(short payStatus) {
        this.payStatus = payStatus;
    }

    public String getThirdPayno() {
        return thirdPayno;
    }

    public void setThirdPayno(String thirdPayno) {
        this.thirdPayno = thirdPayno;
    }

    @Deprecated
    @ConvertDisabled
    public short getPaystatus() {
        return payStatus;
    }

    @Deprecated
    @ConvertDisabled
    public void setPaystatus(short payStatus) {
        this.payStatus = payStatus;
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
}
