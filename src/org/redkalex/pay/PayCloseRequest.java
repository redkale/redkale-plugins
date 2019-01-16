/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayCloseRequest extends PayRequest {

    protected String thirdpayno = ""; //第三方的支付流水号

    protected long paymoney; //  支付金额。 单位:分 

    protected Map<String, String> attach; //扩展信息

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.paymoney < 1) throw new RuntimeException("paymoney is illegal");
        if (this.thirdpayno == null || this.thirdpayno.isEmpty()) throw new RuntimeException("thirdpayno is illegal");
    }

    public Map<String, String> attach(String key, Object value) {
        if (this.attach == null) this.attach = new TreeMap<>();
        this.attach.put(key, String.valueOf(value));
        return this.attach;
    }

    public String attach(String name) {
        return attach == null ? null : attach.get(name);
    }

    public String attach(String name, String defValue) {
        return attach == null ? defValue : attach.getOrDefault(name, defValue);
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }

    public long getPaymoney() {
        return paymoney;
    }

    public void setPaymoney(long paymoney) {
        this.paymoney = paymoney;
    }

    public Map<String, String> getAttach() {
        return attach;
    }

    public void setAttach(Map<String, String> attach) {
        this.attach = attach;
    }
}
