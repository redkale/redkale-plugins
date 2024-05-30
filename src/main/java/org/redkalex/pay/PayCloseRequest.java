/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import org.redkale.convert.ConvertDisabled;
import org.redkale.util.RedkaleException;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayCloseRequest extends PayRequest {

    protected String thirdPayno = ""; // 第三方的支付流水号

    protected long payMoney; //  支付金额。 单位:分

    protected String payCurrency; //  币种,一般币值*100

    protected Map<String, String> attach; // 扩展信息

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.payMoney < 1) {
            throw new RedkaleException("payMoney is illegal");
        }
        if (this.thirdPayno == null || this.thirdPayno.isEmpty()) {
            throw new RedkaleException("thirdPayno is illegal");
        }
    }

    public Map<String, String> attach(String key, Object value) {
        if (this.attach == null) {
            this.attach = new TreeMap<>();
        }
        this.attach.put(key, String.valueOf(value));
        return this.attach;
    }

    public String attach(String name) {
        return attach == null ? null : attach.get(name);
    }

    public String attach(String name, String defValue) {
        return attach == null ? defValue : attach.getOrDefault(name, defValue);
    }

    public String getThirdPayno() {
        return thirdPayno;
    }

    public void setThirdPayno(String thirdPayno) {
        this.thirdPayno = thirdPayno;
    }

    public long getPayMoney() {
        return payMoney;
    }

    public void setPayMoney(long payMoney) {
        this.payMoney = payMoney;
    }

    public String getPayCurrency() {
        return payCurrency;
    }

    public void setPayCurrency(String payCurrency) {
        this.payCurrency = payCurrency;
    }

    public Map<String, String> getAttach() {
        return attach;
    }

    public void setAttach(Map<String, String> attach) {
        this.attach = attach;
    }

    @Deprecated
    @ConvertDisabled
    public long getPaymoney() {
        return payMoney;
    }

    @Deprecated
    @ConvertDisabled
    public void setPaymoney(long payMoney) {
        this.payMoney = payMoney;
    }

    @Deprecated
    @ConvertDisabled
    public String getPaycurrency() {
        return payCurrency;
    }

    @Deprecated
    @ConvertDisabled
    public void setPaycurrency(String payCurrency) {
        this.payCurrency = payCurrency;
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
