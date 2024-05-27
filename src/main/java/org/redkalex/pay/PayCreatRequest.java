/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import org.redkale.convert.ConvertDisabled;
import org.redkale.util.*;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayCreatRequest extends PayRequest {

    private static final Copier<PayCreatRequest, PayPreRequest> copier =
            Copier.create(PayCreatRequest.class, PayPreRequest.class);

    protected long payMoney; //  支付金额。 单位:分

    protected String payCurrency; //  币种,一般币值*100

    protected String payTitle = ""; // 订单标题

    protected String payBody = ""; // 订单内容描述

    protected int payTimeout = 600; // 支付超时的秒数

    protected String clientAddr = ""; // 客户端IP地址

    protected Map<String, String> attach; // 扩展信息

    public PayPreRequest createPayPreRequest() {
        return copier.apply(this, new PayPreRequest());
    }

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.payMoney < 1) {
            throw new RedkaleException("payMoney is illegal");
        }
        if (this.payTitle == null || this.payTitle.isEmpty()) {
            throw new RedkaleException("payTitle is illegal");
        }
        if (this.payBody == null || this.payBody.isEmpty()) {
            throw new RedkaleException("payBody is illegal");
        }
        if (this.clientAddr == null || this.clientAddr.isEmpty()) {
            throw new RedkaleException("clientAddr is illegal");
        }
        if (this.payTimeout < 300) {
            throw new RedkaleException("payTimeout cannot less 300 seconds");
        }
        if (this.payTimeout > 24 * 60 * 60) {
            throw new RedkaleException("payTimeout cannot greater 1 day");
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

    public String getPayTitle() {
        return payTitle;
    }

    public void setPayTitle(String payTitle) {
        this.payTitle = payTitle;
    }

    public String getPayBody() {
        return payBody;
    }

    public void setPayBody(String payBody) {
        this.payBody = payBody;
    }

    public int getPayTimeout() {
        return payTimeout;
    }

    public void setPayTimeout(int payTimeout) {
        this.payTimeout = payTimeout;
    }

    public String getClientAddr() {
        return clientAddr;
    }

    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }

    public Map<String, String> getAttach() {
        return attach;
    }

    public void setAttach(Map<String, String> attach) {
        this.attach = attach;
    }

    @Deprecated
    public Map<String, String> add(String key, String value) {
        if (this.attach == null) {
            this.attach = new TreeMap<>();
        }
        this.attach.put(key, value);
        return this.attach;
    }

    @Deprecated
    @ConvertDisabled
    public Map<String, String> getMap() {
        return attach;
    }

    @Deprecated
    @ConvertDisabled
    public void setMap(Map<String, String> map) {
        this.attach = map;
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
    public String getPaytitle() {
        return payTitle;
    }

    @Deprecated
    @ConvertDisabled
    public void setPaytitle(String payTitle) {
        this.payTitle = payTitle;
    }

    @Deprecated
    @ConvertDisabled
    public String getPaybody() {
        return payBody;
    }

    @Deprecated
    @ConvertDisabled
    public void setPaybody(String payBody) {
        this.payBody = payBody;
    }
}
