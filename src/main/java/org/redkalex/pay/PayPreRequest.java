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
public class PayPreRequest extends PayRequest {

    protected long payMoney; //  支付金额。 单位:分

    protected String payCurrency; //  币种,一般币值*100

    protected String payTitle = ""; // 订单标题

    protected String payBody = ""; // 订单内容描述

    protected String notifyUrl = ""; // 回调url 不为空时会替换默认的回调url

    protected String returnUrl = ""; // 页面返回url

    protected int timeoutSeconds = 10 * 60; // 支付超时的分钟数

    protected String clientHost = ""; // HTTP请求的Host

    protected String clientAddr = ""; // 客户端IP地址

    protected Map<String, String> attach; // 扩展信息

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.payMoney < 1) {
            throw new RedkaleException("payMoney is illegal");
        }
        if (this.payTitle == null || this.payTitle.isEmpty() || this.payTitle.indexOf('"') >= 0) {
            throw new RedkaleException("payTitle is illegal");
        }
        if (this.payBody == null || this.payBody.isEmpty() || this.payBody.indexOf('"') >= 0) {
            throw new RedkaleException("payBody is illegal");
        }
        if (this.clientAddr == null || this.clientAddr.isEmpty()) {
            throw new RedkaleException("clientAddr is illegal");
        }
        if (this.timeoutSeconds != 0 && this.timeoutSeconds < 3 * 60) {
            throw new RedkaleException("timeoutms cannot less 3 minutes");
        }
        if (this.timeoutSeconds > 24 * 60) {
            throw new RedkaleException("timeoutms cannot greater 1 day");
        }
    }

    public Map<String, String> attach(String key, Object value) {
        if (this.attach == null) {
            this.attach = new TreeMap<>();
        }
        this.attach.put(key, String.valueOf(value));
        return this.attach;
    }

    public String getAttach(String name) {
        return attach == null ? null : attach.get(name);
    }

    public String getAttach(String name, String defValue) {
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

    public String getNotifyUrl() {
        return notifyUrl;
    }

    public void setNotifyUrl(String notifyUrl) {
        this.notifyUrl = notifyUrl;
    }

    public String getReturnUrl() {
        return returnUrl;
    }

    public void setReturnUrl(String returnUrl) {
        this.returnUrl = returnUrl;
    }

    @Deprecated
    @ConvertDisabled
    public short getPayway() {
        return payWay;
    }

    @Deprecated
    @ConvertDisabled
    public void setPayway(short payWay) {
        this.payWay = payWay;
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

    @Deprecated
    @ConvertDisabled
    public String getNotifyurl() {
        return notifyUrl;
    }

    @Deprecated
    @ConvertDisabled
    public void setNotifyurl(String notifyUrl) {
        this.notifyUrl = notifyUrl;
    }

    @Deprecated
    @ConvertDisabled
    public String getClienthost() {
        return clientHost;
    }

    @Deprecated
    @ConvertDisabled
    public void setClienthost(String clientHost) {
        this.clientHost = clientHost;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getClientHost() {
        return clientHost;
    }

    public void setClientHost(String clientHost) {
        this.clientHost = clientHost;
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
    public String attach(String name) {
        return attach == null ? null : attach.get(name);
    }

    @Deprecated
    public String attach(String name, String defValue) {
        return attach == null ? defValue : attach.getOrDefault(name, defValue);
    }

    @Deprecated
    public String getMapValue(String name) {
        return attach == null ? null : attach.get(name);
    }

    @Deprecated
    public String getMapValue(String name, String defValue) {
        return attach == null ? defValue : attach.getOrDefault(name, defValue);
    }

    @Deprecated
    public Map<String, String> getMap() {
        return attach;
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
    public void setMap(Map<String, String> map) {
        this.attach = map;
    }
}
