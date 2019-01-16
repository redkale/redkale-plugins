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
public class PayCreatRequest extends PayRequest {

    protected long paymoney; //  支付金额。 单位:分 

    protected String paytitle = ""; //订单标题

    protected String paybody = ""; //订单内容描述

    protected int paytimeout = 600; //支付超时的秒数

    protected String clientAddr = "";  //客户端IP地址

    protected Map<String, String> attach; //扩展信息

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.paymoney < 1) throw new RuntimeException("paymoney is illegal");
        if (this.paytitle == null || this.paytitle.isEmpty()) throw new RuntimeException("paytitle is illegal");
        if (this.paybody == null || this.paybody.isEmpty()) throw new RuntimeException("paybody is illegal");
        if (this.clientAddr == null || this.clientAddr.isEmpty()) throw new RuntimeException("clientAddr is illegal");
        if (this.paytimeout < 300) throw new RuntimeException("paytimeout cannot less 300 seconds");
        if (this.paytimeout > 24 * 60 * 60) throw new RuntimeException("paytimeout cannot greater 1 day");
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

    public long getPaymoney() {
        return paymoney;
    }

    public void setPaymoney(long paymoney) {
        this.paymoney = paymoney;
    }

    public String getPaytitle() {
        return paytitle;
    }

    public void setPaytitle(String paytitle) {
        this.paytitle = paytitle;
    }

    public String getPaybody() {
        return paybody;
    }

    public void setPaybody(String paybody) {
        this.paybody = paybody;
    }

    public int getPaytimeout() {
        return paytimeout;
    }

    public void setPaytimeout(int paytimeout) {
        this.paytimeout = paytimeout;
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
        if (this.attach == null) this.attach = new TreeMap<>();
        this.attach.put(key, value);
        return this.attach;
    }

    @Deprecated
    public Map<String, String> getMap() {
        return attach;
    }

    @Deprecated
    public void setMap(Map<String, String> map) {
        this.attach = map;
    }
}
