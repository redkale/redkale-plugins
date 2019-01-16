/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import org.redkale.convert.json.JsonFactory;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayNotifyRequest {

    protected String appid = ""; //APP账号ID

    protected short paytype; //支付类型; 

    protected String text;

    protected Map<String, String> attach;

    public PayNotifyRequest() {
    }

    public PayNotifyRequest(short paytype, String text) {
        this.paytype = paytype;
        this.text = text;
    }

    public PayNotifyRequest(short paytype, Map<String, String> attach) {
        this.paytype = paytype;
        this.attach = attach;
    }

    public void checkVaild() {
        if (this.paytype < 1) throw new RuntimeException("paytype is illegal");
        if (this.paytype == Pays.PAYTYPE_ALIPAY && (this.appid == null || this.appid.isEmpty())) throw new RuntimeException("appid is illegal");
        if ((this.text == null || this.text.isEmpty()) && (this.attach == null || this.attach.isEmpty())) throw new RuntimeException("text and attach both is empty");
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

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Map<String, String> getAttach() {
        return attach;
    }

    public void setAttach(Map<String, String> attach) {
        this.attach = attach;
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
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
