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

    protected Map<String, String> map;

    public PayNotifyRequest() {
    }

    public PayNotifyRequest(short paytype, String text) {
        this.paytype = paytype;
        this.text = text;
    }

    public PayNotifyRequest(short paytype, Map<String, String> map) {
        this.paytype = paytype;
        this.map = map;
    }

    public void checkVaild() {
        if (this.paytype < 1) throw new RuntimeException("paytype is illegal");
        if (this.paytype == Pays.PAYTYPE_ALIPAY && (this.appid == null || this.appid.isEmpty())) throw new RuntimeException("appid is illegal");
        if ((this.text == null || this.text.isEmpty()) && (map == null || map.isEmpty())) throw new RuntimeException("text and map both is empty");
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

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }

}
