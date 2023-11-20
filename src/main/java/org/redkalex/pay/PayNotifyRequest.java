/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import org.redkale.convert.ConvertDisabled;
import org.redkale.convert.json.JsonFactory;
import org.redkale.net.http.RestHeaders;
import org.redkale.util.RedkaleException;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayNotifyRequest {

    protected String appid = ""; //APP账号ID

    protected short payType; //支付类型; 

    protected String body;

    protected RestHeaders headers;

    protected Map<String, String> attach;

    public PayNotifyRequest() {
    }

    public PayNotifyRequest(short paytype, String body) {
        this.payType = paytype;
        this.body = body;
    }

    public PayNotifyRequest(short paytype, Map<String, String> attach) {
        this.payType = paytype;
        this.attach = attach;
    }

    public PayNotifyRequest(short paytype) {
        this.payType = paytype;
    }

    public PayNotifyRequest(String appid, short paytype) {
        this.appid = appid;
        this.payType = paytype;
    }

    public PayNotifyRequest headers(RestHeaders headers) {
        this.headers = headers;
        return this;
    }

    public PayNotifyRequest attachByRemoveEmptyValue(Map<String, String> params) {
        final TreeMap<String, String> map = new TreeMap<>(params);
        List<String> emptyKeys = new ArrayList<>();
        for (Map.Entry<String, String> en : map.entrySet()) { //去掉空值的参数
            if (en.getValue().isEmpty()) {
                emptyKeys.add(en.getKey());
            }
        }
        emptyKeys.forEach(x -> map.remove(x));
        this.attach = map;
        return this;
    }

    public void checkVaild() {
        if (this.payType < 1) {
            throw new RedkaleException("payType is illegal");
        }
        if (this.payType == Pays.PAYTYPE_ALIPAY && (this.appid == null || this.appid.isEmpty())) {
            throw new RedkaleException("appid is illegal");
        }
        if ((this.body == null || this.body.isEmpty()) && (this.attach == null || this.attach.isEmpty())) {
            throw new RedkaleException("text and attach both is empty");
        }
    }

    public PayNotifyRequest addAttach(String key, Object value) {
        if (this.attach == null) {
            this.attach = new TreeMap<>();
        }
        this.attach.put(key, String.valueOf(value));
        return this;
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

    public short getPayType() {
        return payType;
    }

    public void setPayType(short payType) {
        this.payType = payType;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public Map<String, String> getAttach() {
        return attach;
    }

    public void setAttach(Map<String, String> attach) {
        this.attach = attach;
    }

    public RestHeaders getHeaders() {
        return headers;
    }

    public void setHeaders(RestHeaders headers) {
        this.headers = headers;
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }

    @Deprecated
    @ConvertDisabled
    public short getPaytype() {
        return payType;
    }

    @Deprecated
    @ConvertDisabled
    public void setPaytype(short payType) {
        this.payType = payType;
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
}
