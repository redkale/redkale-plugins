/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import org.redkale.convert.json.JsonFactory;
import org.redkale.service.RetResult;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayNotifyResponse extends RetResult<String> {

    protected short paytype; //支付类型; 

    protected String payno = ""; //自己的订单号

    protected String thirdpayno = ""; //第三方的支付流水号

    protected Map<String, String> map; //扩展信息

    @Override
    public PayNotifyResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayNotifyResponse retinfo(String retinfo) {
        if (retinfo != null) this.retinfo = retinfo;
        return this;
    }

    @Override
    public PayNotifyResponse result(String result) {
        this.setResult(result);
        return this;
    }

    public Map<String, String> add(String key, String value) {
        if (this.map == null) this.map = new TreeMap<>();
        this.map.put(key, value);
        return this.map;
    }

    public String getMapValue(String name) {
        return map == null ? null : map.get(name);
    }

    public String getMapValue(String name, String defValue) {
        return map == null ? defValue : map.getOrDefault(name, defValue);
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }

    public short getPaytype() {
        return paytype;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }

    public void setPaytype(short paytype) {
        this.paytype = paytype;
    }

    public String getPayno() {
        return payno;
    }

    public void setPayno(String payno) {
        this.payno = payno;
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }

}
