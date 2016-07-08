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
 * @author zhangjx
 */
public class PayNotifyRequest {

    protected short paytype; //支付类型; 

    private String text;

    private Map<String, String> map;

    public void checkVaild() {
        if ((this.text == null || this.text.isEmpty() && (map == null || map.isEmpty()))) throw new RuntimeException("text and map both is empty");
        if (this.paytype < 1) throw new RuntimeException("paytype is illegal");
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
