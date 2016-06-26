/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.convert.json.JsonFactory;

/**
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
public class PayRequest {

    protected short paytype; //支付类型; 

    protected String tradeno = ""; //自己的订单号

    public void checkVaild() {
        if (this.tradeno == null || this.tradeno.isEmpty()) throw new RuntimeException("tradeno is illegal");
        if (this.paytype < 1) throw new RuntimeException("paytype is illegal");
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }

    public short getPaytype() {
        return paytype;
    }

    public void setPaytype(short paytype) {
        this.paytype = paytype;
    }

    public String getTradeno() {
        return tradeno;
    }

    public void setTradeno(String tradeno) {
        this.tradeno = tradeno;
    }

}
