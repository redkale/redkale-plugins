/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

/**
 *
 * @author zhangjx
 */
public class PayQueryRequest extends PayRequest {

    protected short payway;  //支付途径; APP WEB NATIVE

    public PayQueryRequest() {
    }

    public PayQueryRequest(short paytype, short payway, String payno) {
        super(paytype, payno);
        this.payway = payway;
    }

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.payway < 1) throw new RuntimeException("payway is illegal");
    }

    public short getPayway() {
        return payway;
    }

    public void setPayway(short payway) {
        this.payway = payway;
    }

}
