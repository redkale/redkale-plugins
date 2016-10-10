/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

/**
 * 
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayCloseRequest extends PayRequest {

    protected String thirdpayno = ""; //第三方的支付流水号
    
    protected long paymoney; //  支付金额。 单位:分 

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.paymoney < 1) throw new RuntimeException("paymoney is illegal");
        if (this.thirdpayno == null || this.thirdpayno.isEmpty()) throw new RuntimeException("thirdpayno is illegal");
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }

    public long getPaymoney() {
        return paymoney;
    }

    public void setPaymoney(long paymoney) {
        this.paymoney = paymoney;
    }

}
