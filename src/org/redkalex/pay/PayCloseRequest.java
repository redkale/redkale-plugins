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
public class PayCloseRequest extends PayRequest {

    protected String thirdpayno = ""; //第三方的支付流水号
    
    protected long trademoney; //  支付金额。 单位:分 

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.trademoney < 1) throw new RuntimeException("trademoney is illegal");
        if (this.thirdpayno == null || this.thirdpayno.isEmpty()) throw new RuntimeException("thirdpayno is illegal");
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }

    public long getTrademoney() {
        return trademoney;
    }

    public void setTrademoney(long trademoney) {
        this.trademoney = trademoney;
    }

}
