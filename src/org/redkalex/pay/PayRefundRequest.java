/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

/**
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
public class PayRefundRequest extends PayRequest {


    protected long trademoney; //  支付金额。 单位:分 

    protected String thirdpayno = ""; //第三方的支付流水号
    
    protected long refundmoney; //  退款金额。 单位:分  不能大于支付金额
    
    //微信支付: 商户系统内部唯一，同一退款单号多次请求只退一笔
    //支付宝： 标识一次退款请求，同一笔交易多次退款需要保证唯一，如需部分退款，则此参数必传
    protected String refundno = ""; //退款编号 商户系统内部的退款单号。

    @Override
    public void checkVaild() {
        super.checkVaild();
        if (this.refundmoney < 1) throw new RuntimeException("refundmoney is illegal");
        if (this.trademoney < 1) throw new RuntimeException("trademoney is illegal");
        if (this.refundno == null || this.refundno.isEmpty()) throw new RuntimeException("refundno is illegal");
        if (this.thirdpayno == null || this.thirdpayno.isEmpty()) throw new RuntimeException("thirdpayno is illegal");
    }

    public long getRefundmoney() {
        return refundmoney;
    }

    public void setRefundmoney(long refundmoney) {
        this.refundmoney = refundmoney;
    }

    public long getTrademoney() {
        return trademoney;
    }

    public void setTrademoney(long trademoney) {
        this.trademoney = trademoney;
    }

    public String getRefundno() {
        return refundno;
    }

    public void setRefundno(String refundno) {
        this.refundno = refundno;
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }

}
