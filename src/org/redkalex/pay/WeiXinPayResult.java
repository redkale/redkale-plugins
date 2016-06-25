/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import org.redkale.convert.json.JsonFactory;
import org.redkale.service.RetResult;

/**
 *
 * @see http://redkale.org
 * @author zhangjx
 */
public class WeiXinPayResult extends RetResult<String> {


    private long orderid;

    private long payid;

    private long payedmoney;

    private short paystatus;

    public WeiXinPayResult() {
    }

    public WeiXinPayResult(int retcode) {
        super(retcode);
    }

    public WeiXinPayResult(long orderid, long payid, short paystatus, long payedmoney, String resultcontent) {
        this.orderid = orderid;
        this.payid = payid;
        this.paystatus = paystatus;
        this.payedmoney = payedmoney;
        this.setResult(resultcontent);
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }

    public long getOrderid() {
        return orderid;
    }

    public void setOrderid(long orderid) {
        this.orderid = orderid;
    }

    public long getPayid() {
        return payid;
    }

    public void setPayid(long payid) {
        this.payid = payid;
    }

    public long getPayedmoney() {
        return payedmoney;
    }

    public void setPayedmoney(long payedmoney) {
        this.payedmoney = payedmoney;
    }

    public short getPaystatus() {
        return paystatus;
    }

    public void setPaystatus(short paystatus) {
        this.paystatus = paystatus;
    }

}
