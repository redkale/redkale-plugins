/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayRefundResponse extends PayResponse {

    protected long refundedmoney; //  已退款金额。 单位:分

    @Override
    public PayRefundResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayRefundResponse retinfo(String retinfo) {
        if (retinfo != null) this.retinfo = retinfo;
        return this;
    }

    @Override
    public PayRefundResponse result(Map<String, String> result) {
        this.setResult(result);
        return this;
    }
    
    public long getRefundedmoney() {
        return refundedmoney;
    }

    public void setRefundedmoney(long refundedmoney) {
        this.refundedmoney = refundedmoney;
    }

}
