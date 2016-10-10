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
public class PayRefundQueryResponse extends PayResponse {

    @Override
    public PayRefundQueryResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayRefundQueryResponse retinfo(String retinfo) {
        if (retinfo != null) this.retinfo = retinfo;
        return this;
    }

    @Override
    public PayRefundQueryResponse result(Map<String, String> result) {
        this.setResult(result);
        return this;
    }
}
