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
public class PayCreatResponse extends PayResponse {

    private String thirdpayno = ""; //第三方的支付流水号

    @Override
    public PayCreatResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayCreatResponse retinfo(String retinfo) {
        if (retinfo != null) this.retinfo = retinfo;
        return this;
    }

    @Override
    public PayCreatResponse result(Map<String, String> result) {
        this.setResult(result);
        return this;
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }

}
