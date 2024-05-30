/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;
import org.redkale.convert.ConvertDisabled;
import org.redkale.util.Copier;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayCreatResponse extends PayResponse {

    private static final Copier<PayPreResponse, PayCreatResponse> copier =
            Copier.create(PayPreResponse.class, PayCreatResponse.class);

    private String thirdPayno = ""; // 第三方的支付流水号

    public PayCreatResponse() {}

    public PayCreatResponse(PayPreResponse resp) {
        copier.apply(resp, this);
    }

    @Override
    public PayCreatResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayCreatResponse retinfo(String retinfo) {
        if (retinfo != null) {
            this.retinfo = retinfo;
        }
        return this;
    }

    @Override
    public PayCreatResponse result(Map<String, String> result) {
        this.setResult(result);
        return this;
    }

    public String getThirdPayno() {
        return thirdPayno;
    }

    public void setThirdPayno(String thirdPayno) {
        this.thirdPayno = thirdPayno;
    }

    @Deprecated
    @ConvertDisabled
    public String getThirdpayno() {
        return thirdPayno;
    }

    @Deprecated
    @ConvertDisabled
    public void setThirdpayno(String thirdPayno) {
        this.thirdPayno = thirdPayno;
    }
}
