/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;
import org.redkale.convert.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayPreResponse extends PayResponse {

    @ConvertColumn(ignore = true, type = ConvertType.JSON)
    private String appid = "";

    private String thirdpayno = ""; //第三方的支付流水号

    @Override
    public PayPreResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayPreResponse retinfo(String retinfo) {
        if (retinfo != null) this.retinfo = retinfo;
        return this;
    }

    @Override
    public PayPreResponse result(Map<String, String> result) {
        this.setResult(result);
        return this;
    }

    public String getThirdpayno() {
        return thirdpayno;
    }

    public void setThirdpayno(String thirdpayno) {
        this.thirdpayno = thirdpayno;
    }

    public String getAppid() {
        return appid == null ? "" : appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

}
