/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;
import org.redkale.convert.*;
import org.redkale.convert.json.JsonFactory;
import org.redkale.service.RetResult;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayResponse extends RetResult<Map<String, String>> {

    @ConvertColumn(ignore = true, type = ConvertType.JSON)
    protected String responsetext = ""; //第三方支付返回的原始结果字符串

    public PayResponse() {
    }

    public PayResponse(Map<String, String> result) {
        super(result);
    }

    public PayResponse(int retcode) {
        super(retcode);
    }

    public PayResponse(int retcode, String retinfo) {
        super(retcode, retinfo);
    }

    public PayResponse(int retcode, String retinfo, Map<String, String> result) {
        super(retcode, retinfo, result);
    }

    public String attach(String name) {
        return attach == null ? null : attach.get(name);
    }

    public String attach(String name, String defValue) {
        return attach == null ? defValue : attach.getOrDefault(name, defValue);
    }

    @Override
    public PayResponse retcode(int retcode) {
        this.retcode = retcode;
        this.retinfo = PayRetCodes.retInfo(retcode);
        return this;
    }

    @Override
    public PayResponse retinfo(String retinfo) {
        if (retinfo != null) this.retinfo = retinfo;
        return this;
    }

    @Override
    public PayResponse result(Map<String, String> result) {
        this.setResult(result);
        return this;
    }

    public String getResponsetext() {
        return responsetext;
    }

    public void setResponsetext(String responsetext) {
        this.responsetext = responsetext;
    }

    @Override
    public String toString() {
        return jf.getConvert().convertTo(this);
    }

    private static final JsonFactory jf = JsonFactory.create().skipAllIgnore(true);
}
