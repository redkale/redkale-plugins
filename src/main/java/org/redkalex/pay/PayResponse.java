/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.redkale.convert.*;
import org.redkale.convert.json.JsonFactory;
import org.redkale.service.RetResult;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class PayResponse extends RetResult<Map<String, String>> {

    @ConvertColumn(ignore = true, type = ConvertType.JSON)
    protected String responseText = ""; // 第三方支付返回的原始结果字符串

    public PayResponse() {}

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

    @Override
    public CompletableFuture toFuture() {
        return CompletableFuture.completedFuture(this);
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

    public String getResponseText() {
        return responseText;
    }

    public void setResponseText(String responseText) {
        this.responseText = responseText;
    }

    @Override
    public String toString() {
        return jf.getConvert().convertTo(this);
    }

    private static final JsonFactory jf = JsonFactory.create().skipAllIgnore(true);

    @Deprecated
    @ConvertDisabled
    public String getResponsetext() {
        return responseText;
    }

    @Deprecated
    @ConvertDisabled
    public void setResponsetext(String responseText) {
        this.responseText = responseText;
    }
}
