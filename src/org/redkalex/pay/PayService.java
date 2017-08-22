/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.Map;
import javax.annotation.Resource;
import org.redkale.service.*;
import org.redkale.util.*;
import static org.redkalex.pay.Pays.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public class PayService extends AbstractPayService {

    @Resource
    private UnionPayService unionPayService;

    @Resource
    private WeiXinPayService weiXinPayService;

    @Resource
    private AliPayService aliPayService;

    @Resource
    private EhkingPayService ehkingPayService;

    @Override
    public void init(AnyValue config) {
    }

    @Override
    public PayPreResponse prepay(PayPreRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.prepay(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.prepay(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.prepay(request);
        if (request.paytype == PAYTYPE_EHKING) return ehkingPayService.prepay(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayNotifyResponse notify(PayNotifyRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.notify(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.notify(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.notify(request);
        if (request.paytype == PAYTYPE_EHKING) return ehkingPayService.notify(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.create(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.create(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.create(request);
        if (request.paytype == PAYTYPE_EHKING) return ehkingPayService.create(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.query(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.query(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.query(request);
        if (request.paytype == PAYTYPE_EHKING) return ehkingPayService.query(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayResponse close(PayCloseRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.close(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.close(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.close(request);
        if (request.paytype == PAYTYPE_EHKING) return ehkingPayService.close(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.refund(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.refund(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.refund(request);
        if (request.paytype == PAYTYPE_EHKING) return ehkingPayService.refund(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayRefundResponse queryRefund(PayRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.queryRefund(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.queryRefund(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.queryRefund(request);
        if (request.paytype == PAYTYPE_EHKING) return ehkingPayService.queryRefund(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    protected String createSign(final PayElement element, Map<String, ?> map) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected boolean checkSign(final PayElement element, Map<String, ?> map) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getNotifyurl(short paytype, String appid) {
        PayElement element = null;
        if (paytype == PAYTYPE_UNION) {
            element = unionPayService.getPayElement(appid);
        } else if (paytype == PAYTYPE_WEIXIN) {
            element = weiXinPayService.getPayElement(appid);
        } else if (paytype == PAYTYPE_ALIPAY) {
            element = aliPayService.getPayElement(appid);
        } else if (paytype == PAYTYPE_EHKING) {
            element = ehkingPayService.getPayElement(appid);
        } else {
            throw new RuntimeException("paytype = " + paytype + " is illegal");
        }
        return element == null ? "" : element.notifyurl;
    }

    public UnionPayService getUnionPayService() {
        return unionPayService;
    }

    public WeiXinPayService getWeiXinPayService() {
        return weiXinPayService;
    }

    public AliPayService getAliPayService() {
        return aliPayService;
    }

    public EhkingPayService getEhkingPayService() {
        return ehkingPayService;
    }
}
