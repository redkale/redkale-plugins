/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.*;
import java.util.concurrent.CompletableFuture;
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
public class MultiPayService extends AbstractPayService {

    @Resource
    private UnionPayService unionPayService;

    @Resource
    private WeiXinPayService weiXinPayService;

    @Resource
    private AliPayService aliPayService;

    @Resource
    private ResourceFactory resourceFactory;

    private Map<Short, AbstractPayService> diyPayServiceMap = new HashMap<>();

    @Override
    public void init(AnyValue config) {
        List<AbstractPayService> services = resourceFactory.query((name, service) -> {
            if (!(service instanceof AbstractPayService)) return false;
            DIYPayService diy = service.getClass().getAnnotation(DIYPayService.class);
            if (diy == null) return false;
            if (diy.paytype() < Pays.MIN_DIY_PAYTYPE) throw new RuntimeException("DIYPayService.paytype must be greater than " + Pays.MIN_DIY_PAYTYPE);
            return true;
        });
        for (AbstractPayService service : services) {
            DIYPayService diy = service.getClass().getAnnotation(DIYPayService.class);
            diyPayServiceMap.put(diy.paytype(), service);
        }
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short paytype) {
        if (paytype == PAYTYPE_UNION) return unionPayService.supportPayType(paytype);
        if (paytype == PAYTYPE_WEIXIN) return weiXinPayService.supportPayType(paytype);
        if (paytype == PAYTYPE_ALIPAY) return aliPayService.supportPayType(paytype);
        AbstractPayService service = diyPayServiceMap.get(paytype);
        if (service == null) return false;
        return service.supportPayType(paytype);
    }

    @Override
    @Comment("重新加载配置")
    public void reloadConfig(short paytype) {
        if (paytype == PAYTYPE_UNION) {
            unionPayService.reloadConfig(paytype);
        } else if (paytype == PAYTYPE_WEIXIN) {
            weiXinPayService.reloadConfig(paytype);
        } else if (paytype == PAYTYPE_ALIPAY) {
            aliPayService.reloadConfig(paytype);
        } else {
            AbstractPayService service = diyPayServiceMap.get(paytype);
            if (service != null) service.reloadConfig(paytype);
        }
    }

    @Override
    public PayPreResponse prepay(PayPreRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.prepay(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.prepay(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.prepay(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.prepay(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public CompletableFuture<PayPreResponse> prepayAsync(PayPreRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.prepayAsync(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.prepayAsync(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.prepayAsync(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.prepayAsync(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayNotifyResponse notify(PayNotifyRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.notify(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.notify(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.notify(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.notify(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.notifyAsync(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.notifyAsync(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.notifyAsync(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.notifyAsync(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.create(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.create(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.create(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.create(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.createAsync(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.createAsync(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.createAsync(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.createAsync(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.query(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.query(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.query(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.query(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public CompletableFuture<PayQueryResponse> queryAsync(PayRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.queryAsync(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.queryAsync(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.queryAsync(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.queryAsync(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayResponse close(PayCloseRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.close(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.close(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.close(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.close(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public CompletableFuture<PayResponse> closeAsync(PayCloseRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.closeAsync(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.closeAsync(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.closeAsync(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.closeAsync(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.refund(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.refund(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.refund(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.refund(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public CompletableFuture<PayRefundResponse> refundAsync(PayRefundRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.refundAsync(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.refundAsync(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.refundAsync(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.refundAsync(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public PayRefundResponse queryRefund(PayRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.queryRefund(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.queryRefund(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.queryRefund(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.queryRefund(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    public CompletableFuture<PayRefundResponse> queryRefundAsync(PayRequest request) {
        if (request.paytype == PAYTYPE_UNION) return unionPayService.queryRefundAsync(request);
        if (request.paytype == PAYTYPE_WEIXIN) return weiXinPayService.queryRefundAsync(request);
        if (request.paytype == PAYTYPE_ALIPAY) return aliPayService.queryRefundAsync(request);
        AbstractPayService diyPayService = diyPayServiceMap.get(request.paytype);
        if (diyPayService != null) return diyPayService.queryRefundAsync(request);
        throw new RuntimeException(request + ".paytype is illegal");
    }

    @Override
    protected String createSign(final PayElement element, Map<String, ?> map) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected boolean checkSign(final PayElement element, Map<String, ?> map) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PayElement getPayElement(String appid) {
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
        } else {
            AbstractPayService diyPayService = diyPayServiceMap.get(paytype);
            if (diyPayService == null) throw new RuntimeException("paytype = " + paytype + " is illegal");
            element = diyPayService.getPayElement(appid);
        }
        return element == null ? "" : element.notifyurl;
    }

    public AbstractPayService getDIYPayService(short paytype) {
        return diyPayServiceMap.get(paytype);
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

}
