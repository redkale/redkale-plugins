/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import static org.redkalex.pay.Pays.*;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.redkale.annotation.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Comment;
import org.redkale.inject.ResourceFactory;
import org.redkale.service.Local;
import org.redkale.util.*;

/**
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
	private FacebookPayService facebookPayService;

	@Resource
	private GooglePayService googlePayService;

	@Resource
	private IosPayService iosPayService;

	@Resource
	private OppoPayService oppoPayService;

	@Resource
	private ResourceFactory resourceFactory;

	private Map<Short, AbstractPayService> diyPayServiceMap = new HashMap<>();

	@Override
	public void init(AnyValue config) {
		List<AbstractPayService> services = resourceFactory.query((name, service) -> {
			if (!(service instanceof AbstractPayService)) return false;
			DIYPayService diy = service.getClass().getAnnotation(DIYPayService.class);
			if (diy == null) return false;
			if (diy.payType() < Pays.MIN_DIY_PAYTYPE)
				throw new RedkaleException("DIYPayService.paytype must be greater than " + Pays.MIN_DIY_PAYTYPE);
			return true;
		});
		for (AbstractPayService service : services) {
			DIYPayService diy = service.getClass().getAnnotation(DIYPayService.class);
			diyPayServiceMap.put(diy.payType(), service);
		}
	}

	@Override
	@Comment("判断是否支持指定支付类型")
	public boolean supportPayType(final short paytype) {
		if (paytype == PAYTYPE_UNION) return unionPayService.supportPayType(paytype);
		if (paytype == PAYTYPE_WEIXIN) return weiXinPayService.supportPayType(paytype);
		if (paytype == PAYTYPE_ALIPAY) return aliPayService.supportPayType(paytype);
		if (paytype == PAYTYPE_FACEBOOK) return facebookPayService.supportPayType(paytype);
		if (paytype == PAYTYPE_GOOGLE) return googlePayService.supportPayType(paytype);
		if (paytype == PAYTYPE_IOS) return iosPayService.supportPayType(paytype);
		if (paytype == PAYTYPE_OPPO) return oppoPayService.supportPayType(paytype);
		AbstractPayService service = diyPayServiceMap.get(paytype);
		if (service == null) return false;
		return service.supportPayType(paytype);
	}

	@Override
	@Comment("重新加载本地文件配置")
	public void reloadConfig(short paytype) {
		if (paytype == PAYTYPE_UNION) {
			unionPayService.reloadConfig(paytype);
		} else if (paytype == PAYTYPE_WEIXIN) {
			weiXinPayService.reloadConfig(paytype);
		} else if (paytype == PAYTYPE_ALIPAY) {
			aliPayService.reloadConfig(paytype);
		} else if (paytype == PAYTYPE_FACEBOOK) {
			facebookPayService.reloadConfig(paytype);
		} else if (paytype == PAYTYPE_GOOGLE) {
			googlePayService.reloadConfig(paytype);
		} else if (paytype == PAYTYPE_IOS) {
			iosPayService.reloadConfig(paytype);
		} else if (paytype == PAYTYPE_OPPO) {
			oppoPayService.reloadConfig(paytype);
		} else {
			AbstractPayService service = diyPayServiceMap.get(paytype);
			if (service != null) service.reloadConfig(paytype);
		}
	}

	@Override
	public PayPreResponse prepay(PayPreRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.prepay(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.prepay(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.prepay(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.prepay(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.prepay(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.prepay(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.prepay(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.prepay(request);
		throw new RedkaleException(request + ".paytype is illegal");
	}

	@Override
	public CompletableFuture<PayPreResponse> prepayAsync(PayPreRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.prepayAsync(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.prepayAsync(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.prepayAsync(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.prepayAsync(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.prepayAsync(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.prepayAsync(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.prepayAsync(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.prepayAsync(request);
		return CompletableFuture.failedFuture(new RuntimeException(request + ".paytype is illegal"));
	}

	@Override
	public PayNotifyResponse notify(PayNotifyRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.notify(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.notify(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.notify(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.notify(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.notify(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.notify(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.notify(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.notify(request);
		throw new RedkaleException(request + ".paytype is illegal");
	}

	@Override
	public CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.notifyAsync(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.notifyAsync(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.notifyAsync(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.notifyAsync(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.notifyAsync(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.notifyAsync(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.notifyAsync(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.notifyAsync(request);
		return CompletableFuture.failedFuture(new RuntimeException(request + ".paytype is illegal"));
	}

	@Override
	public PayCreatResponse create(PayCreatRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.create(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.create(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.create(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.create(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.create(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.create(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.create(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.create(request);
		throw new RedkaleException(request + ".paytype is illegal");
	}

	@Override
	public CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.createAsync(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.createAsync(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.createAsync(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.createAsync(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.createAsync(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.createAsync(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.createAsync(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.createAsync(request);
		return CompletableFuture.failedFuture(new RuntimeException(request + ".paytype is illegal"));
	}

	@Override
	public PayQueryResponse query(PayRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.query(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.query(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.query(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.query(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.query(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.query(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.query(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.query(request);
		throw new RedkaleException(request + ".paytype is illegal");
	}

	@Override
	public CompletableFuture<PayQueryResponse> queryAsync(PayRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.queryAsync(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.queryAsync(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.queryAsync(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.queryAsync(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.queryAsync(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.queryAsync(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.queryAsync(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.queryAsync(request);
		return CompletableFuture.failedFuture(new RuntimeException(request + ".paytype is illegal"));
	}

	@Override
	public PayResponse close(PayCloseRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.close(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.close(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.close(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.close(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.close(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.close(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.close(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.close(request);
		throw new RedkaleException(request + ".paytype is illegal");
	}

	@Override
	public CompletableFuture<PayResponse> closeAsync(PayCloseRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.closeAsync(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.closeAsync(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.closeAsync(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.closeAsync(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.closeAsync(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.closeAsync(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.closeAsync(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.closeAsync(request);
		return CompletableFuture.failedFuture(new RuntimeException(request + ".paytype is illegal"));
	}

	@Override
	public PayRefundResponse refund(PayRefundRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.refund(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.refund(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.refund(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.refund(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.refund(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.refund(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.refund(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.refund(request);
		throw new RedkaleException(request + ".paytype is illegal");
	}

	@Override
	public CompletableFuture<PayRefundResponse> refundAsync(PayRefundRequest request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.refundAsync(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.refundAsync(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.refundAsync(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.refundAsync(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.refundAsync(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.refundAsync(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.refundAsync(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.refundAsync(request);
		return CompletableFuture.failedFuture(new RuntimeException(request + ".paytype is illegal"));
	}

	@Override
	public PayRefundResponse queryRefund(PayRefundQryReq request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.queryRefund(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.queryRefund(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.queryRefund(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.queryRefund(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.queryRefund(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.queryRefund(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.queryRefund(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.queryRefund(request);
		throw new RedkaleException(request + ".paytype is illegal");
	}

	@Override
	public CompletableFuture<PayRefundResponse> queryRefundAsync(PayRefundQryReq request) {
		if (request.payType == PAYTYPE_UNION) return unionPayService.queryRefundAsync(request);
		if (request.payType == PAYTYPE_WEIXIN) return weiXinPayService.queryRefundAsync(request);
		if (request.payType == PAYTYPE_ALIPAY) return aliPayService.queryRefundAsync(request);
		if (request.payType == PAYTYPE_FACEBOOK) return facebookPayService.queryRefundAsync(request);
		if (request.payType == PAYTYPE_GOOGLE) return googlePayService.queryRefundAsync(request);
		if (request.payType == PAYTYPE_IOS) return iosPayService.queryRefundAsync(request);
		if (request.payType == PAYTYPE_OPPO) return oppoPayService.queryRefundAsync(request);
		AbstractPayService diyPayService = diyPayServiceMap.get(request.payType);
		if (diyPayService != null) return diyPayService.queryRefundAsync(request);
		return CompletableFuture.failedFuture(new RuntimeException(request + ".paytype is illegal"));
	}

	@Override
	protected String createSign(final PayElement element, Map<String, ?> map, String text) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	protected boolean checkSign(
			final PayElement element, Map<String, ?> map, String text, Map<String, Serializable> respHeaders) {
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
		} else if (paytype == PAYTYPE_FACEBOOK) {
			element = facebookPayService.getPayElement(appid);
		} else if (paytype == PAYTYPE_GOOGLE) {
			element = googlePayService.getPayElement(appid);
		} else if (paytype == PAYTYPE_IOS) {
			element = iosPayService.getPayElement(appid);
		} else {
			AbstractPayService diyPayService = diyPayServiceMap.get(paytype);
			if (diyPayService == null) throw new RedkaleException("paytype = " + paytype + " is illegal");
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

	public FacebookPayService getFacebookPayService() {
		return facebookPayService;
	}

	public GooglePayService getGooglePayService() {
		return googlePayService;
	}

	public IosPayService getIosPayService() {
		return iosPayService;
	}

	public OppoPayService getOppoPayService() {
		return oppoPayService;
	}
}
