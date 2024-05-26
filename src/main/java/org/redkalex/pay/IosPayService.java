/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import static org.redkalex.pay.PayRetCodes.RETPAY_PAY_ERROR;
import static org.redkalex.pay.Pays.PAYTYPE_IOS;

import java.io.*;
import java.net.http.HttpClient;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import org.redkale.annotation.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Comment;
import org.redkale.annotation.ResourceChanged;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceEvent;
import org.redkale.service.Local;
import org.redkale.util.*;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public final class IosPayService extends AbstractPayService {

    protected static final String SANDBOX = "https://sandbox.itunes.apple.com/verifyReceipt";

    protected static final String APPSTORE = "https://buy.itunes.apple.com/verifyReceipt";

    protected static final String format = "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS"; // yyyy-MM-dd HH:mm:ss

    // 原始的配置
    protected Properties elementProps = new Properties();

    // 配置对象集合
    protected Map<String, IosElement> elements = new HashMap<>();

    @Resource
    @Comment("必须存在全局配置项，@ResourceListener才会起作用")
    protected Environment environment;

    @Resource(name = "pay.ios.conf", required = false) // 支付配置文件路径
    protected String conf = "config.properties";

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource
    protected JsonConvert convert;

    protected HttpClient client;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) {
            this.convert = JsonConvert.root();
        }
        this.reloadConfig(Pays.PAYTYPE_IOS);
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short payType) {
        return payType == PAYTYPE_IOS && !elements.isEmpty();
    }

    @Override
    @Comment("重新加载本地文件配置")
    public void reloadConfig(short payType) {
        if (client == null) {
            this.client = HttpClient.newHttpClient();
        }
        Properties properties = new Properties();
        if (this.conf != null && !this.conf.isEmpty()) { // 存在Ios支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0)
                        ? new File(this.conf)
                        : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead())
                        ? new FileInputStream(file)
                        : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in != null) {
                    properties.load(in);
                    in.close();
                }
                this.elements = IosElement.create(logger, properties);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init ios conf error", e);
            }
        }
        this.environment.forEach(k -> k.startsWith("pay.ios."), (k, v) -> properties.put(k, v));
        this.elements = IosElement.create(logger, properties);
        this.elementProps = properties;
    }

    @ResourceChanged //     //
    @Comment("通过配置中心更改配置后的回调")
    void onResourceChanged(ResourceEvent[] events) {
        Properties changeProps = new Properties();
        changeProps.putAll(this.elementProps);
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            if (event.name().startsWith("pay.ios.")) {
                changeProps.put(event.name(), event.newValue().toString());
                sb.append("@Resource change '")
                        .append(event.name())
                        .append("' to '")
                        .append(event.coverNewValue())
                        .append("'\r\n");
            }
        }
        if (sb.length() < 1) {
            return; // 无相关配置变化
        }
        logger.log(Level.INFO, sb.toString());
        this.elements = IosElement.create(logger, changeProps);
        this.elementProps = changeProps;
    }

    public void setPayElements(Map<String, IosElement> elements) {
        this.elements = elements;
    }

    public void putPayElements(Map<String, IosElement> elements) {
        this.elements.putAll(elements);
    }

    @Override
    public IosElement getPayElement(String appid) {
        return this.elements.get(appid);
    }

    public void setPayElement(String appid, IosElement element) {
        this.elements.put(appid, element);
    }

    public boolean existsPayElement(String appid) {
        return this.elements != null && this.elements.containsKey(appid);
    }

    @Override
    public CompletableFuture<PayPreResponse> prepayAsync(PayPreRequest request) {
        request.checkVaild();
        final PayPreResponse result = new PayPreResponse();
        //        result.setAppid("");
        //        final Map<String, String> rmap = new TreeMap<>();
        //        rmap.put("content", request.getPayno());
        //        result.setResult(rmap);
        return result.toFuture();
    }

    @Override
    public CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request) {
        String beanstr;
        if (request.getAttach() != null && request.getAttach().containsKey("bean")) {
            beanstr = request.attach("bean");
        } else {
            beanstr = request.getBody();
        }
        if (beanstr != null && !beanstr.isEmpty()) {
            IosNotifyBean bean = JsonConvert.root().convertFrom(IosNotifyBean.class, beanstr);
            PayNotifyResponse resp = new PayNotifyResponse();
            resp.setResponseText(beanstr);
            resp.setPayno(bean.payno());
            resp.setPayType(request.getPayType());
            return verifyRequest(APPSTORE, bean.receiptData, request, resp);
        }
        // 没有获取参数
        PayNotifyResponse resp = new PayNotifyResponse();
        resp.setPayType(request.getPayType());
        resp.setRetcode(RETPAY_PAY_ERROR);
        return CompletableFuture.completedFuture(resp);
    }

    protected CompletableFuture<PayNotifyResponse> verifyRequest(
            String url, String receiptData, PayNotifyRequest request, PayNotifyResponse resp) {
        return postHttpContentAsync(client, APPSTORE, "{\"receipt-data\" : \"" + receiptData + "\"}")
                .thenCompose(responseText -> {
                    resp.setResponseText(responseText);
                    try {
                        final Map<String, String> resultmap =
                                JsonConvert.root().convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, responseText);
                        resp.setResult(resultmap);
                        if ("21007".equals(resultmap.get("status"))) {
                            return verifyRequest(SANDBOX, receiptData, request, resp);
                        }
                        if (!"0".equals(resultmap.get("status"))) {
                            return resp.retcode(RETPAY_PAY_ERROR).toFuture();
                        }
                        resp.setPayedMoney(0);
                    } catch (Throwable t) {
                        logger.log(Level.SEVERE, "verifyRequest " + request + " error", t);
                        return resp.retcode(RETPAY_PAY_ERROR).toFuture();
                    }
                    return resp.toFuture();
                });
    }

    @Override
    public CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayQueryResponse> queryAsync(PayRequest request) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<PayResponse> closeAsync(PayCloseRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayRefundResponse> refundAsync(PayRefundRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayRefundResponse> queryRefundAsync(PayRefundQryReq request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public PayPreResponse prepay(final PayPreRequest request) {
        return prepayAsync(request).join();
    }

    @Override
    public PayNotifyResponse notify(PayNotifyRequest request) {
        return notifyAsync(request).join();
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        return createAsync(request).join();
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        return queryAsync(request).join();
    }

    @Override
    public PayResponse close(PayCloseRequest request) {
        return closeAsync(request).join();
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        return refundAsync(request).join();
    }

    @Override
    public PayRefundResponse queryRefund(PayRefundQryReq request) {
        return queryRefundAsync(request).join();
    }

    @Override
    protected String createSign(AbstractPayService.PayElement element, Map<String, ?> map, String text) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected boolean checkSign(
            AbstractPayService.PayElement element,
            Map<String, ?> map,
            String text,
            Map<String, Serializable> respHeaders) {
        return true;
    }

    public static class IosNotifyBean implements Serializable {

        public String payno; // payno

        public String receiptData;

        public String receiptData() {
            return receiptData;
        }

        public String payno() {
            return payno;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class IosElement extends AbstractPayService.PayElement {

        // pay.ios.[x].appid
        public String appid = ""; // APP应用ID

        // pay.ios.[x].secret
        public String secret = ""; // 签名算法需要用到的密钥

        public static Map<String, IosElement> create(Logger logger, Properties properties) {
            String def_appid = properties.getProperty("pay.ios.appid", "").trim();
            String def_secret = properties.getProperty("pay.ios.secret", "").trim();
            String def_notifyurl =
                    properties.getProperty("pay.ios.notifyurl", "").trim();

            final Map<String, IosElement> map = new HashMap<>();
            properties.keySet().stream()
                    .filter(x ->
                            x.toString().startsWith("pay.ios.") && x.toString().endsWith(".appid"))
                    .forEach(appid_key -> {
                        final String prefix = appid_key
                                .toString()
                                .substring(0, appid_key.toString().length() - ".appid".length());

                        String appid = properties
                                .getProperty(prefix + ".appid", def_appid)
                                .trim();
                        String secret = properties
                                .getProperty(prefix + ".secret", def_secret)
                                .trim();
                        String notifyurl = properties
                                .getProperty(prefix + ".notifyurl", def_notifyurl)
                                .trim();

                        if (appid.isEmpty()) {
                            logger.log(Level.WARNING, properties + "; has illegal ios conf by prefix" + prefix);
                            return;
                        }
                        IosElement element = new IosElement();
                        element.appid = appid;
                        element.secret = secret;
                        element.notifyurl = notifyurl;
                        if (element.initElement(logger, null)) {
                            map.put(appid, element);
                            if (def_appid.equals(appid)) {
                                map.put("", element);
                            }
                        }
                    });
            return map;
        }

        @Override
        public boolean initElement(Logger logger, File home) {
            return true;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }
}
