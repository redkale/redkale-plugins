/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.redkale.annotation.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Comment;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceEvent;
import org.redkale.service.Local;
import org.redkale.util.*;
import static org.redkalex.pay.PayRetCodes.*;
import static org.redkalex.pay.Pays.PAYTYPE_FACEBOOK;
import org.redkale.annotation.ResourceChanged;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public final class FacebookPayService extends AbstractPayService {

    protected static final String format = "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS"; //yyyy-MM-dd HH:mm:ss

    //原始的配置
    protected Properties elementProps = new Properties();

    //配置对象集合
    protected Map<String, FacebookElement> elements = new HashMap<>();

    @Resource
    @Comment("必须存在全局配置项，@ResourceListener才会起作用")
    protected Environment environment;

    @Resource(name = "pay.facebook.conf", required = false) //支付配置文件路径
    protected String conf = "config.properties";

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource
    protected JsonConvert convert;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) {
            this.convert = JsonConvert.root();
        }
        this.reloadConfig(Pays.PAYTYPE_FACEBOOK);
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short payType) {
        return payType == PAYTYPE_FACEBOOK && !elements.isEmpty();
    }

    @Override
    @Comment("重新加载本地文件配置")
    public void reloadConfig(short payType) {
        Properties properties = new Properties();
        if (this.conf != null && !this.conf.isEmpty()) { //存在Facebook支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in != null) {
                    properties.load(in);
                    in.close();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init facebook conf error", e);
            }
        }
        this.environment.forEach(k -> k.startsWith("pay.facebook."), (k, v) -> properties.put(k, v));
        this.elements = FacebookElement.create(logger, properties);
        this.elementProps = properties;
    }

    @ResourceChanged //     //    
    @Comment("通过配置中心更改配置后的回调")
    void onResourceChanged(ResourceEvent[] events) {
        Properties changeProps = new Properties();
        changeProps.putAll(this.elementProps);
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            if (event.name().startsWith("pay.facebook.")) {
                changeProps.put(event.name(), event.newValue().toString());
                sb.append("@Resource change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
            }
        }
        if (sb.length() < 1) {
            return; //无相关配置变化
        }
        logger.log(Level.INFO, sb.toString());
        this.elements = FacebookElement.create(logger, changeProps);
        this.elementProps = changeProps;
    }

    public void setPayElements(Map<String, FacebookElement> elements) {
        this.elements = elements;
    }

    public void putPayElements(Map<String, FacebookElement> elements) {
        this.elements.putAll(elements);
    }

    @Override
    public FacebookElement getPayElement(String appid) {
        return this.elements.get(appid);
    }

    public void setPayElement(String appid, FacebookElement element) {
        this.elements.put(appid, element);
    }

    public boolean existsPayElement(String appid) {
        return this.elements != null && this.elements.containsKey(appid);
    }

    @Override
    public CompletableFuture<PayPreResponse> prepayAsync(PayPreRequest request) {
        request.checkVaild();
        final PayPreResponse result = new PayPreResponse();
        final FacebookElement element = elements.get(request.getAppid());
        if (element == null) {
            return result.retcode(RETPAY_CONF_ERROR).toFuture();
        }
        result.setAppid(element.appid);
        final Map<String, String> rmap = new TreeMap<>();
        rmap.put("content", request.getPayno());
        result.setResult(rmap);
        return result.toFuture();
    }

    @Override
    public CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request) {
        if (request.getAttach() != null && request.getAttach().containsKey("bean")) {
            final FacebookElement element = elements.get(request.getAppid());
            String strbean = request.attach("bean");
            NotifyBean bean = JsonConvert.root().convertFrom(NotifyBean.class, strbean);
            PayNotifyResponse resp = new PayNotifyResponse();
            resp.setResponseText(strbean);
            resp.setPayno(bean.payno());
            resp.setPayType(request.getPayType());

            String[] signatures = bean.signature().split("\\.");
            byte[] sig = Base64.getDecoder().decode(signatures[0].replace('-', '+').replace('_', '/'));
            Map<String, String> map = JsonConvert.root().convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, Base64.getDecoder().decode(signatures[1]));
            if (!"HMAC-SHA256".equals(map.get("algorithm"))) {
                return CompletableFuture.completedFuture(resp.retcode(RETPAY_FALSIFY_ERROR));
            }
            byte[] keyBytes = element.secret.getBytes();
            SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA256");
            Mac mac;
            try {
                mac = Mac.getInstance("HmacSHA256");
                mac.init(signingKey);
            } catch (Exception e) {
                return CompletableFuture.completedFuture(resp.retcode(RETPAY_FALSIFY_ERROR));
            }
            byte[] rawHmac = mac.doFinal(signatures[1].getBytes());
            if (!Arrays.equals(sig, rawHmac)) {
                return CompletableFuture.completedFuture(resp.retcode(RETPAY_FALSIFY_ERROR));
            }
            if (map.containsKey("amount")) {
                resp.setPayedMoney((long) (Float.parseFloat(map.get("amount")) * 100));
            }
            return CompletableFuture.completedFuture(resp);
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
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
    protected String createSign(PayElement element, Map<String, ?> map, String text) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected boolean checkSign(PayElement element, Map<String, ?> map, String text, Map<String, Serializable> respHeaders) {
        String[] signatures = text.split("\\.");
        byte[] sig = Base64.getDecoder().decode(signatures[0].replace('-', '+').replace('_', '/'));
        Map<String, String> smp = JsonConvert.root().convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, Base64.getDecoder().decode(signatures[1]));
        if (!"HMAC-SHA256".equals(smp.get("algorithm"))) {
            return false;
        }
        byte[] keyBytes = ((FacebookElement) element).secret.getBytes();
        SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA256");
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(signingKey);
            byte[] rawHmac = mac.doFinal(signatures[1].getBytes());
            return Arrays.equals(sig, rawHmac);
        } catch (Exception e) {
            return false;
        }
    }

    public static class NotifyBean implements Serializable {

        // ----------- 第一种 -----------
        public String developerPayload;//payno

        public String paymentID;

        public String productID;

        public String purchaseTime;

        public String purchaseToken;

        public String signedRequest;

        // ----------- 第二种 -----------
        public String payment_id;

        public String amount; //5.00

        public String currency; //USD

        public String quantity; //1

        public String request_id; //payno

        public String status;

        public String signed_request;

        public String signature() {
            return signed_request == null || signed_request.isEmpty() ? signedRequest : signed_request;
        }

        public String payno() {
            return request_id == null || request_id.isEmpty() ? developerPayload : request_id;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class FacebookElement extends PayElement {

        // pay.facebook.[x].appid
        public String appid = "";  //APP应用ID

        // pay.facebook.[x].secret
        public String secret = ""; //签名算法需要用到的密钥

        public static Map<String, FacebookElement> create(Logger logger, Properties properties) {
            String def_appid = properties.getProperty("pay.facebook.appid", "").trim();
            String def_secret = properties.getProperty("pay.facebook.secret", "").trim();
            String def_notifyurl = properties.getProperty("pay.facebook.notifyurl", "").trim();

            final Map<String, FacebookElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("pay.facebook.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String secret = properties.getProperty(prefix + ".secret", def_secret).trim();
                String notifyurl = properties.getProperty(prefix + ".notifyurl", def_notifyurl).trim();

                if (appid.isEmpty() || notifyurl.isEmpty() || secret.isEmpty()) {
                    logger.log(Level.WARNING, properties + "; has illegal facebook conf by prefix" + prefix);
                    return;
                }
                FacebookElement element = new FacebookElement();
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
