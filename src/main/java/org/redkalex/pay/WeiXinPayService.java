/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.cert.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.regex.Pattern;
import javax.crypto.Cipher;
import javax.crypto.spec.*;
import org.redkale.annotation.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Comment;
import org.redkale.annotation.ResourceListener;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.http.HttpHeaders;
import org.redkale.service.Local;
import org.redkale.util.*;
import static org.redkalex.pay.PayRetCodes.*;
import static org.redkalex.pay.Pays.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public class WeiXinPayService extends AbstractPayService {

    protected static final String format = "%1$tY-%1$tm-%1$tdT%1$tH:%1$tM:%1$tS%1$tz"; //yyyy-MM-dd HH:mm:ss

    protected static final Map<String, String> headers = Utility.ofMap("Content-Type", "application/json", "Accept", "application/json");

    protected static final Pattern PAYXML = Pattern.compile("<([^/>]+)>(.+)</.+>"); // "<([^/>]+)><!\\[CDATA\\[(.+)\\]\\]></.+>"
    
    //原始的配置
    protected Properties elementProps = new Properties();

    //配置对象集合
    protected Map<String, WeixinPayElement> elements = new HashMap<>();

    @Resource
    @Comment("必须存在全局配置项，@ResourceListener才会起作用")
    protected Environment environment;

    @Comment("定时任务")
    protected ScheduledThreadPoolExecutor scheduler;

    @Resource(name = "pay.weixin.conf", required = false) //支付配置文件路径
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
        this.reloadConfig(Pays.PAYTYPE_WEIXIN);
        this.scheduler = new ScheduledThreadPoolExecutor(1, (Runnable r) -> {
            final Thread t = new Thread(r, "Redkalex-Pay-Weixin-Certificate-Task-Thread");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(() -> {
            try {
                for (WeixinPayElement element : elements.values()) {
                    element.updateCertificate(logger);
                }
            } catch (Throwable e) {
                logger.log(Level.SEVERE, WeiXinPayService.class.getSimpleName() + " scheduleAtFixedRate error", e);
            }
        }, 60, 60 * 60, TimeUnit.SECONDS);
    }

    @ResourceListener //    
    @Comment("通过配置中心更改配置后的回调")
    void onResourceChanged(ResourceEvent[] events) {
        Properties changeProps = new Properties();
        changeProps.putAll(this.elementProps);
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            if (event.name().startsWith("pay.weixin.")) {
                changeProps.put(event.name(), event.newValue().toString());
                sb.append("@Resource change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
            }
        }
        if (sb.length() < 1) {
            return; //无相关配置变化
        }
        logger.log(Level.INFO, sb.toString());
        this.elements = WeixinPayElement.create(logger, changeProps, home);
        this.elementProps = changeProps;
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short payType) {
        return payType == Pays.PAYTYPE_WEIXIN && !elements.isEmpty();
    }

    @Override
    @Comment("重新加载本地文件配置")
    public void reloadConfig(short payType) {
        Properties properties = new Properties();
        if (this.conf != null && !this.conf.isEmpty()) { //存在微信支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in != null) {
                    properties.load(in);
                    in.close();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init WeixinPay conf error", e);
            }
        }
        this.environment.forEach(k -> k.startsWith("pay.weixin."), (k, v) -> properties.put(k, v));
        this.elements = WeixinPayElement.create(logger, properties, home);
        this.elementProps = properties;
    }

    public void setPayElements(Map<String, WeixinPayElement> elements) {
        this.elements = elements;
    }

    public void putPayElements(Map<String, WeixinPayElement> elements) {
        this.elements.putAll(elements);
    }

    @Override
    public WeixinPayElement getPayElement(String appid) {
        return this.elements.get(appid);
    }

    public void setPayElement(String appid, WeixinPayElement element) {
        this.elements.put(appid, element);
    }

    public boolean existsPayElement(String appid) {
        return this.elements != null && this.elements.containsKey(appid);
    }

    protected static HttpHeaders createHttpHeaders(final WeixinPayElement element, PayRequest request, String url, String method, String body) throws Exception {
        String path = url.substring(url.indexOf(".com") + ".com".length());
        String timestamp = String.valueOf(System.currentTimeMillis() / 1000);
        String nonce = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime());
        String message = method + "\n" + path + "\n" + timestamp + "\n" + nonce + "\n" + body + "\n";
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(element.privateKey);
        signature.update(message.getBytes(StandardCharsets.UTF_8));
        String signstr = Base64.getEncoder().encodeToString(signature.sign());
        String token = "mchid=\"" + element.merchno + "\","
            + "nonce_str=\"" + nonce + "\","
            + "timestamp=\"" + timestamp + "\","
            + "serial_no=\"" + element.certserialno + "\","
            + "signature=\"" + signstr + "\"";
        return HttpHeaders.of("Content-Type", "application/json", "Accept", "application/json", "Authorization", "WECHATPAY2-SHA256-RSA2048 " + token);
    }

    /**
     * 手机支付或者微信公众号支付时调用
     *
     * @param request PayPreRequest
     *
     * @return PayPreResponse
     */
    @Override
    public PayPreResponse prepay(PayPreRequest request) {
        return prepayAsync(request).join();
    }

    public static void main(String[] args) throws Throwable {

    }

    @Override
    public CompletableFuture<PayPreResponse> prepayAsync(PayPreRequest request) {
        request.checkVaild();
        final PayPreResponse result = new PayPreResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }
            result.setAppid(element.appid);
            final TreeMap<String, Object> map = new TreeMap<>();
            if (request.getAttach() != null) {
                map.putAll(request.getAttach());
            }
            map.put("appid", element.appid);
            map.put("mchid", element.merchno);
            map.put("description", request.getPayBody());
            map.put("out_trade_no", request.getPayno());
            map.put("amount", Utility.ofMap("total", request.getPayMoney(), "currency", "CNY"));
            if (request.getTimeoutSeconds() > 0) {
                map.put("time_expire", String.format(format, System.currentTimeMillis() + request.getTimeoutSeconds() * 1000).replaceFirst("(.*)(\\d\\d)", "$1:$2"));
            }
            map.put("notify_url", ((request.notifyUrl != null && !request.notifyUrl.isEmpty()) ? request.notifyUrl : element.notifyurl));

            String url;
            if (request.getPayWay() == PAYWAY_WEB) { //JSAPI下单
                url = "https://api.mch.weixin.qq.com/v3/pay/transactions/jsapi";
                if (!map.containsKey("openid")) {
                    return result.retcode(RETPAY_PARAM_ERROR).toFuture();
                }
                map.put("payer", Utility.ofMap("openid", map.remove("openid").toString()));
            } else if (request.getPayWay() == PAYWAY_APP) {
                url = "https://api.mch.weixin.qq.com/v3/pay/transactions/app";
            } else if (request.getPayWay() == PAYWAY_H5) {
                url = "https://api.mch.weixin.qq.com/v3/pay/transactions/h5";
                map.put("scene_info", Utility.ofMap("payer_client_ip", request.getClientAddr(), "h5_info", Utility.ofMap("type", "wap")));
            } else if (request.getPayWay() == PAYWAY_NATIVE) {
                url = "https://api.mch.weixin.qq.com/v3/pay/transactions/native";
            } else {
                return result.retcode(RETPAY_PARAM_ERROR).toFuture();
            }
            String body = convert.convertTo(map);
            Map<String, Serializable> respHeaders = new LinkedHashMap<>();
            return postHttpContentAsync(element.client, url, createHttpHeaders(element, request, url, "POST", body), body, respHeaders).thenApply(responseText -> {
                result.setResponseText(responseText);
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "Weixin.prepay." + request.getPayWay() + ": " + body + " --> " + responseText);
                }
                if (responseText == null || responseText.isEmpty() || !checkSign(element, null, responseText, respHeaders)) {
                    return result.retcode(RETPAY_FALSIFY_ERROR);
                }
                //PAYWAY_WEB: {"prepay_id": "wx261153585405162d4d02642eabe7000000"}
                //PAYWAY_APP: {"prepay_id": "wx261153585405162d4d02642eabe7000000"}
                //PAYWAY_NATIVE: {"code_url": "weixin://wxpay/bizpayurl?pr=p4lpSuKzz"}
                //PAYWAY_H5: {"h5_url": "https://wx.tenpay.com/cgi-bin/mmpayweb-bin/checkmweb?prepay_id=wx2916263004719461949c8&package=2150917749"}
                //ERROR: {"code":"NO_AUTH","message":"商户号该产品权限已被关闭，请前往商户平台>产品中心检查后重试"}
                Map<String, String> resultmap = convert.convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, responseText);
                if (resultmap.get("message") != null) {
                    return result.retcode(RETPAY_PARAM_ERROR);
                }
                Map<String, String> rsmap = new LinkedHashMap<>();
                if (request.getPayWay() == PAYWAY_WEB) {
                    rsmap.put(PayPreResponse.PREPAY_PREPAYID, resultmap.get("prepay_id"));
                } else if (request.getPayWay() == PAYWAY_APP) {
                    rsmap.put(PayPreResponse.PREPAY_PREPAYID, resultmap.get("prepay_id"));
                } else if (request.getPayWay() == PAYWAY_H5) {
                    rsmap.put(PayPreResponse.PREPAY_PAYURL, resultmap.get("h5_url"));
                } else if (request.getPayWay() == PAYWAY_NATIVE) {
                    rsmap.put(PayPreResponse.PREPAY_PAYURL, resultmap.get("code_url"));
                }
                result.setResult(rsmap);
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "prepay_pay_error req=" + request + ", resp=" + result.responseText, e);

            return result.toFuture();
        }
    }

    @Override
    public PayNotifyResponse notify(PayNotifyRequest request) {
        return notifyAsync(request).join();
    }

    @Override
    public CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request) {
        request.checkVaild();
        final PayNotifyResponse result = new PayNotifyResponse();
        String rstxt = "{\"code\": \"FAIL\",\"message\": \"失败\"}";
        try {
            result.setResponseText(request.getBody());
            result.setPayType(request.getPayType());
            NotifyData notifyData = convert.convertFrom(NotifyData.class, request.getBody());
            if (notifyData == null || !notifyData.event_type.endsWith(".SUCCESS")) {
                return result.retcode(notifyData != null && notifyData.event_type.startsWith("REFUND.") ? RETPAY_REFUND_FAILED : RETPAY_PAY_FAILED).notifytext(rstxt).toFuture();
            }

            final boolean refund = notifyData.event_type.startsWith("REFUND.");
            final WeixinPayElement element = elements.get(request.getAppid());
            String json = decryptToString(element, notifyData.resource.associated_data, notifyData.resource.nonce, notifyData.resource.ciphertext);
            result.setResponseText(json);
            NotifyPayResult payResult = convert.convertFrom(NotifyPayResult.class, json);
            if ("NOTPAY".equals(payResult.trade_state)) {
                return result.retcode(RETPAY_PAY_WAITING).notifytext(rstxt).toFuture();
            }
            if (refund && !"SUCCESS".equals(payResult.refund_status)) {
                return result.retcode(RETPAY_REFUND_FAILED).notifytext(rstxt).toFuture();
            }
            if (!refund && !"SUCCESS".equals(payResult.trade_state)) {
                return result.retcode(RETPAY_PAY_FAILED).notifytext(rstxt).toFuture();
            }
            result.setPayno(payResult.out_trade_no);
            result.setThirdPayno(payResult.transaction_id == null ? "" : payResult.transaction_id);
            if (refund) {
                result.setRefundedMoney(payResult.amount.refund);
                result.setPayedMoney(payResult.amount.refund);
            } else {
                result.setPayedMoney(payResult.amount.total);
            }
            return result.notifytext("{\"code\": \"SUCCESS\",\"message\": \"成功\"}").toFuture();
        } catch (Exception e) {
            logger.log(Level.WARNING, "notifyAsync error req=" + request + ", resp=" + request.getBody(), e);
            return result.retcode(RETPAY_PAY_FAILED).notifytext(rstxt).toFuture();
        }
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        return createAsync(request).join();
    }

    @Override
    public CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request) {
        request.checkVaild();
        return prepayAsync(request.createPayPreRequest()).thenApply(resp -> new PayCreatResponse(resp));
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        return queryAsync(request).join();
    }

    @Override
    public CompletableFuture<PayQueryResponse> queryAsync(PayRequest request) {
        request.checkVaild();
        final PayQueryResponse result = new PayQueryResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }

            String url = "https://api.mch.weixin.qq.com/v3/pay/transactions/out-trade-no/" + request.getPayno() + "?mchid=" + element.merchno;
            String body = "";
            Map<String, Serializable> respHeaders = new LinkedHashMap<>();
            return getHttpContentAsync(element.client, url, createHttpHeaders(element, request, url, "GET", body), body, respHeaders).thenApply(responseText -> {
                result.setResponseText(responseText);
                if (responseText == null || responseText.isEmpty() || !checkSign(element, null, responseText, respHeaders)) {
                    return result.retcode(RETPAY_FALSIFY_ERROR);
                }

                final NotifyPayResult payResult = convert.convertFrom(NotifyPayResult.class, responseText);
                String state = payResult.trade_state;
                //trade_state 支付状态: SUCCESS—支付成功 REFUND—转入退款 NOTPAY—未支付 CLOSED—已关闭 REVOKED—已撤销（刷卡支付） USERPAYING--用户支付中 PAYERROR--支付失败(其他原因，如银行返回失败)
                short paystatus = PAYSTATUS_PAYNO;
                if (state != null) {
                    switch (state) {
                        case "SUCCESS": paystatus = PAYSTATUS_PAYOK;
                            break;
                        case "NOTPAY": paystatus = PAYSTATUS_UNPAY;
                            break;
                        case "CLOSED": paystatus = PAYSTATUS_CLOSED;
                            break;
                        case "REVOKED": paystatus = PAYSTATUS_CANCELED;
                            break;
                        case "USERPAYING": paystatus = PAYSTATUS_PAYING;
                            break;
                        case "PAYERROR": paystatus = PAYSTATUS_PAYNO;
                            break;
                    }
                }
                result.setPayStatus(paystatus);
                result.setThirdPayno(payResult.transaction_id == null ? "" : payResult.transaction_id);
                result.setPayedMoney(payResult.amount == null ? 0 : payResult.amount.total);
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "query_pay_error req=" + request + ", resp=" + result.responseText, e);
            return result.toFuture();
        }
    }

    @Override
    public PayResponse close(PayCloseRequest request) {
        return closeAsync(request).join();
    }

    @Override
    public CompletableFuture<PayResponse> closeAsync(PayCloseRequest request) {
        request.checkVaild();
        final PayResponse result = new PayResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }
            Map<String, String> map = new TreeMap<>();
            map.put("mchid", element.merchno);

            String url = "https://api.mch.weixin.qq.com/v3/pay/transactions/out-trade-no/" + request.getPayno() + "/close";
            String body = convert.convertTo(map);
            Map<String, Serializable> respHeaders = new LinkedHashMap<>();
            return postHttpContentAsync(element.client, url, createHttpHeaders(element, request, url, "POST", body), body, respHeaders).thenApply(responseText -> {
                result.setResponseText(responseText);
                if (responseText == null || responseText.isEmpty() || !checkSign(element, null, responseText, respHeaders)) {
                    return result.retcode(RETPAY_FALSIFY_ERROR);
                }

                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "close_pay_error req=" + request + ", resp=" + result.responseText, e);
            return result.toFuture();
        }

    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        return refundAsync(request).join();
    }

    //https://pay.weixin.qq.com/wiki/doc/api/micropay.php?chapter=9_4
    @Override
    public CompletableFuture<PayRefundResponse> refundAsync(PayRefundRequest request) {
        request.checkVaild();
        final PayRefundResponse result = new PayRefundResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }
            final TreeMap<String, Object> map = new TreeMap<>();
            map.put("out_trade_no", request.getPayno());
            map.put("out_refund_no", request.getRefundno());
            map.put("amount", Utility.ofMap("total", request.getPayMoney(), "refund", request.getRefundMoney(), "currency", "CNY"));
            map.put("notify_url", element.notifyurl);

            String url = "https://api.mch.weixin.qq.com/v3/refund/domestic/refunds";
            String body = convert.convertTo(map);
            Map<String, Serializable> respHeaders = new LinkedHashMap<>();
            return postHttpContentAsync(element.client, url, createHttpHeaders(element, request, url, "POST", body), body, respHeaders).thenApply(responseText -> {
                result.setResponseText(responseText);
                if (responseText == null || responseText.isEmpty() || !checkSign(element, null, responseText, respHeaders)) {
                    return result.retcode(RETPAY_FALSIFY_ERROR);
                }

                RefundData refundData = convert.convertFrom(RefundData.class, responseText);
                if (!"SUCCESS".equals(refundData.status)) {
                    return result.retcode(RETPAY_REFUND_ERROR);
                }
                result.setRefundedMoney(refundData.amount.refund);
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_REFUND_ERROR);
            logger.log(Level.WARNING, "refund_pay_error req=" + request + ", resp=" + result.responseText, e);
            return result.toFuture();
        }
    }

    @Override
    public PayRefundResponse queryRefund(PayRefundQryReq request) {
        return queryRefundAsync(request).join();
    }

    @Override
    public CompletableFuture<PayRefundResponse> queryRefundAsync(PayRefundQryReq request) {
        request.checkVaild();
        final PayRefundResponse result = new PayRefundResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }

            String url = "https://api.mch.weixin.qq.com/v3/refund/domestic/refunds/" + request.getRefundno();
            String body = "";
            Map<String, Serializable> respHeaders = new LinkedHashMap<>();
            return getHttpContentAsync(element.client, url, createHttpHeaders(element, request, url, "GET", body), body, respHeaders).thenApply(responseText -> {
                result.setResponseText(responseText);
                if (responseText == null || responseText.isEmpty() || !checkSign(element, null, responseText, respHeaders)) {
                    return result.retcode(RETPAY_FALSIFY_ERROR);
                }

                RefundData refundData = convert.convertFrom(RefundData.class, responseText);
                if (!"SUCCESS".equals(refundData.status)) {
                    return result.retcode(RETPAY_REFUND_ERROR);
                }
                result.setRefundedMoney(refundData.amount.refund);
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "query_pay_error req=" + request + ", resp=" + result.responseText, e);
            return result.toFuture();
        }

    }

    @Override
    protected String createSign(final PayElement element, Map<String, ?> map, String text) { //计算签名
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected boolean checkSign(final PayElement element, Map<String, ?> map, String responseText, Map<String, Serializable> respHeaders) {  //验证签名
        String requestid = respHeaders.getOrDefault("Request-ID", respHeaders.get("request-id")).toString();
        String timestamp = respHeaders.getOrDefault("Wechatpay-Timestamp", respHeaders.get("wechatpay-timestamp")).toString();
        String nonce = respHeaders.getOrDefault("Wechatpay-Nonce", respHeaders.get("wechatpay-nonce")).toString();
        String serial = respHeaders.getOrDefault("Wechatpay-Serial", respHeaders.get("wechatpay-serial")).toString();
        String signature = respHeaders.getOrDefault("Wechatpay-Signature", respHeaders.get("wechatpay-signature")).toString();
        String message = timestamp + "\n" + nonce + "\n" + responseText + "\n";
        if (requestid == null || timestamp == null || nonce == null || serial == null || signature == null) {
            logger.log(Level.WARNING, "WeixinPay checkSign header is half-baked, responseText=" + responseText + ", respHeaders =" + respHeaders);
            return false;
        }
        X509Certificate certificate = ((WeixinPayElement) element).certificates.get(serial);
        if (certificate == null) {
            logger.log(Level.WARNING, "WeixinPay checkSign certificate is null, responseText=" + responseText + ", respHeaders =" + respHeaders);
            return false;
        }
        try {
            Signature sign = Signature.getInstance("SHA256withRSA");
            sign.initVerify(certificate);
            sign.update(message.getBytes(StandardCharsets.UTF_8));
            return sign.verify(Base64.getDecoder().decode(signature));
        } catch (Exception e) {
            logger.log(Level.WARNING, "WeixinPay checkSign error, responseText=" + responseText + ", respHeaders =" + respHeaders, e);
            return false;
        }
    }

    protected static String decryptToString(WeixinPayElement element, String associated_data, String nonce, String ciphertext) throws Exception {
        SecretKeySpec key = new SecretKeySpec(element.apiv3key.getBytes(StandardCharsets.UTF_8), "AES");
        GCMParameterSpec spec = new GCMParameterSpec(128, nonce.getBytes(StandardCharsets.UTF_8));
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, key, spec);
        cipher.updateAAD(associated_data.getBytes(StandardCharsets.UTF_8));
        return new String(cipher.doFinal(Base64.getDecoder().decode(ciphertext)), StandardCharsets.UTF_8);
    }

    public static class WeixinPayElement extends PayElement {

        // pay.weixin.[x].merchno
        public String merchno = ""; //商户ID

        // pay.weixin.[x].submerchno
        public String submerchno = ""; //子商户ID，受理模式必填

        // pay.weixin.[x].appid
        public String appid = "";  //APP应用ID

        // pay.weixin.[x].subappid
        public String subappid = ""; //子应用ID，受理模式必填

        // pay.weixin.[x].apiv3key
        public String apiv3key = ""; //APIv3密钥

        // pay.weixin.[x].certserialno
        public String certserialno = ""; //HTTP证书的序列号

        // pay.weixin.[x].prikeypath
        public String prikeypath = "";  //apiclient_key.pem文件路径，用来加载证书用, 不是/开头且没有:字符，视为{APP_HOME}/conf相对下的路径

        // pay.weixin.[x].prikeybase64
        public String prikeybase64 = ""; //apiclient_key.pem文件内容

        // pay.weixin.[x].certpath
        public String certpath = "";  //apiclient_cert.pem文件路径，用来加载证书用, 不是/开头且没有:字符，视为{APP_HOME}/conf相对下的路径

        // pay.weixin.[x].certbase64
        public String certbase64 = "";  //apiclient_cert.pem文件内容

        //
        protected PrivateKey privateKey; //应用私钥

        //
        protected PublicKey publicKey;

        //
        protected Map<String, X509Certificate> certificates = new LinkedHashMap<>();

        //
        protected HttpClient client;

        public static Map<String, WeixinPayElement> create(Logger logger, Properties properties, File home) {
            String def_appid = properties.getProperty("pay.weixin.appid", "").trim();
            String def_subappid = properties.getProperty("pay.weixin.subappid", "").trim();
            String def_merchno = properties.getProperty("pay.weixin.merchno", "").trim();
            String def_submerchno = properties.getProperty("pay.weixin.submerchno", "").trim();
            String def_notifyurl = properties.getProperty("pay.weixin.notifyurl", "").trim();
            String def_apiv3key = properties.getProperty("pay.weixin.apiv3key", "").trim();
            String def_certserialno = properties.getProperty("pay.weixin.certserialno", "").trim();
            String def_prikeypath = properties.getProperty("pay.weixin.prikeypath", "").trim();
            String def_prikeybase64 = properties.getProperty("pay.weixin.prikeybase64", "").trim();
            String def_certpath = properties.getProperty("pay.weixin.certpath", "").trim();
            String def_certbase64 = properties.getProperty("pay.weixin.certbase64", "").trim();

            final Map<String, WeixinPayElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("pay.weixin.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String subappid = properties.getProperty(prefix + ".subappid", def_subappid).trim();
                String merchno = properties.getProperty(prefix + ".merchno", def_merchno).trim();
                String submerchno = properties.getProperty(prefix + ".submerchno", def_submerchno).trim();
                String notifyurl = properties.getProperty(prefix + ".notifyurl", def_notifyurl).trim();
                String apiv3key = properties.getProperty(prefix + ".apiv3key", def_apiv3key).trim();
                String certserialno = properties.getProperty(prefix + ".certserialno", def_certserialno).trim();
                String prikeypath = properties.getProperty(prefix + ".prikeypath", def_prikeypath).trim();
                String prikeybase64 = properties.getProperty(prefix + ".prikeybase64", def_prikeybase64).trim();
                String certpath = properties.getProperty(prefix + ".certpath", def_certpath).trim();
                String certbase64 = properties.getProperty(prefix + ".certbase64", def_certbase64).trim();

                if (appid.isEmpty() || merchno.isEmpty() || notifyurl.isEmpty() || apiv3key.isEmpty() || apiv3key.length() != 32
                    || (prikeypath.isEmpty() && prikeybase64.isEmpty()) || (certpath.isEmpty() && certbase64.isEmpty())) {
                    logger.log(Level.WARNING, properties + "; has illegal weixinpay conf by prefix" + prefix);
                    return;
                }
                WeixinPayElement element = new WeixinPayElement();
                element.appid = appid;
                element.subappid = subappid;
                element.merchno = merchno;
                element.submerchno = submerchno;
                element.notifyurl = notifyurl;
                element.apiv3key = apiv3key;
                element.certserialno = certserialno;
                element.prikeypath = prikeypath;
                element.prikeybase64 = prikeybase64;
                element.certpath = certpath;
                element.certbase64 = certbase64;
                if (element.initElement(logger, home)) {
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
            try {
                InputStream in;

                if (this.prikeybase64 != null && !this.prikeybase64.isEmpty()) {
                    in = new ByteArrayInputStream(this.prikeybase64.getBytes(StandardCharsets.UTF_8));
                } else {
                    File file = (prikeypath.indexOf('/') == 0 || prikeypath.indexOf(':') > 0) ? new File(this.certpath) : new File(home, "conf/" + this.prikeypath);
                    in = file.isFile() ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.prikeypath);
                }
                if (in == null) {
                    return false;
                }
                KeyFactory kf = KeyFactory.getInstance("RSA");
                String pripem = Utility.read(in, StandardCharsets.UTF_8).replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replaceAll("\\s+", "");
                this.privateKey = kf.generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pripem)));
                in.close();

                if (this.certbase64 != null && !this.certbase64.isEmpty()) {
                    in = new ByteArrayInputStream(Base64.getDecoder().decode(this.certbase64));
                } else {
                    File file = (certpath.indexOf('/') == 0 || certpath.indexOf(':') > 0) ? new File(this.certpath) : new File(home, "conf/" + this.certpath);
                    in = file.isFile() ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.certpath);
                }
                if (in == null) {
                    return false;
                }
                CertificateFactory certificatefactory = CertificateFactory.getInstance("X.509");
                X509Certificate x509Cert = (X509Certificate) certificatefactory.generateCertificate(in);
                x509Cert.checkValidity();
                this.certificates.put(x509Cert.getSerialNumber().toString(16).toUpperCase(), x509Cert);
                in.close();
                this.client = HttpClient.newBuilder().build();

                updateCertificate(logger).join();
                return true;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init WeixinPay initElement error", e);
                return false;
            }
        }

        public CompletableFuture<String> updateCertificate(Logger logger) throws Exception {
            String body = "";
            String url = "https://api.mch.weixin.qq.com/v3/certificates";
            Map<String, Serializable> respHeaders = new LinkedHashMap<>();
            return getHttpContentAsync(url, 6000, createHttpHeaders(this, null, url, "GET", body), body, respHeaders).thenApply(responseText -> {
                if (responseText == null || responseText.isEmpty()) {
                    return "";
                }
                CertificateData dataResult = JsonConvert.root().convertFrom(CertificateData.class, responseText);
                if (dataResult.data == null) {
                    return "";
                }
                for (CertificateItem item : dataResult.data) {
                    try {
                        String nonce = item.encrypt_certificate.nonce;
                        String ciphertext = item.encrypt_certificate.ciphertext;
                        String associated_data = item.encrypt_certificate.associated_data;

                        String cert = decryptToString(this, associated_data, nonce, ciphertext);
                        CertificateFactory cf = CertificateFactory.getInstance("X509");
                        X509Certificate x509Cert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(cert.getBytes(StandardCharsets.UTF_8)));
                        x509Cert.checkValidity();
                        certificates.put(x509Cert.getSerialNumber().toString(16).toUpperCase(), x509Cert);
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "init WeixinPay updateCertificate error, " + item, e);
                        continue;
                    }
                }
                return "";
            });
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class CertificateData {

        public CertificateItem[] data;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class CertificateItem {

        public String serial_no;

        public String effective_time;

        public String expire_time;

        public CertificateEncrypt encrypt_certificate;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class CertificateEncrypt {

        public String algorithm;

        public String nonce;

        public String associated_data;

        public String ciphertext;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class NotifyData {

        public String id;

        public String create_time;

        public String event_type;

        public String resource_type;

        public String summary;

        public NotifyResource resource;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class NotifyResource {

        public String algorithm;

        public String nonce;

        public String associated_data;

        public String ciphertext;

        public String original_type;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class NotifyPayResult { //查询接口也会使用此类

        public String appid;

        public String mchid;

        public String transaction_id = "";

        public String trade_state; //SUCCESS

        public String refund_status;   //SUCCESS

        public String out_trade_no;

        public String trade_type;

        public NotifyPayAmount amount;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class NotifyPayAmount {

        public long total;

        public long refund; //退款通知才会有此值

        public String currency;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class RefundData {

        public String refund_id;

        public String out_refund_no;

        public String status;

        public RefundAmount amount;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class RefundAmount {

        public long total;

        public long refund;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }
}
