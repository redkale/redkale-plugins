/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.nio.charset.*;
import java.security.*;
import java.security.spec.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import java.util.stream.Collectors;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Comment;
import org.redkale.annotation.*;
import org.redkale.annotation.ResourceListener;
import org.redkale.convert.json.*;
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
@Comment("支付宝支付服务")
public final class AliPayService extends AbstractPayService {

    protected static final String format = "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS"; //yyyy-MM-dd HH:mm:ss

    protected static final Map<String, String> headers = Utility.ofMap("Content-Type", "application/x-www-form-urlencoded", "Accept", "application/json");

    protected static final Charset CHARSET_GBK = Charset.forName("GBK");

    //原始的配置
    protected Properties elementProps = new Properties();

    //配置对象集合
    protected Map<String, AliPayElement> elements = new HashMap<>();

    @Resource
    @Comment("必须存在全局配置项，@ResourceListener才会起作用")
    protected Environment environment;

    @Resource(name = "pay.alipay.conf", required = false) //支付配置文件路径
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
        this.reloadConfig(Pays.PAYTYPE_ALIPAY);
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short payType) {
        return payType == PAYTYPE_ALIPAY && !elements.isEmpty();
    }

    @Override
    @Comment("重新加载本地文件配置")
    public void reloadConfig(short payType) {
        Properties properties = new Properties();
        if (this.conf != null && !this.conf.isEmpty()) { //存在支付宝支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in != null) {
                    properties.load(in);
                    in.close();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init alipay conf error", e);
            }
        }
        this.environment.forEach(k -> k.startsWith("pay.alipay."), (k, v) -> properties.put(k, v));
        this.elements = AliPayElement.create(logger, properties);
        this.elementProps = properties;
    }

    @ResourceListener //    
    @Comment("通过配置中心更改配置后的回调")
    void onResourceChanged(ResourceEvent[] events) {
        Properties changeProps = new Properties();
        changeProps.putAll(this.elementProps);
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            if (event.name().startsWith("pay.alipay.")) {
                if (event.newValue() == null) {
                    changeProps.remove(event.name());
                } else {
                    changeProps.put(event.name(), event.newValue().toString());
                }
                sb.append("@Resource change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
            }
        }
        if (sb.length() < 1) {
            return; //无相关配置变化
        }
        logger.log(Level.INFO, sb.toString());
        this.elements = AliPayElement.create(logger, changeProps);
        this.elementProps = changeProps;
    }

    public void setPayElements(Map<String, AliPayElement> elements) {
        this.elements = elements;
    }

    public void putPayElements(Map<String, AliPayElement> elements) {
        this.elements.putAll(elements);
    }

    @Override
    public AliPayElement getPayElement(String appid) {
        return this.elements.get(appid);
    }

    public void setPayElement(String appid, AliPayElement element) {
        this.elements.put(appid, element);
    }

    public boolean existsPayElement(String appid) {
        return this.elements != null && this.elements.containsKey(appid);
    }

    @Override
    public PayPreResponse prepay(final PayPreRequest request) {
        return prepayAsync(request).join();
    }

    protected String joinEncodeMap(Map<String, ?> map, Charset charset) {
        if (!(map instanceof SortedMap)) {
            map = new TreeMap<>(map);
        }
        return map.entrySet().stream().map((e -> e.getKey() + "=" + urlEncode(e.getValue(), charset))).collect(Collectors.joining("&"));
    }

    @Override
    public CompletableFuture<PayPreResponse> prepayAsync(final PayPreRequest request) {
        request.checkVaild();
        //参数说明： https://doc.open.alipay.com/doc2/detail.htm?spm=a219a.7629140.0.0.lMJkw3&treeId=59&articleId=103663&docType=1
        final PayPreResponse result = new PayPreResponse();
        try {
            final AliPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }
            result.setAppid(element.appid);
            long now = System.currentTimeMillis();

            TreeMap<String, String> paramap = new TreeMap<>();
            paramap.put("app_id", element.appid);
            paramap.put("charset", element.charsetname);
            paramap.put("format", "json");
            paramap.put("sign_type", "RSA2");
            paramap.put("version", "1.0");
            paramap.put("timestamp", Utility.formatTime(now));

            if (request.getPayWay() == PAYWAY_WEB) {
                paramap.put("qr_pay_mode", "2");
                paramap.put("method", "alipay.trade.page.pay");
            } else if (request.getPayWay() == PAYWAY_APP) {
                paramap.put("method", "alipay.trade.app.pay");
            } else {
                paramap.put("method", "alipay.trade.wap.pay");
            }

            if (request.notifyUrl != null && !request.notifyUrl.isEmpty()) {
                paramap.put("notify_url", request.notifyUrl);
            } else if (element.notifyurl != null && !element.notifyurl.isEmpty()) {
                paramap.put("notify_url", element.notifyurl);
            }
            if (request.returnUrl != null && !request.returnUrl.isEmpty()) {
                paramap.put("return_url", request.returnUrl);
            }
            paramap.put("alipay_sdk", "alipay-sdk-java-dynamicVersionNo");
            //paramap.put("return_url", "");

            final TreeMap<String, String> biz_content = new TreeMap<>();
            if (request.getAttach() != null) {
                biz_content.putAll(request.getAttach());
            }
            biz_content.put("out_trade_no", request.getPayno());
            biz_content.put("total_amount", "" + (request.getPayMoney() / 100.0));
            biz_content.put("subject", "" + request.getPayTitle());
            if (request.getPayWay() == PAYWAY_WEB) {
                biz_content.put("product_code", "FAST_INSTANT_TRADE_PAY");
            } else if (request.getPayWay() == PAYWAY_APP) {
                biz_content.put("product_code", "QUICK_MSECURITY_PAY");
            } else {
                biz_content.put("product_code", "QUICK_WAP_PAY");
                biz_content.put("quit_url", paramap.get("notify_url")); //返回url 
            }
            if (request.getTimeoutSeconds() > 0) {
                biz_content.put("time_expire", Utility.formatTime(now + request.getTimeoutSeconds() * 1000));
            }
            paramap.put("biz_content", convert.convertTo(biz_content));
            paramap.put("sign", createSign(element, paramap, null));
            result.setResult(Utility.ofMap(PayPreResponse.PREPAY_PAYURL, "https://openapi.alipay.com/gateway.do?" + joinEncodeMap(paramap, element.charset)));
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "prepay_pay_error req=" + request + ", resp=" + result.responseText, e);
        }
        return result.toFuture();
    }

    @Override
    public PayNotifyResponse notify(final PayNotifyRequest request) {
        return notifyAsync(request).join();
    }

    //手机支付回调  
    // https://doc.open.alipay.com/doc2/detail.htm?spm=a219a.7629140.0.0.UywIMY&treeId=59&articleId=103666&docType=1
    @Override
    public CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request) {
        request.checkVaild();
        final PayNotifyResponse result = new PayNotifyResponse();
        result.setPayType(request.getPayType());
        final String rstext = "success";
        Map<String, String> map = request.getAttach();
        final AliPayElement element = elements.get(request.getAppid());
        if (element == null) {
            return result.retcode(RETPAY_CONF_ERROR).toFuture();
        }
        result.setPayno(map.getOrDefault("out_trade_no", ""));
        result.setThirdPayno(map.getOrDefault("trade_no", ""));
        if (!checkSign(element, map, request.getBody(), request.getHeaders())) {
            return result.retcode(RETPAY_FALSIFY_ERROR).toFuture();
        }
        String state = map.getOrDefault("trade_status", "");
        if ("WAIT_BUYER_PAY".equals(state)) {
            return result.retcode(RETPAY_PAY_WAITING).toFuture();
        }
        if (!"TRADE_SUCCESS".equals(state)) {
            return result.retcode(RETPAY_PAY_FAILED).toFuture();
        }
        result.setPayedMoney((long) (Float.parseFloat(map.get("total_amount")) * 100));
        return result.notifytext(rstext).toFuture();
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        return createAsync(request).join();
    }

    @Override
    public CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request) {
        request.checkVaild();
        final PayCreatResponse result = new PayCreatResponse();
        try {
            final AliPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("app_id", element.appid);
            map.put("method", "alipay.trade.create");
            map.put("format", "JSON");
            map.put("charset", element.charsetname);
            map.put("sign_type", "RSA2");
            map.put("timestamp", String.format(format, System.currentTimeMillis()));
            map.put("version", "1.0");
            if (element.notifyurl != null && !element.notifyurl.isEmpty()) {
                map.put("notify_url", element.notifyurl);
            }

            final TreeMap<String, String> biz_content = new TreeMap<>();
            if (request.getAttach() != null) {
                biz_content.putAll(request.getAttach());
            }
            biz_content.put("out_trade_no", request.getPayno());
            //biz_content.putIfAbsent("scene", "bar_code");
            biz_content.put("total_amount", "" + (request.getPayMoney() / 100.0));
            biz_content.put("subject", "" + request.getPayTitle());
            biz_content.put("body", request.getPayBody());
            map.put("biz_content", convert.convertTo(biz_content));

            map.put("sign", createSign(element, map, null));

            return postHttpContentAsync("https://openapi.alipay.com/gateway.do", element.charset, headers, joinEncodeMap(map, element.charset)).thenApply(responseText -> {
                //{"alipay_trade_create_response":{"code":"40002","msg":"Invalid Arguments","sub_code":"isv.invalid-signature","sub_msg":"无效签名"},"sign":"xxxxxxxxxxxx"}
                result.setResponseText(responseText);
                final InnerCreateResponse resp = convert.convertFrom(InnerCreateResponse.class, responseText);
                resp.responseText = responseText; //原始的返回内容            
                if (!checkSign(element, resp)) {
                    return result.retcode(RETPAY_FALSIFY_ERROR);
                }
                final Map<String, String> resultmap = resp.alipay_trade_create_response;
                result.setResult(resultmap);
                if (!"SUCCESS".equalsIgnoreCase(resultmap.get("msg"))) {
                    return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("sub_msg"));
                }
                result.setThirdPayno(resultmap.getOrDefault("trade_no", ""));
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "create_pay_error req=" + request + ", resp=" + result.responseText, e);
            return result.toFuture();
        }
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
            final AliPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("app_id", element.appid);
            map.put("sign_type", "RSA2");
            map.put("charset", element.charsetname);
            map.put("format", "json");
            map.put("version", "1.0");
            map.put("timestamp", String.format(format, System.currentTimeMillis()));
            map.put("method", "alipay.trade.query");

            final TreeMap<String, String> biz_content = new TreeMap<>();
            biz_content.put("out_trade_no", request.getPayno());
            map.put("biz_content", convert.convertTo(biz_content));

            map.put("sign", createSign(element, map, null));

            return postHttpBytesContentAsync("https://openapi.alipay.com/gateway.do", headers, joinEncodeMap(map, element.charset)).thenApply(bytes -> {
                String responseText = new String(bytes, element.charset);
                result.setResponseText(responseText);
                InnerQueryResponse resp = convert.convertFrom(InnerQueryResponse.class, responseText);
                resp.responseText = responseText; //原始的返回内容            
                if (!checkSign(element, resp)) { //可能会乱码, ALipay的bug?
                    String responseText2 = new String(bytes, CHARSET_GBK);
                    InnerQueryResponse resp2 = convert.convertFrom(InnerQueryResponse.class, responseText2);
                    resp2.responseText = responseText2; //原始的返回内容     
                    if (!checkSign(element, resp2)) {
                        return result.retcode(RETPAY_FALSIFY_ERROR);
                    }
                    resp = resp2;
                    result.setResponseText(responseText2);
                }
                final Map<String, String> resultmap = resp.alipay_trade_query_response;
                result.setResult(resultmap);
                if (!"SUCCESS".equalsIgnoreCase(resultmap.get("msg"))) {
                    return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("sub_msg"));
                }
                //trade_status 交易状态：WAIT_BUYER_PAY（交易创建，等待买家付款）、TRADE_CLOSED（未付款交易超时关闭，或支付完成后全额退款）、TRADE_SUCCESS（交易支付成功）、TRADE_FINISHED（交易结束，不可退款）
                short paystatus = PAYSTATUS_PAYNO;
                switch (resultmap.get("trade_status")) {
                    case "TRADE_SUCCESS": paystatus = PAYSTATUS_PAYOK;
                        break;
                    case "WAIT_BUYER_PAY": paystatus = PAYSTATUS_UNPAY;
                        break;
                    case "TRADE_CLOSED": paystatus = PAYSTATUS_CLOSED;
                        break;
                    case "TRADE_FINISHED": paystatus = PAYSTATUS_PAYOK;
                        break;
                }
                result.setPayStatus(paystatus);
                result.setThirdPayno(resultmap.getOrDefault("trade_no", ""));
                result.setPayedMoney((long) (Double.parseDouble(resultmap.get("total_amount")) * 100));
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
            final AliPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("app_id", element.appid);
            map.put("sign_type", "RSA2");
            map.put("charset", element.charsetname);
            map.put("format", "json");
            map.put("version", "1.0");
            map.put("timestamp", String.format(format, System.currentTimeMillis()));
            map.put("method", "alipay.trade.close");
            if (element.notifyurl != null && !element.notifyurl.isEmpty()) {
                map.put("notify_url", element.notifyurl);
            }

            final TreeMap<String, String> biz_content = new TreeMap<>();
            biz_content.put("out_trade_no", request.getPayno());
            map.put("biz_content", convert.convertTo(biz_content));

            map.put("sign", createSign(element, map, null));

            return postHttpContentAsync("https://openapi.alipay.com/gateway.do", element.charset, headers, joinEncodeMap(map, element.charset)).thenApply(responseText -> {

                result.setResponseText(responseText);
                final InnerCloseResponse resp = convert.convertFrom(InnerCloseResponse.class, responseText);
                resp.responseText = responseText; //原始的返回内容            
                if (!checkSign(element, resp)) {
                    return result.retcode(RETPAY_FALSIFY_ERROR);
                }
                final Map<String, String> resultmap = resp.alipay_trade_close_response;
                result.setResult(resultmap);
                if (!"SUCCESS".equalsIgnoreCase(resultmap.get("msg"))) {
                    return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("sub_msg"));
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

    //https://doc.open.alipay.com/docs/api.htm?spm=a219a.7629065.0.0.wavZ99&apiId=759&docType=4
    @Override
    public CompletableFuture<PayRefundResponse> refundAsync(PayRefundRequest request) {
        request.checkVaild();
        final PayRefundResponse result = new PayRefundResponse();
        try {
            final AliPayElement element = elements.get(request.getAppid());
            if (element == null) {
                return result.retcode(RETPAY_CONF_ERROR).toFuture();
            }
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("app_id", element.appid);
            map.put("sign_type", "RSA2");
            map.put("charset", element.charsetname);
            map.put("format", "json");
            map.put("version", "1.0");
            map.put("timestamp", String.format(format, System.currentTimeMillis()));
            map.put("method", "alipay.trade.refund");

            final TreeMap<String, String> biz_content = new TreeMap<>();
            biz_content.put("out_trade_no", request.getPayno());
            biz_content.put("refund_amount", "" + (request.getRefundMoney() / 100.0));
            map.put("biz_content", convert.convertTo(biz_content));

            map.put("sign", createSign(element, map, null));

            return postHttpContentAsync("https://openapi.alipay.com/gateway.do", element.charset, headers, joinEncodeMap(map, element.charset)).thenApply(responseText -> {

                result.setResponseText(responseText);
                final InnerCloseResponse resp = convert.convertFrom(InnerCloseResponse.class, responseText);
                resp.responseText = responseText; //原始的返回内容            
                if (!checkSign(element, resp)) {
                    return result.retcode(RETPAY_FALSIFY_ERROR);
                }
                final Map<String, String> resultmap = resp.alipay_trade_close_response;
                result.setResult(resultmap);
                if (!"SUCCESS".equalsIgnoreCase(resultmap.get("msg"))) {
                    return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("sub_msg"));
                }
                result.setRefundedMoney((long) (Double.parseDouble(resultmap.get("refund_fee")) * 100));
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
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
        PayQueryResponse queryResponse = query(request);
        final PayRefundResponse response = new PayRefundResponse();
        response.setRetcode(queryResponse.getRetcode());
        response.setRetinfo(queryResponse.getRetinfo());
        response.setResponseText(queryResponse.getResponseText());
        response.setResult(queryResponse.getResult());
        if (queryResponse.isSuccess()) {
            response.setRefundedMoney((long) (Double.parseDouble(response.getResult().get("receipt_amount")) * 100));
        }
        return response.toFuture();
    }

    protected boolean checkSign(final PayElement element, InnerResponse response) {
        if (((AliPayElement) element).aliKey == null) {
            return true;
        }
        try {
            String text = response.responseText;
            text = text.substring(text.indexOf(':') + 1, text.indexOf(",\"sign\""));

            Signature signature = Signature.getInstance("SHA256WithRSA");
            signature.initVerify(((AliPayElement) element).aliKey);
            signature.update(text.getBytes(((AliPayElement) element).charset));
            return signature.verify(Base64.getDecoder().decode(response.sign.getBytes()));
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "checkSign error: element =" + element + ", response =" + response);
            return false;
        }
    }

    @Override
    protected boolean checkSign(final PayElement element, Map<String, ?> map0, String text0, Map<String, String> respHeaders) { //支付宝玩另类
        if (((AliPayElement) element).aliKey == null) {
            return true;
        }
        Map<String, String> map = (Map<String, String>) map0;
        String sign = (String) map.remove("sign");
        if (sign == null) {
            return false;
        }
        String sign_type = (String) map.remove("sign_type");
        String text = joinMap(map);
        map.put("sign", sign);
        if (sign_type != null) {
            map.put("sign_type", sign_type);
        }
        try {
            Signature signature = Signature.getInstance("SHA256WithRSA");
            signature.initVerify(((AliPayElement) element).aliKey);
            signature.update(text.getBytes(StandardCharsets.UTF_8));
            return signature.verify(Base64.getDecoder().decode(sign.getBytes()));
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    protected String createSign(final PayElement element, Map<String, ?> map, String text) {
        try {
            Signature signature = Signature.getInstance("SHA256WithRSA");
            signature.initSign(((AliPayElement) element).priKey);
            signature.update(joinMap(map).getBytes(((AliPayElement) element).charset));
            return Base64.getEncoder().encodeToString(signature.sign());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static class InnerCloseResponse extends InnerResponse {

        public Map<String, String> alipay_trade_close_response;

    }

    public static class InnerQueryResponse extends InnerResponse {

        public Map<String, String> alipay_trade_query_response;

    }

    public static class InnerCreateResponse extends InnerResponse {

        public Map<String, String> alipay_trade_create_response;

    }

    public static class InnerResponse {

        public String responseText;

        public String sign;

        @Override
        public String toString() {
            return JsonFactory.root().getConvert().convertTo(this);
        }
    }

    public static class AliPayElement extends PayElement {

        // pay.alipay.[x].merchno
        public String merchno = ""; //商户ID 签约的支付宝账号对应的支付宝唯一用户号。以2088开头的16位纯数字组成。

        // pay.alipay.[x].sellerid
        public String sellerid = ""; //商户账号，为空时则使用 merchno

        // pay.alipay.[x].charset
        public String charsetname = "UTF-8"; //字符集 

        public Charset charset;

        // pay.alipay.[x].appid
        public String appid = "";  //APP应用ID

        // pay.alipay.[x].appprikeybase64
        public String appprikeybase64 = ""; //应用私钥

        // pay.alipay.[x].apppubkeybase64
        public String apppubkeybase64 = ""; //应用公钥

        // pay.alipay.[x].alipubkeybase64
        public String alipubkeybase64 = ""; //支付宝公钥

        //        
        protected PrivateKey priKey; //应用私钥

        //  
        protected PublicKey pubKey; //应用公钥

        //
        protected PublicKey aliKey; //支付宝公钥

        public static Map<String, AliPayElement> create(Logger logger, Properties properties) {
            String def_appid = properties.getProperty("pay.alipay.appid", "").trim();
            String def_merchno = properties.getProperty("pay.alipay.merchno", "").trim();
            String def_sellerid = properties.getProperty("pay.alipay.sellerid", "").trim();
            String def_charsetname = properties.getProperty("pay.alipay.charsetname", "UTF-8").trim();
            String def_notifyurl = properties.getProperty("pay.alipay.notifyurl", "").trim();
            String def_appprikeybase64 = properties.getProperty("pay.alipay.appprikeybase64", "").trim();
            String def_apppubkeybase64 = properties.getProperty("pay.alipay.apppubkeybase64", "").trim();
            String def_alipubkeybase64 = properties.getProperty("pay.alipay.alipubkeybase64", "").trim();

            final Map<String, AliPayElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("pay.alipay.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String merchno = properties.getProperty(prefix + ".merchno", def_merchno).trim();
                String sellerid = properties.getProperty(prefix + ".sellerid", def_sellerid).trim();
                String charsetname = properties.getProperty(prefix + ".charsetname", def_charsetname).trim();
                String notifyurl = properties.getProperty(prefix + ".notifyurl", def_notifyurl).trim();
                String appprikeybase64 = properties.getProperty(prefix + ".appprikeybase64", def_appprikeybase64).trim();
                String apppubkeybase64 = properties.getProperty(prefix + ".apppubkeybase64", def_apppubkeybase64).trim();
                String alipubkeybase64 = properties.getProperty(prefix + ".alipubkeybase64", def_alipubkeybase64).trim();

                if (appid.isEmpty() || notifyurl.isEmpty() || appprikeybase64.isEmpty()) {
                    logger.log(Level.WARNING, properties + "; has illegal alipay conf by prefix" + prefix);
                    return;
                }
                AliPayElement element = new AliPayElement();
                element.appid = appid;
                element.merchno = merchno;
                element.charsetname = charsetname;
                element.notifyurl = notifyurl;
                element.appprikeybase64 = appprikeybase64;
                element.apppubkeybase64 = apppubkeybase64;
                element.alipubkeybase64 = alipubkeybase64;
                element.sellerid = sellerid.isEmpty() ? merchno : sellerid;
                element.charset = Charset.forName(charsetname);
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
            try {
                final KeyFactory factory = KeyFactory.getInstance("RSA");
                if (this.apppubkeybase64 != null && !this.apppubkeybase64.isEmpty()) {
                    this.pubKey = factory.generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(this.apppubkeybase64)));
                } else {
                    this.pubKey = null;
                }
                PKCS8EncodedKeySpec priPKCS8 = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(this.appprikeybase64));
                this.priKey = factory.generatePrivate(priPKCS8);

                if (this.alipubkeybase64 != null && !this.alipubkeybase64.isEmpty()) {
                    this.aliKey = factory.generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(this.alipubkeybase64)));
                } else {
                    this.aliKey = null;
                }
                return true;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init alipay sslcontext error", e);
                return false;
            }
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }
}
