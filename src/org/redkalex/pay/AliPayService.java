/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.*;
import java.security.spec.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import javax.annotation.Resource;
import org.redkale.util.*;
import org.redkale.convert.json.*;
import static org.redkalex.pay.Pays.*;
import static org.redkalex.pay.PayRetCodes.*;
import org.redkale.service.Local;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
@Comment("支付宝支付服务")
public class AliPayService extends AbstractPayService {

    protected static final String format = "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS"; //yyyy-MM-dd HH:mm:ss

    //配置集合
    protected Map<String, AliPayElement> elements = new HashMap<>();

    @Resource(name = "property.pay.alipay.conf") //支付配置文件路径
    protected String conf = "config.properties";

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource
    protected JsonConvert convert;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = JsonConvert.root();
        this.reloadConfig(Pays.PAYTYPE_ALIPAY);
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short paytype) {
        return paytype == PAYTYPE_ALIPAY && !elements.isEmpty();
    }

    @Override
    @Comment("重新加载配置")
    public void reloadConfig(short paytype) {
        if (this.conf != null && !this.conf.isEmpty()) { //存在支付宝支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in == null) return;
                Properties properties = new Properties();
                properties.load(in);
                in.close();
                this.elements = AliPayElement.create(logger, properties);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init alipay conf error", e);
            }
        }
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

    @Override
    public CompletableFuture<PayPreResponse> prepayAsync(final PayPreRequest request) {
        request.checkVaild();
        //参数说明： https://doc.open.alipay.com/doc2/detail.htm?spm=a219a.7629140.0.0.lMJkw3&treeId=59&articleId=103663&docType=1
        final PayPreResponse result = new PayPreResponse();
        try {
            final AliPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
            result.setAppid(element.appid);
            long now = System.currentTimeMillis();
            // 签约合作者身份ID
            String param = "";
            if (element.merchno == null || element.merchno.isEmpty()) {
                TreeMap<String, String> paramap = new TreeMap<>();
                paramap.put("app_id", element.appid);
                paramap.put("method", "alipay.trade.wap.pay");
                paramap.put("format", "JSON");
                //paramap.put("return_url", "");
                paramap.put("charset", "utf-8");
                paramap.put("sign_type", "RSA2");
                paramap.put("timestamp", Utility.formatTime(now));
                if (request.notifyurl != null && !request.notifyurl.isEmpty()) {
                    paramap.put("notify_url", request.notifyurl);
                }
                final TreeMap<String, String> biz_content = new TreeMap<>();
                if (request.getAttach() != null) biz_content.putAll(request.getAttach());
                biz_content.put("subject", "" + request.getPaytitle());
                biz_content.put("out_trade_no", request.getPayno());
                paramap.put("time_expire", Utility.formatTime(now + request.getTimeoutms()));
                biz_content.put("total_amount", "" + (request.getPaymoney() / 100.0));
                biz_content.put("quit_url", element.notifyurl); //返回url
                biz_content.put("product_code", "QUICK_WAP_WAY");
                biz_content.put("goods_id", "gid-" + request.getPayno());
                biz_content.put("goods_name", request.getPaytitle());
                biz_content.put("quantity", "1");
                biz_content.put("price", "" + (request.getPaymoney() / 100.0));

                paramap.put("biz_content", convert.convertTo(biz_content));
                Signature signature = Signature.getInstance("SHA256WithRSA");
                signature.initSign(element.priKey);
                signature.update(joinMap(paramap).getBytes("UTF-8"));
                paramap.put("sign", URLEncoder.encode(Base64.getEncoder().encodeToString(signature.sign()), "UTF-8"));
                System.out.println(joinMap(paramap));
                return postHttpContentAsync("https://openapi.alipay.com/gateway.do", joinMap(paramap)).thenApply(responseText -> {
                    result.setResponsetext(responseText);
                    System.out.println("返回结果：" + responseText);
                    Map<String, String> resultmap = convert.convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, responseText);

                    return result;
                });
            } else {
                param = "partner=" + "\"" + element.merchno + "\"";
                // 签约卖家支付宝账号(也可用身份ID)
                param += "&seller_id=" + "\"" + element.sellerid + "\"";
                // 商户网站唯一订单号
                param += "&out_trade_no=" + "\"" + request.getPayno() + "\"";
                // 商品名称
                param += "&subject=" + "\"" + request.getPaytitle() + "\"";
                // 商品详情
                param += "&body=" + "\"" + request.getPaybody() + "\"";
                // 商品金额
                param += "&total_fee=" + "\"" + (request.getPaymoney() / 100.0) + "\"";
                // 服务器异步通知页面路径
                param += "&notify_url=" + "\"" + ((request.notifyurl != null && !request.notifyurl.isEmpty()) ? request.notifyurl : element.notifyurl) + "\"";
                // 服务接口名称， 固定值
                param += "&service=\"mobile.securitypay.pay\"";
                // 支付类型， 固定值
                param += "&payment_type=\"1\"";
                // 参数编码， 固定值
                param += "&_input_charset=\"utf-8\"";

                // 设置未付款交易的超时时间
                // 默认30分钟，一旦超时，该笔交易就会自动被关闭。
                // 取值范围：1m～15d。 m-分钟，h-小时，d-天，1c-当天（无论交易何时创建，都在0点关闭）。
                // 该参数数值不接受小数点，如1.5h，可转换为90m。
                param += "&it_b_pay=\"" + request.getTimeoutms() + "m\"";

                Signature signature = Signature.getInstance("SHA256WithRSA");
                signature.initSign(element.priKey);
                signature.update(param.getBytes("UTF-8"));
                param += "&sign=\"" + URLEncoder.encode(Base64.getEncoder().encodeToString(signature.sign()), "UTF-8") + "\"";
                param += "&sign_type=\"RSA2\"";
            }
            final Map<String, String> rmap = new TreeMap<>();
            rmap.put("content", param);
            result.setResult(rmap);
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "prepay_pay_error req=" + request + ", resp=" + result.responsetext, e);
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
        result.setPaytype(request.getPaytype());
        final String rstext = "success";
        Map<String, String> map = request.getAttach();
        final AliPayElement element = elements.get(request.getAppid());
        if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
        result.setPayno(map.getOrDefault("out_trade_no", ""));
        result.setThirdpayno(map.getOrDefault("trade_no", ""));
        if (!checkSign(element, map, null)) return result.retcode(RETPAY_FALSIFY_ERROR).toFuture();
        String state = map.getOrDefault("trade_status", "");
        if ("WAIT_BUYER_PAY".equals(state)) return result.retcode(RETPAY_PAY_WAITING).toFuture();
        if (!"TRADE_SUCCESS".equals(state)) return result.retcode(RETPAY_PAY_FAILED).toFuture();
        result.setPayedmoney((long) (Float.parseFloat(map.get("total_fee")) * 100));
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
            if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("app_id", element.appid);
            map.put("method", "alipay.trade.create");
            map.put("format", "JSON");
            map.put("charset", element.charset);
            map.put("sign_type", "RSA2");
            map.put("timestamp", String.format(format, System.currentTimeMillis()));
            map.put("version", "1.0");
            if (element.notifyurl != null && !element.notifyurl.isEmpty()) map.put("notify_url", element.notifyurl);

            final TreeMap<String, String> biz_content = new TreeMap<>();
            if (request.getAttach() != null) biz_content.putAll(request.getAttach());
            biz_content.put("out_trade_no", request.getPayno());
            biz_content.putIfAbsent("scene", "bar_code");
            biz_content.put("total_amount", "" + (request.getPaymoney() / 100.0));
            biz_content.put("subject", "" + request.getPaytitle());
            biz_content.put("body", request.getPaybody());
            map.put("biz_content", convert.convertTo(biz_content));

            map.put("sign", createSign(element, map, null));

            return postHttpContentAsync("https://openapi.alipay.com/gateway.do", Charset.forName(element.charset), joinMap(map)).thenApply(responseText -> {
                //{"alipay_trade_create_response":{"code":"40002","msg":"Invalid Arguments","sub_code":"isv.invalid-signature","sub_msg":"无效签名"},"sign":"xxxxxxxxxxxx"}
                result.setResponsetext(responseText);
                final InnerCreateResponse resp = convert.convertFrom(InnerCreateResponse.class, responseText);
                resp.responseText = responseText; //原始的返回内容            
                if (!checkSign(element, resp)) return result.retcode(RETPAY_FALSIFY_ERROR);
                final Map<String, String> resultmap = resp.alipay_trade_create_response;
                result.setResult(resultmap);
                if (!"SUCCESS".equalsIgnoreCase(resultmap.get("msg"))) {
                    return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("sub_msg"));
                }
                result.setThirdpayno(resultmap.getOrDefault("trade_no", ""));
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "create_pay_error req=" + request + ", resp=" + result.responsetext, e);
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
            if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("app_id", element.appid);
            map.put("sign_type", "RSA2");
            map.put("charset", element.charset);
            map.put("format", "json");
            map.put("version", "1.0");
            //if (this.notifyurl != null && !this.notifyurl.isEmpty()) map.put("notify_url", this.notifyurl); // 查询不需要
            map.put("timestamp", String.format(format, System.currentTimeMillis()));
            map.put("method", "alipay.trade.query");

            final TreeMap<String, String> biz_content = new TreeMap<>();
            biz_content.put("out_trade_no", request.getPayno());
            map.put("biz_content", convert.convertTo(biz_content));

            map.put("sign", createSign(element, map, null));

            return postHttpContentAsync("https://openapi.alipay.com/gateway.do", Charset.forName(element.charset), joinMap(map)).thenApply(responseText -> {

                result.setResponsetext(responseText);
                final InnerQueryResponse resp = convert.convertFrom(InnerQueryResponse.class, responseText);
                resp.responseText = responseText; //原始的返回内容            
                if (!checkSign(element, resp)) return result.retcode(RETPAY_FALSIFY_ERROR);
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
                result.setPaystatus(paystatus);
                result.setThirdpayno(resultmap.getOrDefault("trade_no", ""));
                result.setPayedmoney((long) (Double.parseDouble(resultmap.get("receipt_amount")) * 100));
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "query_pay_error req=" + request + ", resp=" + result.responsetext, e);
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
            if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("app_id", element.appid);
            map.put("sign_type", "RSA2");
            map.put("charset", element.charset);
            map.put("format", "json");
            map.put("version", "1.0");
            if (element.notifyurl != null && !element.notifyurl.isEmpty()) map.put("notify_url", element.notifyurl);
            map.put("timestamp", String.format(format, System.currentTimeMillis()));
            map.put("method", "alipay.trade.close");

            final TreeMap<String, String> biz_content = new TreeMap<>();
            biz_content.put("out_trade_no", request.getPayno());
            map.put("biz_content", convert.convertTo(biz_content));

            map.put("sign", createSign(element, map, null));

            return postHttpContentAsync("https://openapi.alipay.com/gateway.do", Charset.forName(element.charset), joinMap(map)).thenApply(responseText -> {

                result.setResponsetext(responseText);
                final InnerCloseResponse resp = convert.convertFrom(InnerCloseResponse.class, responseText);
                resp.responseText = responseText; //原始的返回内容            
                if (!checkSign(element, resp)) return result.retcode(RETPAY_FALSIFY_ERROR);
                final Map<String, String> resultmap = resp.alipay_trade_close_response;
                result.setResult(resultmap);
                if (!"SUCCESS".equalsIgnoreCase(resultmap.get("msg"))) {
                    return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("sub_msg"));
                }
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "close_pay_error req=" + request + ", resp=" + result.responsetext, e);
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
            if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("app_id", element.appid);
            map.put("sign_type", "RSA2");
            map.put("charset", element.charset);
            map.put("format", "json");
            map.put("version", "1.0");
            map.put("timestamp", String.format(format, System.currentTimeMillis()));
            map.put("method", "alipay.trade.refund");

            final TreeMap<String, String> biz_content = new TreeMap<>();
            biz_content.put("out_trade_no", request.getPayno());
            biz_content.put("refund_amount", "" + (request.getRefundmoney() / 100.0));
            map.put("biz_content", convert.convertTo(biz_content));

            map.put("sign", createSign(element, map, null));

            return postHttpContentAsync("https://openapi.alipay.com/gateway.do", Charset.forName(element.charset), joinMap(map)).thenApply(responseText -> {

                result.setResponsetext(responseText);
                final InnerCloseResponse resp = convert.convertFrom(InnerCloseResponse.class, responseText);
                resp.responseText = responseText; //原始的返回内容            
                if (!checkSign(element, resp)) return result.retcode(RETPAY_FALSIFY_ERROR);
                final Map<String, String> resultmap = resp.alipay_trade_close_response;
                result.setResult(resultmap);
                if (!"SUCCESS".equalsIgnoreCase(resultmap.get("msg"))) {
                    return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("sub_msg"));
                }
                result.setRefundedmoney((long) (Double.parseDouble(resultmap.get("refund_fee")) * 100));
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "refund_pay_error req=" + request + ", resp=" + result.responsetext, e);
            return result.toFuture();
        }
    }

    @Override
    public PayRefundResponse queryRefund(PayRequest request) {
        return queryRefundAsync(request).join();
    }

    @Override
    public CompletableFuture<PayRefundResponse> queryRefundAsync(PayRequest request) {
        PayQueryResponse queryResponse = query(request);
        final PayRefundResponse response = new PayRefundResponse();
        response.setRetcode(queryResponse.getRetcode());
        response.setRetinfo(queryResponse.getRetinfo());
        response.setResponsetext(queryResponse.getResponsetext());
        response.setResult(queryResponse.getResult());
        if (queryResponse.isSuccess()) {
            response.setRefundedmoney((long) (Double.parseDouble(response.getResult().get("receipt_amount")) * 100));
        }
        return response.toFuture();
    }

    protected boolean checkSign(final PayElement element, InnerResponse response) {
        if (((AliPayElement) element).pubKey == null) return true;
        try {
            String text = response.responseText;
            text = text.substring(text.indexOf(':') + 1, text.indexOf(",\"sign\""));

            Signature signature = Signature.getInstance("SHA256WithRSA");
            signature.initVerify(((AliPayElement) element).pubKey);
            signature.update(text.getBytes(((AliPayElement) element).charset));
            return signature.verify(Base64.getDecoder().decode(response.sign.getBytes()));
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "checkSign error: element =" + element + ", response =" + response);
            return false;
        }
    }

    @Override
    protected String createSign(final PayElement element, Map<String, ?> map, String text) {
        try {
            Signature signature = Signature.getInstance("SHA256WithRSA");
            signature.initSign(((AliPayElement) element).priKey);
            signature.update(joinMap(map).getBytes(((AliPayElement) element).charset));
            return URLEncoder.encode(Base64.getEncoder().encodeToString(signature.sign()), "UTF-8");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected boolean checkSign(final PayElement element, Map<String, ?> map0, String text0) { //支付宝玩另类
        if (((AliPayElement) element).pubKey == null) return true;
        Map<String, String> map = (Map<String, String>) map0;
        String sign = (String) map.remove("sign");
        if (sign == null) return false;
        String sign_type = (String) map.remove("sign_type");
        String text = joinMap(map);
        map.put("sign", sign);
        if (sign_type != null) map.put("sign_type", sign_type);
        try {
            Signature signature = Signature.getInstance("SHA256WithRSA");
            signature.initVerify(((AliPayElement) element).pubKey);
            signature.update(text.getBytes("UTF-8"));
            return signature.verify(Base64.getDecoder().decode(sign.getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
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
        public String charset = "UTF-8"; //字符集 

        // pay.alipay.[x].appid
        public String appid = "";  //APP应用ID

        // pay.alipay.[x].notifyurl
        //public String notifyurl = ""; //回调url
        // pay.alipay.[x].signcertkey
        public String signcertkey = ""; //签名算法需要用到的密钥

        // pay.alipay.[x].verifycertkey
        public String verifycertkey = ""; //公钥

        //        
        protected PrivateKey priKey; //私钥

        //  
        protected PublicKey pubKey; //公钥

        public static Map<String, AliPayElement> create(Logger logger, Properties properties) {
            String def_appid = properties.getProperty("pay.alipay.appid", "").trim();
            String def_merchno = properties.getProperty("pay.alipay.merchno", "").trim();
            String def_sellerid = properties.getProperty("pay.alipay.sellerid", "").trim();
            String def_charset = properties.getProperty("pay.alipay.charset", "UTF-8").trim();
            String def_notifyurl = properties.getProperty("pay.alipay.notifyurl", "").trim();
            String def_signcertkey = properties.getProperty("pay.alipay.signcertkey", "").trim();
            String def_verifycertkey = properties.getProperty("pay.alipay.verifycertkey", "").trim();

            final Map<String, AliPayElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("pay.alipay.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String merchno = properties.getProperty(prefix + ".merchno", def_merchno).trim();
                String sellerid = properties.getProperty(prefix + ".sellerid", def_sellerid).trim();
                String charset = properties.getProperty(prefix + ".charset", def_charset).trim();
                String notifyurl = properties.getProperty(prefix + ".notifyurl", def_notifyurl).trim();
                String signcertkey = properties.getProperty(prefix + ".signcertkey", def_signcertkey).trim();
                String verifycertkey = properties.getProperty(prefix + ".verifycertkey", def_verifycertkey).trim();

                if (appid.isEmpty() || notifyurl.isEmpty() || signcertkey.isEmpty()) {
                    logger.log(Level.WARNING, properties + "; has illegal alipay conf by prefix" + prefix);
                    return;
                }
                AliPayElement element = new AliPayElement();
                element.appid = appid;
                element.merchno = merchno;
                element.sellerid = sellerid.isEmpty() ? merchno : sellerid;
                element.charset = charset;
                element.notifyurl = notifyurl;
                element.signcertkey = signcertkey;
                element.verifycertkey = verifycertkey;
                if (element.initElement(logger, null)) {
                    map.put(appid, element);
                    if (def_appid.equals(appid)) map.put("", element);
                }
            });
            return map;
        }

        @Override
        public boolean initElement(Logger logger, File home) {
            try {
                final KeyFactory factory = KeyFactory.getInstance("RSA2");
                if (this.verifycertkey != null && !this.verifycertkey.isEmpty()) {
                    this.pubKey = factory.generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(this.verifycertkey)));
                } else {
                    this.pubKey = null;
                }
                PKCS8EncodedKeySpec priPKCS8 = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(this.signcertkey));
                this.priKey = factory.generatePrivate(priPKCS8);
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
