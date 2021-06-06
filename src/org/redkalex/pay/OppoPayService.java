/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import java.util.regex.*;
import javax.annotation.Resource;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.Local;
import org.redkale.util.*;
import static org.redkalex.pay.PayRetCodes.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public class OppoPayService extends AbstractPayService {

    protected static final String format = "%1$tY%1$tm%1$td%1$tH%1$tM%1$tS"; //yyyyMMddHHmmss

    protected static final Pattern PAYXML = Pattern.compile("<([^/>]+)>(.+)</.+>"); // "<([^/>]+)><!\\[CDATA\\[(.+)\\]\\]></.+>"

    //配置集合
    protected Map<String, OppoPayElement> elements = new HashMap<>();

    @Resource(name = "property.pay.oppo.conf") //支付配置文件路径
    protected String conf = "config.properties";

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource
    protected JsonConvert convert;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = JsonConvert.root();
        this.reloadConfig(Pays.PAYTYPE_OPPO);
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short paytype) {
        return paytype == Pays.PAYTYPE_OPPO && !elements.isEmpty();
    }

    @Override
    @Comment("重新加载配置")
    public void reloadConfig(short paytype) {
        if (this.conf != null && !this.conf.isEmpty()) { //存在微信支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in == null) return;
                Properties properties = new Properties();
                properties.load(in);
                in.close();
                this.elements = OppoPayElement.create(logger, properties, home);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init weixinpay conf error", e);
            }
        }
    }

    public void setPayElements(Map<String, OppoPayElement> elements) {
        this.elements = elements;
    }

    public void putPayElements(Map<String, OppoPayElement> elements) {
        this.elements.putAll(elements);
    }

    @Override
    public OppoPayElement getPayElement(String appid) {
        return this.elements.get(appid);
    }

    public void setPayElement(String appid, OppoPayElement element) {
        this.elements.put(appid, element);
    }

    public boolean existsPayElement(String appid) {
        return this.elements != null && this.elements.containsKey(appid);
    }

    @Override
    public PayPreResponse prepay(PayPreRequest request) {
        return prepayAsync(request).join();
    }

    @Override
    public CompletableFuture<PayPreResponse> prepayAsync(PayPreRequest request) {
        request.checkVaild();
        final PayPreResponse result = new PayPreResponse();
        try {
            final OppoPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
            result.setAppid(element.appid);
            final long timestamp = System.currentTimeMillis();
            final TreeMap<String, String> map = new TreeMap<>();
            if (request.getAttach() != null) map.putAll(request.getAttach()); //含openId、appVersion、engineVersion
            map.put("appId", element.appid);
            map.put("timestamp", "" + timestamp);
            map.put("productName", request.getPaytitle());
            map.put("productDesc", request.getPaybody());
            map.put("cpOrderId", request.getPayno());
            map.put("price", "" + request.getPaymoney());
            map.put("count", "1");
            map.put("currency", "CNY");
            map.put("ip", request.getClientAddr());
            map.put("callBackUrl", ((request.notifyurl != null && !request.notifyurl.isEmpty()) ? request.notifyurl : element.notifyurl));
            map.put("sign", createSign(element, map, null));

            return postHttpContentAsync("	https://jits.open.oppomobile.com/jitsopen/api/pay/v1.0/preOrder", joinMap(map)).thenApply(responseText -> {
                result.setResponsetext(responseText);

                OppoPrePayResult preresult = JsonConvert.root().convertFrom(OppoPrePayResult.class, responseText);
                if (!"200".equals(preresult.code)) return result.retcode(RETPAY_PAY_ERROR);
                if (preresult.data == null) return result.retcode(RETPAY_PAY_ERROR);
                if (preresult.data.orderNo == null) return result.retcode(RETPAY_PAY_ERROR);
                result.setThirdpayno(preresult.data.orderNo);
                final Map<String, String> retmap = new TreeMap<>();
                retmap.put("appId", element.appid);
                retmap.put("token", map.get("token"));
                retmap.put("timestamp", "" + timestamp);
                retmap.put("orderNo", preresult.data.orderNo);
                final TreeMap<String, String> signmap = new TreeMap<>();
                signmap.put("appKey ", element.appkey);
                signmap.put("orderNo ", preresult.data.orderNo);
                signmap.put("timestamp ", "" + timestamp);
                retmap.put("paySign", createSign(element, signmap, null));
                result.setResult(retmap);
                return result;
            });
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "prepay_pay_error req=" + request + ", resp=" + result.responsetext, e);
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
        result.setPaytype(request.getPaytype());
        final String rstext = "<xml><return_code><![CDATA[SUCCESS]]></return_code><return_msg><![CDATA[OK]]></return_msg></xml>";
        Map<String, String> map = null;
        String appid = request.getAppid();
        if (appid == null || appid.isEmpty()) appid = map.getOrDefault("appid", "");
        final OppoPayElement element = elements.get(appid);
        if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
        result.setPayno(map.getOrDefault("out_trade_no", ""));
        result.setThirdpayno(map.getOrDefault("transaction_id", ""));
        if ("NOTPAY".equals(map.get("return_code"))) return result.retcode(RETPAY_PAY_WAITING).notifytext(rstext).toFuture();
        if (!"SUCCESS".equals(map.get("return_code"))) return result.retcode(RETPAY_PAY_FAILED).notifytext(rstext).toFuture();
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        if (!checkSign(element, map, null)) return result.retcode(RETPAY_FALSIFY_ERROR).notifytext(rstext).toFuture();
        String state = map.get("trade_state");
        if (state == null && "SUCCESS".equals(map.get("result_code")) && Long.parseLong(map.get("total_fee")) > 0) {
            state = "SUCCESS";
            result.setPayedmoney(Long.parseLong(map.get("total_fee")));
        }
        if (!"SUCCESS".equals(state)) return result.retcode(RETPAY_PAY_FAILED).notifytext(rstext).toFuture();
        return result.notifytext(rstext).toFuture();
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        return createAsync(request).join();
    }

    @Override
    public CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        return queryAsync(request).join();
    }

    @Override
    public CompletableFuture<PayQueryResponse> queryAsync(PayRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PayResponse close(PayCloseRequest request) {
        return closeAsync(request).join();
    }

    @Override
    public CompletableFuture<PayResponse> closeAsync(PayCloseRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        return refundAsync(request).join();
    }

    @Override
    public CompletableFuture<PayRefundResponse> refundAsync(PayRefundRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PayRefundResponse queryRefund(PayRequest request) {
        return queryRefundAsync(request).join();
    }

    @Override
    public CompletableFuture<PayRefundResponse> queryRefundAsync(PayRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected String createSign(final PayElement element, Map<String, ?> map, String text) { //计算签名
        final StringBuilder sb = new StringBuilder();
        map.forEach((x, y) -> {
            if (!((String) y).isEmpty()) sb.append(x).append('=').append(y).append('&');
        });
        try {
            KeyFactory keyf = KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec spec = map.containsKey("orderNo") ? ((OppoPayElement) element).appkeyPKCS8 : ((OppoPayElement) element).signkeyPKCS8;
            PrivateKey priKey = keyf.generatePrivate(spec);
            java.security.Signature signature = java.security.Signature.getInstance("SHA256WithRSA");

            signature.initSign(priKey);
            signature.update(sb.toString().getBytes(StandardCharsets.UTF_8));

            byte[] signed = signature.sign();
            return Utility.binToHexString(Base64.getEncoder().encode(signed));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected boolean checkSign(final PayElement element, Map<String, ?> map, String text) {  //验证签名
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        String sign = (String) map.remove("sign");
        final StringBuilder sb = new StringBuilder();
        map.forEach((x, y) -> {
            if (!((String) y).isEmpty()) sb.append(x).append('=').append(y).append('&');
        });
        try {
            KeyFactory keyf = KeyFactory.getInstance("RSA");
            PrivateKey priKey = keyf.generatePrivate(((OppoPayElement) element).signkeyPKCS8);
            java.security.Signature signature = java.security.Signature.getInstance("SHA256WithRSA");
            signature.initSign(priKey);
            signature.update(sb.toString().getBytes(StandardCharsets.UTF_8));

            byte[] signed = signature.sign();
            return sign.equals(Utility.binToHexString(Base64.getEncoder().encode(signed)));
        } catch (Exception e) {
            return false;
        }
    }

    public static class OppoPrePayResult {

        public String code;

        public String msg;

        public OppoPrePayResultData data;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class OppoPrePayResultData {

        public String appId;

        public String cpOrderId;

        public String orderNo;

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class OppoPayElement extends PayElement {

        // pay.oppo.[x].appid
        public String appid = "";  //APP应用ID

        // pay.oppo.[x].appkey
        public String appkey = "";  //支付签名用到的密钥

        // pay.oppo.[x].signkey
        public String signkey = ""; //签名算法需要用到的密钥

        public PKCS8EncodedKeySpec appkeyPKCS8;

        public PKCS8EncodedKeySpec signkeyPKCS8;

        public static Map<String, OppoPayElement> create(Logger logger, Properties properties, File home) {
            String def_appid = properties.getProperty("pay.oppo.appid", "").trim();
            String def_appkey = properties.getProperty("pay.oppo.appkey", "").trim();
            String def_signkey = properties.getProperty("pay.oppo.signkey", "").trim();
            String def_notifyurl = properties.getProperty("pay.oppo.notifyurl", "").trim();

            final Map<String, OppoPayElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("pay.oppo.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String appkey = properties.getProperty(prefix + ".appkey", def_appkey).trim();
                String signkey = properties.getProperty(prefix + ".signkey", def_signkey).trim();
                String notifyurl = properties.getProperty(prefix + ".notifyurl", def_notifyurl).trim();

                if (appid.isEmpty() || notifyurl.isEmpty() || appkey.isEmpty() || signkey.isEmpty()) {
                    logger.log(Level.WARNING, properties + "; has illegal oppopay conf by prefix" + prefix);
                    return;
                }
                OppoPayElement element = new OppoPayElement();
                element.appid = appid;
                element.appkey = appkey;
                element.signkey = signkey;
                element.notifyurl = notifyurl;
                if (element.initElement(logger, home)) {
                    map.put(appid, element);
                    if (def_appid.equals(appid)) map.put("", element);
                }
            });
            return map;
        }

        @Override
        public boolean initElement(Logger logger, File home) {
            appkeyPKCS8 = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(appkey));
            signkeyPKCS8 = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(signkey));
            return true;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

}
