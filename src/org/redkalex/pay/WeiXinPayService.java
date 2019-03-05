/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.security.*;
import java.util.*;
import java.util.logging.*;
import java.util.regex.*;
import javax.annotation.Resource;
import javax.net.ssl.*;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.*;
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

    protected static final String format = "%1$tY%1$tm%1$td%1$tH%1$tM%1$tS"; //yyyyMMddHHmmss

    protected static final Pattern PAYXML = Pattern.compile("<([^/>]+)>(.+)</.+>"); // "<([^/>]+)><!\\[CDATA\\[(.+)\\]\\]></.+>"

    //配置集合
    protected Map<String, WeixinPayElement> elements = new HashMap<>();

    @Resource(name = "property.pay.weixin.conf") //支付配置文件路径
    protected String conf = "config.properties";

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource
    protected JsonConvert convert;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = JsonConvert.root();
        if (this.conf != null && !this.conf.isEmpty()) { //存在微信支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in == null) return;
                Properties properties = new Properties();
                properties.load(in);
                in.close();
                this.elements = WeixinPayElement.create(logger, properties, home);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init weixinpay conf error", e);
            }
        }
    }

    public void setPayElements(Map<String, WeixinPayElement> elements) {
        this.elements = elements;
    }

    public void putPayElements(Map<String, WeixinPayElement> elements) {
        this.elements.putAll(elements);
    }

    public WeixinPayElement getPayElement(String appid) {
        return this.elements.get(appid);
    }

    public void setPayElement(String appid, WeixinPayElement element) {
        this.elements.put(appid, element);
    }

    public boolean existsPayElement(String appid) {
        return this.elements != null && this.elements.containsKey(appid);
    }

    /**
     * 手机支付或者微信公众号支付时调用
     *
     * @param request PayPreRequest
     *
     * @return PayPreResponse
     */
    @Override
    public PayPreResponse prepay(final PayPreRequest request) {
        request.checkVaild();
        final PayPreResponse result = new PayPreResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR);
            result.setAppid(element.appid);
            final TreeMap<String, String> map = new TreeMap<>();
            if (request.getAttach() != null) map.putAll(request.getAttach());
            map.put("appid", element.appid);
            map.put("mch_id", element.merchno);
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.put("body", request.getPaybody());
            //map.put("attach", "" + payid);
            map.put("out_trade_no", request.getPayno());
            map.put("total_fee", "" + request.getPaymoney());
            map.put("spbill_create_ip", request.getClientAddr());
            map.put("time_expire", String.format(format, System.currentTimeMillis() + request.getTimeoutms() * 60 * 1000));
            map.put("notify_url", ((request.notifyurl != null && !request.notifyurl.isEmpty()) ? request.notifyurl : element.notifyurl));
            map.put("trade_type", request.getPayway() == PAYWAY_WEB ? "JSAPI" : "APP");
            //trade_type=JSAPI，openid参数必传，用户在商户appid下的唯一标识
            if (request.getPayway() == PAYWAY_WEB && !map.containsKey("openid")) return result.retcode(RETPAY_OPENID_ERROR);
            map.put("sign", createSign(element, map));

            final String responseText = Utility.postHttpContent("https://api.mch.weixin.qq.com/pay/unifiedorder", formatMapToXML(map));
            result.setResponsetext(responseText);

            Map<String, String> resultmap = formatXMLToMap(responseText);
            if (!"SUCCESS".equals(resultmap.get("return_code"))) return result.retcode(RETPAY_PAY_ERROR);
            if (!checkSign(element, resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            /**
             * "appId" : "wx2421b1c4370ec43b", //公众号名称，由商户传入 "timeStamp":" 1395712654", //时间戳，自1970年以来的秒数 "nonceStr" : "e61463f8efa94090b1f366cccfbbb444", //随机串 "package" :
             * "prepay_id=u802345jgfjsdfgsdg888", "signType" : "MD5", //微信签名方式: "paySign" : "70EA570631E4BB79628FBCA90534C63FF7FADD89" //微信签名
             */
            final String timestamp = Long.toString(System.currentTimeMillis() / 1000);
            final String noncestr = Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime());
            final Map<String, String> retmap = new TreeMap<>();
            if (request.getPayway() == PAYWAY_WEB) {
                retmap.put("appId", element.appid);
                retmap.put("timeStamp", timestamp);
                retmap.put("nonceStr", noncestr);
                retmap.put("package", "prepay_id=" + resultmap.get("prepay_id"));
                retmap.put("signType", "MD5");
                retmap.put("paySign", createSign(element, retmap));
            } else {
                retmap.put("appid", element.appid);
                retmap.put("partnerid", element.merchno);
                retmap.put("prepayid", resultmap.get("prepay_id"));
                retmap.put("timestamp", timestamp);
                retmap.put("noncestr", noncestr);
                retmap.put("package", "Sign=WXPay"); //固定值            
                retmap.put("sign", createSign(element, retmap));
            }
            result.setResult(retmap);

        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "prepay_pay_error req=" + request + ", resp=" + result.responsetext, e);
        }
        return result;
    }

    /**
     * &lt;xml&gt;
     * &lt;appid&gt;&lt;![CDATA[wx2421b1c4370ec43b]]&gt;&lt;/appid&gt;
     * &lt;attach&gt;&lt;![CDATA[支付测试]]&gt;&lt;/attach&gt;
     * &lt;bank_type&gt;&lt;![CDATA[CFT]]&gt;&lt;/bank_type&gt;
     * &lt;fee_type&gt;&lt;![CDATA[CNY]]&gt;&lt;/fee_type&gt;
     * &lt;is_subscribe&gt;&lt;![CDATA[Y]]&gt;&lt;/is_subscribe&gt;
     * &lt;mch_id&gt;&lt;![CDATA[10000100]]&gt;&lt;/mch_id&gt;
     * &lt;nonce_str&gt;&lt;![CDATA[5d2b6c2a8db53831f7eda20af46e531c]]&gt;&lt;/nonce_str&gt;
     * &lt;openid&gt;&lt;![CDATA[oUpF8uMEb4qRXf22hE3X68TekukE]]&gt;&lt;/openid&gt;
     * &lt;out_trade_no&gt;&lt;![CDATA[1409811653]]&gt;&lt;/out_trade_no&gt;
     * &lt;result_code&gt;&lt;![CDATA[SUCCESS]]&gt;&lt;/result_code&gt;
     * &lt;return_code&gt;&lt;![CDATA[SUCCESS]]&gt;&lt;/return_code&gt;
     * &lt;sign&gt;&lt;![CDATA[B552ED6B279343CB493C5DD0D78AB241]]&gt;&lt;/sign&gt;
     * &lt;sub_mch_id&gt;&lt;![CDATA[10000100]]&gt;&lt;/sub_mch_id&gt;
     * &lt;time_end&gt;&lt;![CDATA[20140903131540]]&gt;&lt;/time_end&gt;
     * &lt;total_fee&gt;1&lt;/total_fee&gt;
     * &lt;trade_type&gt;&lt;![CDATA[JSAPI]]&gt;&lt;/trade_type&gt;
     * &lt;transaction_id&gt;&lt;![CDATA[1004400740201409030005092168]]&gt;&lt;/transaction_id&gt;
     * &lt;/xml&gt;
     *
     * @param request PayNotifyRequest
     *
     * @return PayNotifyResponse
     */
    @Override
    public PayNotifyResponse notify(PayNotifyRequest request) {
        request.checkVaild();
        final PayNotifyResponse result = new PayNotifyResponse();
        result.setPaytype(request.getPaytype());
        final String rstext = "<xml><return_code><![CDATA[SUCCESS]]></return_code><return_msg><![CDATA[OK]]></return_msg></xml>";
        Map<String, String> map = formatXMLToMap(request.getText());
        String appid = request.getAppid();
        if (appid == null || appid.isEmpty()) appid = map.getOrDefault("appid", "");
        final WeixinPayElement element = elements.get(appid);
        if (element == null) return result.retcode(RETPAY_CONF_ERROR);
        result.setPayno(map.getOrDefault("out_trade_no", ""));
        result.setThirdpayno(map.getOrDefault("transaction_id", ""));
        if ("NOTPAY".equals(map.get("return_code"))) return result.retcode(RETPAY_PAY_WAITING).notifytext(rstext);
        if (!"SUCCESS".equals(map.get("return_code"))) return result.retcode(RETPAY_PAY_FAILED).notifytext(rstext);
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        if (!checkSign(element, map)) return result.retcode(RETPAY_FALSIFY_ERROR).notifytext(rstext);
        String state = map.get("trade_state");
        if (state == null && "SUCCESS".equals(map.get("result_code")) && Long.parseLong(map.get("total_fee")) > 0) {
            state = "SUCCESS";
        }
        if (!"SUCCESS".equals(state)) return result.retcode(RETPAY_PAY_FAILED).notifytext(rstext);
        return result.notifytext(rstext);
    }

    /**
     * 网页支付
     *
     * @param request PayCreatRequest
     *
     * @return PayCreatResponse
     */
    @Override
    public PayCreatResponse create(final PayCreatRequest request) {
        request.checkVaild();
        final PayCreatResponse result = new PayCreatResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR);
            final TreeMap<String, String> map = new TreeMap<>();
            if (request.getAttach() != null) map.putAll(request.getAttach());
            map.put("appid", element.appid);
            map.put("mch_id", element.merchno);
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.put("body", request.getPaybody());
            //map.put("attach", "" + payid);
            map.put("out_trade_no", request.getPayno());
            map.put("total_fee", "" + request.getPaymoney());
            map.put("spbill_create_ip", request.getClientAddr());
            map.put("time_expire", String.format(format, System.currentTimeMillis() + request.getPaytimeout() * 1000));
            map.put("notify_url", element.notifyurl);
            map.put("sign", createSign(element, map));

            final String responseText = Utility.postHttpContent("https://api.mch.weixin.qq.com/pay/unifiedorder", formatMapToXML(map));
            result.setResponsetext(responseText);

            Map<String, String> resultmap = formatXMLToMap(responseText);
            if (!"SUCCESS".equals(resultmap.get("return_code"))) return result.retcode(RETPAY_PAY_ERROR);
            if (!checkSign(element, resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            /**
             * "appId" : "wx2421b1c4370ec43b", //公众号名称，由商户传入 "timeStamp":" 1395712654", //时间戳，自1970年以来的秒数 "nonceStr" : "e61463f8efa94090b1f366cccfbbb444", //随机串 "package" :
             * "prepay_id=u802345jgfjsdfgsdg888", "signType" : "MD5", //微信签名方式: "paySign" : "70EA570631E4BB79628FBCA90534C63FF7FADD89" //微信签名
             */
            final Map<String, String> rmap = new TreeMap<>();
            result.setResult(rmap);
            rmap.put("appId", element.appid);
            rmap.put("timeStamp", Long.toString(System.currentTimeMillis() / 1000));
            rmap.put("nonceStr", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            rmap.put("package", "prepay_id=" + resultmap.get("prepay_id"));
            rmap.put("signType", "MD5");
            rmap.put("paySign", createSign(element, rmap));
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "create_pay_error req=" + request + ", resp=" + result.responsetext, e);
        }
        return result;
    }

    /**
     * 回调支付接口返回的结果
     * &lt; p &gt;
     * &lt; xml &gt;
     * &lt; appid &gt;&lt; ![CDATA[wx4ad12c89818dd981]] &gt;&lt; /appid &gt;
     * &lt; attach &gt;&lt; ![CDATA[10000070334]] &gt;&lt; /attach &gt;
     * &lt; bank_type &gt;&lt; ![CDATA[ICBC_DEBIT]] &gt;&lt; /bank_type &gt;
     * &lt; cash_fee &gt;&lt; ![CDATA[10]] &gt;&lt; /cash_fee &gt;
     * &lt; fee_type &gt;&lt; ![CDATA[CNY]] &gt;&lt; /fee_type &gt;
     * &lt; is_subscribe &gt;&lt; ![CDATA[Y]] &gt;&lt; /is_subscribe &gt;
     * &lt; mch_id &gt;&lt; ![CDATA[1241384602]] &gt;&lt; /mch_id &gt;
     * &lt; nonce_str &gt;&lt; ![CDATA[14d69ac6d6525f27dc9bcbebc]] &gt;&lt; /nonce_str &gt;
     * &lt; openid &gt;&lt; ![CDATA[ojEVbsyDUzGqlgX3eDgmAMaUDucA]] &gt;&lt; /openid &gt;
     * &lt; out_trade_no &gt;&lt; ![CDATA[1000072334]] &gt;&lt; /out_trade_no &gt;
     * &lt; result_code &gt;&lt; ![CDATA[SUCCESS]] &gt;&lt; /result_code &gt;
     * &lt; return_code &gt;&lt; ![CDATA[SUCCESS]] &gt;&lt; /return_code &gt;
     * &lt; sign &gt;&lt; ![CDATA[60D95E25EA9C4F54BD1020952303C4E2]] &gt;&lt; /sign &gt;
     * &lt; time_end &gt;&lt; ![CDATA[20150519085546]] &gt;&lt; /time_end &gt;
     * &lt; total_fee &gt;10&lt; /total_fee &gt;
     * &lt; trade_type &gt;&lt; ![CDATA[JSAPI]] &gt;&lt; /trade_type &gt;
     * &lt; transaction_id &gt;&lt; ![CDATA[1009630061201505190139511926]] &gt;&lt; /transaction_id &gt;
     * &lt; /xml &gt;
     *
     * @param request PayRequest
     *
     * @return PayQueryResponse
     */
    @Override
    public PayQueryResponse query(final PayRequest request) {
        request.checkVaild();
        final PayQueryResponse result = new PayQueryResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR);
            final Map<String, String> map = new TreeMap<>();
            map.put("appid", element.appid);
            map.put("mch_id", element.merchno);
            map.put("out_trade_no", request.getPayno());
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.put("sign", createSign(element, map));

            final String responseText = Utility.postHttpContent("https://api.mch.weixin.qq.com/pay/orderquery", formatMapToXML(map));
            result.setResponsetext(responseText);

            final Map<String, String> resultmap = formatXMLToMap(responseText);
            result.setResult(resultmap);

            String state = resultmap.getOrDefault("trade_state", "");
            if (state.isEmpty() && "SUCCESS".equals(resultmap.get("result_code")) && Long.parseLong(resultmap.get("total_fee")) > 0) {
                state = "SUCCESS";
            }
            if (!checkSign(element, resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            if (state.isEmpty()) logger.warning("weixin.pay.query = " + resultmap);

            //trade_state 支付状态: SUCCESS—支付成功 REFUND—转入退款 NOTPAY—未支付 CLOSED—已关闭 REVOKED—已撤销（刷卡支付） USERPAYING--用户支付中 PAYERROR--支付失败(其他原因，如银行返回失败)
            short paystatus = PAYSTATUS_PAYNO;
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
            result.setPaystatus(paystatus);
            result.setThirdpayno(resultmap.getOrDefault("transaction_id", ""));
            result.setPayedmoney(Long.parseLong(resultmap.getOrDefault("total_fee", "0")));
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "query_pay_error req=" + request + ", resp=" + result.responsetext, e);
        }
        return result;
    }

    @Override
    public PayResponse close(final PayCloseRequest request) {
        request.checkVaild();
        final PayResponse result = new PayResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR);
            Map<String, String> map = new TreeMap<>();
            map.put("appid", element.appid);
            map.put("mch_id", element.merchno);
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.put("out_trade_no", request.getPayno());
            map.put("sign", createSign(element, map));

            final String responseText = Utility.postHttpContent("https://api.mch.weixin.qq.com/pay/closeorder", formatMapToXML(map));
            result.setResponsetext(responseText);

            Map<String, String> resultmap = formatXMLToMap(responseText);
            if (!"SUCCESS".equals(resultmap.get("return_code"))) return result.retcode(RETPAY_PAY_ERROR);
            if (!checkSign(element, resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            result.setResult(resultmap);

        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "close_pay_error req=" + request + ", resp=" + result.responsetext, e);
        }
        return result;
    }

    //https://pay.weixin.qq.com/wiki/doc/api/micropay.php?chapter=9_4
    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        request.checkVaild();
        final PayRefundResponse result = new PayRefundResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR);
            final TreeMap<String, String> map = new TreeMap<>();
            map.put("appid", element.appid);
            map.put("mch_id", element.merchno);
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.put("out_trade_no", request.getPayno());
            map.put("total_fee", "" + request.getPaymoney());
            map.put("out_refund_no", request.getRefundno());
            map.put("refund_fee", "" + request.getRefundmoney());
            map.put("op_user_id", element.merchno);
            map.put("sign", createSign(element, map));

            final String responseText = Utility.postHttpContent(element.paySSLContext, "https://api.mch.weixin.qq.com/secapi/pay/refund", formatMapToXML(map));
            result.setResponsetext(responseText);

            Map<String, String> resultmap = formatXMLToMap(responseText);
            if (!"SUCCESS".equals(resultmap.get("return_code"))) return result.retcode(RETPAY_REFUND_ERROR);
            if (!checkSign(element, resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            result.setRefundedmoney(Long.parseLong(resultmap.get("refund_fee")));
        } catch (Exception e) {
            result.setRetcode(RETPAY_REFUND_ERROR);
            logger.log(Level.WARNING, "refund_pay_error req=" + request + ", resp=" + result.responsetext, e);
        }
        return result;
    }

    @Override
    public PayRefundResponse queryRefund(PayRequest request) {
        request.checkVaild();
        final PayRefundResponse result = new PayRefundResponse();
        try {
            final WeixinPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR);
            final Map<String, String> map = new TreeMap<>();
            map.put("appid", element.appid);
            map.put("mch_id", element.merchno);
            map.put("out_trade_no", request.getPayno());
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.put("sign", createSign(element, map));

            final String responseText = Utility.postHttpContent("https://api.mch.weixin.qq.com/pay/refundquery", formatMapToXML(map));
            result.setResponsetext(responseText);

            final Map<String, String> resultmap = formatXMLToMap(responseText);
            result.setResult(resultmap);

            if (!"SUCCESS".equals(resultmap.get("return_code"))) return result.retcode(RETPAY_PAY_ERROR);
            if (!checkSign(element, resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            //trade_state SUCCESS—退款成功 FAIL—退款失败 PROCESSING—退款处理中 NOTSURE—未确定，需要商户原退款单号重新发起 
            //CHANGE—转入代发，退款到银行发现用户的卡作废或者冻结了，导致原路退款银行卡失败，资金回流到商户的现金帐号，需要商户人工干预，通过线下或者财付通转账的方式进行退款。

            result.setResult(resultmap);
            result.setRefundedmoney(Long.parseLong(resultmap.get("refund_fee_$n")));
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "query_pay_error req=" + request + ", resp=" + result.responsetext, e);
        }
        return result;
    }

    @Override
    protected String createSign(final PayElement element, Map<String, ?> map) throws Exception { //计算签名
        final StringBuilder sb = new StringBuilder();
        map.forEach((x, y) -> {
            if (!((String) y).isEmpty()) sb.append(x).append('=').append(y).append('&');
        });
        sb.append("key=").append(((WeixinPayElement) element).signkey);
        return Utility.binToHexString(MessageDigest.getInstance("MD5").digest(sb.toString().getBytes())).toUpperCase();
    }

    @Override
    protected boolean checkSign(final PayElement element, Map<String, ?> map) {  //验证签名
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        String sign = (String) map.remove("sign");
        final StringBuilder sb = new StringBuilder();
        map.forEach((x, y) -> {
            if (!((String) y).isEmpty()) sb.append(x).append('=').append(y).append('&');
        });
        sb.append("key=").append(((WeixinPayElement) element).signkey);
        try {
            return sign.equals(Utility.binToHexString(MessageDigest.getInstance("MD5").digest(sb.toString().getBytes())).toUpperCase());
        } catch (Exception e) {
            return false;
        }
    }

    protected static String formatMapToXML(final Map<String, String> map) {
        final StringBuilder sb = new StringBuilder();
        sb.append("<xml>");
        map.forEach((x, y) -> sb.append('<').append(x).append('>').append(y.replace("<", "&lt;").replace(">", "&gt;").replace("&", "&amp;")).append("</").append(x).append('>'));
        sb.append("</xml>");
        return sb.toString();
    }

    public static Map<String, String> formatXMLToMap(final String xml) {
        Map<String, String> map = new TreeMap<>();
        Matcher m = PAYXML.matcher(xml.substring(xml.indexOf('>') + 1));
        while (m.find()) {
            String val = m.group(2);
            if (val.startsWith("<![CDATA[")) val = val.substring("<![CDATA[".length(), val.length() - 3);
            map.put(m.group(1), val);
        }
        return map;
    }

    public static class WeixinPayElement extends PayElement {

        // pay.weixin.[x].merchno
        public String merchno = ""; //商户ID

        // pay.weixin.[x].submerchno
        public String submerchno = ""; //子商户ID，受理模式必填

        // pay.weixin.[x].appid
        public String appid = "";  //APP应用ID

        // pay.weixin.[x].notifyurl
        //public String notifyurl = ""; //回调url
        // pay.weixin.[x].signkey
        public String signkey = ""; //签名算法需要用到的密钥

        // pay.weixin.[x].certpwd
        public String certpwd = ""; //HTTP证书的密码，默认值等于【商户ID】

        // pay.weixin.[x].certpath
        public String certpath = "";  //apiclient_cert.p12 HTTP证书在服务器中的路径，用来加载证书用, 不是/开头且没有:字符，视为{APP_HOME}/conf相对下的路径

        // pay.weixin.[x].certbase64
        public String certbase64 = "";  //证书内容，存在的话则不取certpath文件中的内容

        //
        protected SSLContext paySSLContext;

        public static Map<String, WeixinPayElement> create(Logger logger, Properties properties, File home) {
            String def_appid = properties.getProperty("pay.weixin.appid", "").trim();
            String def_merchno = properties.getProperty("pay.weixin.merchno", "").trim();
            String def_submerchno = properties.getProperty("pay.weixin.submerchno", "").trim();
            String def_notifyurl = properties.getProperty("pay.weixin.notifyurl", "").trim();
            String def_signkey = properties.getProperty("pay.weixin.signkey", "").trim();
            String def_certpwd = properties.getProperty("pay.weixin.certpwd", "").trim();
            String def_certpath = properties.getProperty("pay.weixin.certpath", "").trim();
            String def_certbase64 = properties.getProperty("pay.weixin.certbase64", "").trim();

            final Map<String, WeixinPayElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("pay.weixin.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String merchno = properties.getProperty(prefix + ".merchno", def_merchno).trim();
                String submerchno = properties.getProperty(prefix + ".submerchno", def_submerchno).trim();
                String notifyurl = properties.getProperty(prefix + ".notifyurl", def_notifyurl).trim();
                String signkey = properties.getProperty(prefix + ".signkey", def_signkey).trim();
                String certpwd = properties.getProperty(prefix + ".certpwd", def_certpwd).trim();
                String certpath = properties.getProperty(prefix + ".certpath", def_certpath).trim();
                String certbase64 = properties.getProperty(prefix + ".certbase64", def_certbase64).trim();

                if (appid.isEmpty() || merchno.isEmpty() || notifyurl.isEmpty() || signkey.isEmpty() || (certpath.isEmpty() && certbase64.isEmpty())) {
                    logger.log(Level.WARNING, properties + "; has illegal weixinpay conf by prefix" + prefix);
                    return;
                }
                WeixinPayElement element = new WeixinPayElement();
                element.appid = appid;
                element.merchno = merchno;
                element.submerchno = submerchno;
                element.notifyurl = notifyurl;
                element.signkey = signkey;
                element.certpwd = certpwd;
                element.certpath = certpath;
                element.certbase64 = certbase64;
                if (element.initElement(logger, home)) {
                    map.put(appid, element);
                    if (def_appid.equals(appid)) map.put("", element);
                }
            });
            return map;
        }

        @Override
        public boolean initElement(Logger logger, File home) {
            try {
                InputStream in;
                if (this.certbase64 != null && !this.certbase64.isEmpty()) {
                    in = new ByteArrayInputStream(Base64.getDecoder().decode(this.certbase64));
                } else {
                    File file = (certpath.indexOf('/') == 0 || certpath.indexOf(':') > 0) ? new File(this.certpath) : new File(home, "conf/" + this.certpath);
                    in = file.isFile() ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.certpath);
                }
                if (in == null) return false;
                //需要更新%JDK_HOME%\jre\lib\security下的policy
                //http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html 
                setPaySSLContext(in);
                return true;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init weixinpay sslcontext error", e);
                return false;
            }
        }

        public void setPaySSLContext(InputStream in) throws Exception {
            final KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(in, this.certpwd.toCharArray());
            in.close();
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, certpwd.toCharArray());
            SSLContext ctx = SSLContext.getInstance("TLSv1");
            ctx.init(keyManagerFactory.getKeyManagers(), null, null);
            this.paySSLContext = ctx;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

}
