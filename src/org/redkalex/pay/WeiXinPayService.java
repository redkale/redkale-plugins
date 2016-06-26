/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.security.*;
import java.util.*;
import java.util.logging.*;
import java.util.regex.*;
import javax.annotation.Resource;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.*;
import org.redkale.util.*;
import static org.redkalex.pay.Pays.*;

/**
 *
 * @see http://redkale.org
 * @author zhangjx
 */
@AutoLoad(false)
@LocalService
public class WeiXinPayService extends AbstractPayService {

    private static final String format = "%1$tY%1$tm%1$td%1$tH%1$tM%1$tS"; //yyyyMMddHHmmss

    private static final Pattern PAYXML = Pattern.compile("<([^/>]+)>(.+)</.+>"); // "<([^/>]+)><!\\[CDATA\\[(.+)\\]\\]></.+>"

    public static final int PAY_WX_ERROR = 4012101;//微信支付失败

    public static final int PAY_FALSIFY_ORDER = 4012017;//交易签名被篡改

    public static final int PAY_STATUS_ERROR = 4012018;//订单或者支付状态不正确

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected final boolean fine = logger.isLoggable(Level.FINE);

    protected final boolean finer = logger.isLoggable(Level.FINER);

    protected final boolean finest = logger.isLoggable(Level.FINEST);

    @Resource(name = "property.pay.weixin.merchno") //商户ID
    protected String wxpaymchid = "xxxxxxxxxxx";

    @Resource(name = "property.pay.weixin.submerchno") //子商户ID，受理模式必填
    protected String wxpaysdbmchid = "";

    @Resource(name = "property.pay.weixin.appid") //公众账号ID
    protected String wxpayappid = "wxYYYYYYYYYYYY";

    @Resource(name = "property.pay.weixin.notifyurl") //回调url
    protected String wxpaynotifyurl = "http: //xxxxxx";

    @Resource(name = "property.pay.weixin.signkey") //签名算法需要用到的秘钥
    protected String wxpaykey = "##########################";

    @Resource(name = "property.pay.weixin.certpwd")
    protected String wxpaycertpwd = "xxxxxxxxxx"; //HTTP证书的密码，默认等于MCHID

    @Resource(name = "property.pay.weixin.certpath") //HTTP证书在服务器中的路径，用来加载证书用
    protected String wxpaycertpath = "apiclient_cert.p12";

    @Resource
    protected JsonConvert convert;

    /**
     * <xml>
     * <return_code><![CDATA[SUCCESS]]></return_code>
     * <return_msg><![CDATA[OK]]></return_msg>
     * <appid><![CDATA[wx4ad12c89818dd981]]></appid>
     * <mch_id><![CDATA[1241384602]]></mch_id>
     * <nonce_str><![CDATA[RpGucJ6wKtPgpTJy]]></nonce_str>
     * <sign><![CDATA[DFD99D5DA7DCA4FB5FB79ECAD49B9369]]></sign>
     * <result_code><![CDATA[SUCCESS]]></result_code>
     * <prepay_id><![CDATA[wx2015051518135700aaea6bc30284682518]]></prepay_id>
     * <trade_type><![CDATA[JSAPI]]></trade_type>
     * </xml>
     *
     * @param request
     *
     * @return
     */
    @Override
    public PayResponse create(final PayCreatRequest request) {
        request.checkVaild();
        final PayResponse result = new PayResponse();
        try {
            final TreeMap<String, String> map = new TreeMap<>();
            if (request.getMap() != null) map.putAll(request.getMap());
            map.put("appid", this.wxpayappid);
            map.put("mch_id", this.wxpaymchid);
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.putIfAbsent("body", request.getTradebody());
            //map.put("attach", "" + payid);
            map.put("out_trade_no", "" + request.getTradeno());
            map.put("total_fee", "" + request.getTrademoney());
            map.put("spbill_create_ip", request.getClientAddr());
            map.put("time_expire", String.format(format, System.currentTimeMillis() + request.getPaytimeout() * 1000));
            map.put("notify_url", this.wxpaynotifyurl);
            map.put("sign", createSign(map));

            final String responseText = Utility.postHttpContent("https://api.mch.weixin.qq.com/pay/unifiedorder", formatMapToXML(map));
            result.setResponseText(responseText);

            Map<String, String> wxresult = formatXMLToMap(responseText);
            if (!"SUCCESS".equals(wxresult.get("return_code"))) return result.retcode(PAY_WX_ERROR);
            if (!checkSign(wxresult)) return result.retcode(PAY_FALSIFY_ORDER);
            /**
             * "appId" : "wx2421b1c4370ec43b", //公众号名称，由商户传入 "timeStamp":" 1395712654", //时间戳，自1970年以来的秒数 "nonceStr" : "e61463f8efa94090b1f366cccfbbb444", //随机串 "package" :
             * "prepay_id=u802345jgfjsdfgsdg888", "signType" : "MD5", //微信签名方式: "paySign" : "70EA570631E4BB79628FBCA90534C63FF7FADD89" //微信签名
             */
            final Map<String, String> rmap = new TreeMap<>();
            result.setResult(rmap);
            rmap.put("appId", this.wxpayappid);
            rmap.put("timeStamp", Long.toString(System.currentTimeMillis() / 1000));
            rmap.put("nonceStr", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            rmap.put("package", "prepay_id=" + wxresult.get("prepay_id"));
            rmap.put("signType", "MD5");
            rmap.put("paySign", createSign(rmap));
        } catch (Exception e) {
            result.setRetcode(PAY_WX_ERROR);
            logger.log(Level.WARNING, "paying error.", e);
        }
        return result;
    }

    /**
     * 回调支付接口返回的结果
     * <p>
     * <xml>
     * <appid><![CDATA[wx4ad12c89818dd981]]></appid>
     * <attach><![CDATA[10000070334]]></attach>
     * <bank_type><![CDATA[ICBC_DEBIT]]></bank_type>
     * <cash_fee><![CDATA[10]]></cash_fee>
     * <fee_type><![CDATA[CNY]]></fee_type>
     * <is_subscribe><![CDATA[Y]]></is_subscribe>
     * <mch_id><![CDATA[1241384602]]></mch_id>
     * <nonce_str><![CDATA[14d69ac6d6525f27dc9bcbebc]]></nonce_str>
     * <openid><![CDATA[ojEVbsyDUzGqlgX3eDgmAMaUDucA]]></openid>
     * <out_trade_no><![CDATA[1000072334]]></out_trade_no>
     * <result_code><![CDATA[SUCCESS]]></result_code>
     * <return_code><![CDATA[SUCCESS]]></return_code>
     * <sign><![CDATA[60D95E25EA9C4F54BD1020952303C4E2]]></sign>
     * <time_end><![CDATA[20150519085546]]></time_end>
     * <total_fee>10</total_fee>
     * <trade_type><![CDATA[JSAPI]]></trade_type>
     * <transaction_id><![CDATA[1009630061201505190139511926]]></transaction_id>
     * </xml>
     *
     * @param request
     *
     * @return
     */
    @Override
    public PayQueryResponse query(final PayRequest request) {
        request.checkVaild();
        final PayQueryResponse result = new PayQueryResponse();
        try {
            final Map<String, String> map = new TreeMap<>();
            map.put("appid", this.wxpayappid);
            map.put("mch_id", this.wxpaymchid);
            map.put("out_trade_no", "" + request.getTradeno());
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.put("sign", createSign(map));

            final String responseText = Utility.postHttpContent("https://api.mch.weixin.qq.com/pay/orderquery", formatMapToXML(map));
            result.setResponseText(responseText);

            final Map<String, String> wxresult = formatXMLToMap(responseText);
            result.setResult(wxresult);

            String state = map.get("trade_state");
            if (state == null && "SUCCESS".equals(map.get("result_code")) && Long.parseLong(map.get("total_fee")) > 0) {
                state = "SUCCESS";
            }
            short paystatus = "SUCCESS".equals(state) ? PAYSTATUS_PAYOK : PAYSTATUS_UNPAY;
        } catch (Exception e) {
            result.setRetcode(PAY_WX_ERROR);
            logger.log(Level.WARNING, "querypay error.", e);
        }
        return result;
    }

    @Override
    public PayResponse close(final PayRequest request) {
        request.checkVaild();
        final PayResponse result = new PayResponse();
        try {
            Map<String, String> map = new TreeMap<>();
            map.put("appid", wxpayappid);
            map.put("mch_id", wxpaymchid);
            map.put("nonce_str", Long.toHexString(System.currentTimeMillis()) + Long.toHexString(System.nanoTime()));
            map.put("out_trade_no", "" + request.getTradeno());
            map.put("sign", createSign(map));

            final String responseText = Utility.postHttpContent("https://api.mch.weixin.qq.com/pay/closeorder", formatMapToXML(map));
            result.setResponseText(responseText);

            Map<String, String> wxresult = formatXMLToMap(responseText);
            result.setResult(wxresult);
            if (!"SUCCESS".equals(wxresult.get("return_code"))) return result.retcode(PAY_WX_ERROR);
            if (!checkSign(wxresult)) return result.retcode(PAY_FALSIFY_ORDER);
        } catch (Exception e) {
            result.setRetcode(PAY_WX_ERROR);
            logger.log(Level.WARNING, "closepay error ", e);
        }
        return result;
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PayRefundQueryResponse queryRefund(PayRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    protected String createSign(Map<String, String> map) throws NoSuchAlgorithmException {
        final StringBuilder sb = new StringBuilder();
        map.forEach((x, y) -> sb.append(x).append('=').append(y).append('&'));
        sb.append("key=").append(this.wxpaykey);
        return Utility.binToHexString(MessageDigest.getInstance("MD5").digest(sb.toString().getBytes())).toUpperCase();
    }

    protected boolean checkSign(Map<String, String> map) {
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        String sign = map.remove("sign");
        final StringBuilder sb = new StringBuilder();
        map.forEach((x, y) -> sb.append(x).append('=').append(y).append('&'));
        sb.append("key=").append(wxpaykey);
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

    protected static Map<String, String> formatXMLToMap(final String xml) {
        Map<String, String> map = new TreeMap<>();
        Matcher m = PAYXML.matcher(xml.substring(xml.indexOf('>') + 1));
        while (m.find()) {
            String val = m.group(2);
            if (val.startsWith("<![CDATA[")) val = val.substring("<![CDATA[".length(), val.length() - 3);
            map.put(m.group(1), val);
        }
        return map;
    }
}
