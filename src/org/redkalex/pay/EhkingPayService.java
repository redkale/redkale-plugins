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
import javax.annotation.Resource;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.*;
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
public class EhkingPayService extends AbstractPayService {

    private static final Map<String, String> header = new HashMap<>();

    static {
        header.put("Content-Type", "application/vnd.ehking-v1.0+json");
    }

    protected static final String format = "%1$tY%1$tm%1$td%1$tH%1$tM%1$tS"; //yyyyMMddHHmmss

    //配置集合
    protected Map<String, EhkingPayElement> elements = new HashMap<>();

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource(name = "property.pay.ehking.conf") //支付配置文件路径
    protected String conf = "config.properties";

    @Resource
    protected JsonConvert convert;

    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = JsonConvert.root();
        if (this.conf != null && !this.conf.isEmpty()) { //存在易宝支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in == null) return;
                Properties properties = new Properties();
                properties.load(in);
                in.close();
                this.elements = EhkingPayElement.create(logger, properties, home);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init ehkingpay conf error", e);
            }
        }
    }

    public void setPayElements(Map<String, EhkingPayElement> elements) {
        this.elements = elements;
    }

    public void putPayElements(Map<String, EhkingPayElement> elements) {
        this.elements.putAll(elements);
    }

    public EhkingPayElement getPayElement(String appid) {
        return this.elements.get(appid);
    }

    public void setPayElement(String appid, EhkingPayElement element) {
        this.elements.put(appid, element);
    }

    public boolean existsPayElement(String appid) {
        return this.elements != null && this.elements.containsKey(appid);
    }

    @Override
    public PayPreResponse prepay(PayPreRequest request) {
        request.checkVaild();
        final PayPreResponse result = new PayPreResponse();
        try {
            final EhkingPayElement element = elements.get(request.getAppid());
            if (element == null) return result.retcode(RETPAY_CONF_ERROR);
            result.setAppid(element.appid);
            LinkedHashMap<String, Object> map = new LinkedHashMap<>();

            final Map<String, String> pd = new LinkedHashMap<>();
            pd.put("name", request.getPaytitle());
            pd.put("quantity", "1");
            pd.put("amount", "" + request.getPaymoney());
            pd.put("receiver", request.getAttach("receiver", ""));

            map.put("merchantId", element.merchno);
            map.put("orderAmount", "" + request.getPaymoney());
            map.put("orderCurrency", "CNY");
            map.put("requestId", request.getPayno());
            String notifyurl = (request.notifyurl != null && !request.notifyurl.isEmpty()) ? request.notifyurl : element.notifyurl;
            map.put("notifyUrl", notifyurl);
            map.put("callbackUrl", request.getAttach("gotourl", notifyurl));
            map.put("remark", request.getPayno());
            map.put("paymentModeCode", request.getAttach("bankcode", ""));
            map.put("forUse", request.getAttach("foruse", ""));

            map.put("productDetails", new Object[]{pd});
            map.put("payer", new LinkedHashMap<>());
            map.put("bankCard", new LinkedHashMap<>());

            map.put("hmac", createSign(element, map));

            result.responsetext = Utility.postHttpContent(element.createurl, header, convert.convertTo(map));
            Map<String, String> resultmap = convert.convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, result.responsetext.replace("\"violations\":{", "\"violations\":'{").replace("}}", "}}'"));
            result.setResult(resultmap);
            final Map<String, String> rsmap = new LinkedHashMap<>();
            rsmap.put("merchantId", resultmap.getOrDefault("merchantId", ""));
            rsmap.put("requestId", resultmap.getOrDefault("requestId", ""));
            rsmap.put("status", resultmap.getOrDefault("status", ""));
            rsmap.put("redirectUrl", resultmap.getOrDefault("redirectUrl", ""));
            rsmap.put("hmac", resultmap.getOrDefault("hmac", ""));
            if (!checkSign(element, rsmap)) return result.retcode(RETPAY_FALSIFY_ERROR);

            if (!"SUCCESS".equalsIgnoreCase(resultmap.get("status")) && !"REDIRECT".equalsIgnoreCase(resultmap.get("status"))) {
                return result.retcode(RETPAY_PAY_ERROR);
            }

            final Map<String, String> rmap = new TreeMap<>();
            rmap.put("redirect", resultmap.getOrDefault("redirectUrl", ""));
            rmap.put("requestid", resultmap.getOrDefault("requestId", ""));
            result.setThirdpayno(rmap.get("requestid"));
            result.setResult(rmap);
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "prepay_pay_error req=" + request + ", resp=" + result.responsetext, e);
        }
        return result;
    }

    @Override
    public PayNotifyResponse notify(PayNotifyRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PayResponse close(PayCloseRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public PayRefundResponse queryRefund(PayRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    protected String createSign(PayElement element, String aValue) {
        byte value[] = aValue.getBytes(UTF8);
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
        md.update(((EhkingPayElement) element).inpad);
        md.update(value);
        byte dg[] = md.digest();
        md.reset();
        md.update(((EhkingPayElement) element).outpad);
        md.update(dg, 0, 16);
        dg = md.digest();
        return Utility.binToHexString(dg);
    }

    @Override
    protected String createSign(PayElement element, Map<String, ?> map0) {
        Map<String, Object> map = (Map<String, Object>) map0;
        final String hmac = (String) map.remove("hmac");
        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Object> en : map.entrySet()) {
            if ("productDetails".equals(en.getKey())) {
                final Map<String, String> pd = (Map<String, String>) ((Object[]) en.getValue())[0];
                pd.forEach((x, y) -> sb.append(y));
            } else if (en.getValue() instanceof String) {
                sb.append(en.getValue());
            }
        }
        String rs = createSign(element, sb.toString());
        if (hmac != null) map.put("hmac", hmac);
        return rs;
    }

    @Override
    protected boolean checkSign(PayElement element, Map<String, ?> map0) {
        Map<String, Object> map = (Map<String, Object>) map0;
        return ((String) map.getOrDefault("hmac", "")).equalsIgnoreCase(createSign(element, map));
    }

    public static class EhkingPayElement extends PayElement {

        //"pay.ehking.merchno" //商户ID 
        public String merchno = ""; //

        //"pay.ehking.appid"
        public String appid = "";  //虚拟APPID, 为空则取merchno

        //"pay.ehking.merchkey" //商户Key
        public String merchkey = ""; //

        //"pay.ehking.notifyurl" //回调url
        public String notifyurl = "";

        //"pay.ehking.createurl" //请求付款url
        public String createurl = "https://api.ehking.com/onlinePay/order";

        //"pay.ehking.queryurl" //请求查询url
        public String queryurl = "https://api.ehking.com/onlinePay/query";

        //"pay.ehking.refundurl" //退款url
        public String refundurl = "https://xxxx";

        //"pay.ehking.closeurl" //请求关闭url
        public String closeurl = "https://xxxx";

        protected byte[] inpad;

        protected byte[] outpad;

        @Override
        public boolean initElement(Logger logger, File home) {
            byte k_ipad[] = new byte[64];

            byte k_opad[] = new byte[64];

            byte keyb[] = this.merchkey.getBytes(UTF8);

            Arrays.fill(k_ipad, keyb.length, 64, (byte) 54);
            Arrays.fill(k_opad, keyb.length, 64, (byte) 92);
            for (int i = 0; i < keyb.length; i++) {
                k_ipad[i] = (byte) (keyb[i] ^ 0x36);
                k_opad[i] = (byte) (keyb[i] ^ 0x5c);
            }
            this.inpad = k_ipad;
            this.outpad = k_opad;
            return true;
        }

        public static Map<String, EhkingPayElement> create(Logger logger, Properties properties, File home) {
            String def_merchno = properties.getProperty("pay.ehking.merchno", "").trim();
            String def_appid = properties.getProperty("pay.ehking.appid", def_merchno).trim();
            String def_merchkey = properties.getProperty("pay.ehking.merchkey", "").trim();
            String def_notifyurl = properties.getProperty("pay.ehking.notifyurl", "").trim();

            String def_createurl = properties.getProperty("pay.ehking.createurl", "https://api.ehking.com/onlinePay/order").trim();
            String def_queryurl = properties.getProperty("pay.ehking.queryurl", "https://api.ehking.com/onlinePay/query").trim();
            String def_refundurl = properties.getProperty("pay.ehking.refundurl", "https://xxxx").trim();
            String def_closeurl = properties.getProperty("pay.ehking.closeurl", "https://xxxx").trim();

            final Map<String, EhkingPayElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("pay.ehking.") && x.toString().endsWith(".merchno")).forEach(merchno_key -> {
                final String prefix = merchno_key.toString().substring(0, merchno_key.toString().length() - ".merchno".length());

                String merchno = properties.getProperty(prefix + ".merchno", def_merchno).trim();
                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String merchkey = properties.getProperty(prefix + ".merchkey", def_merchkey).trim();
                String notifyurl = properties.getProperty(prefix + ".notifyurl", def_notifyurl).trim();
                String createurl = properties.getProperty(prefix + ".createurl", def_createurl).trim();
                String queryurl = properties.getProperty(prefix + ".queryurl", def_queryurl).trim();
                String refundurl = properties.getProperty(prefix + ".refundurl", def_refundurl).trim();
                String closeurl = properties.getProperty(prefix + ".closeurl", def_closeurl).trim();

                if (merchno.isEmpty() || notifyurl.isEmpty()) {
                    logger.log(Level.WARNING, properties + "; has illegal ehkingpay conf by prefix" + prefix);
                    return;
                }
                EhkingPayElement element = new EhkingPayElement();
                element.merchno = merchno;
                element.appid = appid;
                element.merchkey = merchkey;
                element.notifyurl = notifyurl;
                element.createurl = createurl;
                element.queryurl = queryurl;
                element.refundurl = refundurl;
                element.closeurl = closeurl;
                if (element.initElement(logger, null)) {
                    map.put(appid, element);
                    if (def_appid.equals(appid)) map.put("", element);
                }
            });
            //if (logger.isLoggable(Level.FINEST)) logger.finest("" + map);
            return map;
        }
    }
}
