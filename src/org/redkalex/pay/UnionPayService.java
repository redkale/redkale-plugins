/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.net.URLEncoder;
import java.security.*;
import java.security.cert.*;
import java.util.*;
import java.util.logging.*;
import javax.annotation.Resource;
import javax.net.ssl.SSLContext;
import org.redkale.util.*;
import org.redkale.service.*;
import org.redkale.convert.json.*;
import static org.redkalex.pay.Pays.*;
import static org.redkalex.pay.PayRetCodes.*;

/**
 * 银联支付官网文档： https://open.unionpay.com/ajweb/help/file/techFile?productId=1
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
@AutoLoad(false)
@LocalService
public class UnionPayService extends AbstractPayService {

    private static final String format = "%1$tY%1$tm%1$td%1$tH%1$tM%1$tS"; //yyyyMMddHHmmss

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected final boolean fine = logger.isLoggable(Level.FINE);

    protected final boolean finer = logger.isLoggable(Level.FINER);

    protected final boolean finest = logger.isLoggable(Level.FINEST);

    @Resource(name = "property.pay.union.merchno") //商户ID 
    protected String merchno = "777290058110097"; //777290058110097 为测试账号

    @Resource(name = "property.pay.union.version") //银联协议版本
    protected String version = "5.0.0";

    @Resource(name = "property.pay.union.notifyurl") //回调url
    protected String notifyurl = "";

    @Resource(name = "property.pay.union.createurl") //请求付款url
    protected String createurl = "https://gateway.95516.com/gateway/api/appTransReq.do";

    @Resource(name = "property.pay.union.queryurl") //请求查询url
    protected String queryurl = "https://gateway.95516.com/gateway/api/queryTrans.do";

    @Resource(name = "property.pay.union.refundurl") //退款url
    protected String refundurl = "https://gateway.95516.com/gateway/api/backTransReq.do";

    @Resource(name = "property.pay.union.closeurl") //请求关闭url
    protected String closeurl = "https://gateway.95516.com/gateway/api/backTransReq.do";

    @Resource(name = "property.pay.union.signcertpwd")
    protected String signcertpwd = ""; //HTTP证书的密码，默认等于000000

    @Resource(name = "property.pay.union.signcertpath") //HTTP证书在服务器中的路径，用来加载证书用, 不是/开头且没有:字符，视为{APP_HOME}/conf相对下的路径
    protected String signcertpath = "";

    @Resource(name = "property.pay.union.verifycertpath") //检测证书路径，视为{APP_HOME}/conf相对下的路径
    protected String verifycertpath = "";

    @Resource(name = "APP_HOME")
    protected File home;

    protected SSLContext paySSLContext;

    @Resource
    protected JsonConvert convert;

    protected String signcertid = ""; //签名证书中的证书序列号（单证书） 证书的物理编号

    protected String verifycertid = "";

    protected PrivateKey priKey; //私钥

    protected PublicKey pubKey; //公钥

    @Override
    public void init(AnyValue conf) {
        if (this.merchno == null || this.merchno.isEmpty()) return;//没有启用银联支付
        if (this.signcertpwd == null || this.signcertpwd.isEmpty()) return;//没有启用银联支付
        if (this.signcertpath == null || this.signcertpath.isEmpty()) return;//没有启用银联支付

        if (this.convert == null) this.convert = JsonConvert.root();
        try {
            //读取签名证书私钥
            File signfile = (signcertpath.indexOf('/') == 0 || signcertpath.indexOf(':') > 0) ? new File(this.signcertpath) : new File(home, "conf/" + this.signcertpath);
            InputStream signin = signfile.isFile() ? new FileInputStream(signfile) : getClass().getResourceAsStream("/META-INF/" + this.signcertpath);

            final KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(signin, this.signcertpwd.toCharArray());
            signin.close();
            Enumeration<String> aliasenum = keyStore.aliases();
            final String keyAlias = aliasenum.hasMoreElements() ? aliasenum.nextElement() : null;
            this.priKey = (PrivateKey) keyStore.getKey(keyAlias, this.signcertpwd.toCharArray());
            X509Certificate cert = (X509Certificate) keyStore.getCertificate(keyAlias);
            this.signcertid = cert.getSerialNumber().toString();

            //读取验证证书公钥
            File verifyfile = (verifycertpath.indexOf('/') == 0 || verifycertpath.indexOf(':') > 0) ? new File(this.verifycertpath) : new File(home, "conf/" + this.verifycertpath);
            InputStream verifyin = verifyfile.isFile() ? new FileInputStream(verifyfile) : getClass().getResourceAsStream("/META-INF/" + this.verifycertpath);

            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate verifycert = (X509Certificate) cf.generateCertificate(verifyin);
            verifyin.close();
            this.verifycertid = verifycert.getSerialNumber().toString();
            this.pubKey = verifycert.getPublicKey();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Throwable {
        UnionPayService service = new UnionPayService();
        service.createurl = "https://101.231.204.80:5000/gateway/api/appTransReq.do"; //请求支付url
        service.queryurl = "https://101.231.204.80:5000/gateway/api/queryTrans.do"; //请求查询url
        service.refundurl = "https://101.231.204.80:5000/gateway/api/backTransReq.do"; //请求退款url
        service.closeurl = "https://101.231.204.80:5000/gateway/api/backTransReq.do";  //请求关闭url

        service.home = new File("D:/Java-Project/RedkalePluginsProject");
        service.signcertpath = "acp_test_sign.pfx"; //放在 {APP_HOME}/conf 目录下
        service.verifycertpath = "acp_test_verify_sign.cer";//放在 {APP_HOME}/conf 目录下
        service.signcertpwd = "000000";
        service.init(null);

        //支付
        final PayCreatRequest creatRequest = new PayCreatRequest();
        creatRequest.setPaytype(Pays.PAYTYPE_UNION);
        creatRequest.setPayno("Redkale100000001");
        creatRequest.setPaymoney(10); //1毛钱
        creatRequest.setPaytitle("一斤红菜苔");
        creatRequest.setPaybody("一斤红菜苔");
        creatRequest.setClientAddr(Utility.localInetAddress().getHostAddress());
        final PayCreatResponse creatResponse = service.create(creatRequest);
        System.out.println(creatResponse);

        //查询
        //请求不能太频繁，否则 You have been added to the blacklist. Please don't do stress testing. TPS could not be greater than 0.5 . BlackList Will be clear at time 00:00
        PayRequest queryRequest = new PayRequest();
        queryRequest.setPaytype(Pays.PAYTYPE_UNION);
        queryRequest.setPayno(creatRequest.getPayno());
        //PayQueryResponse queryResponse = service.query(queryRequest);
        //System.out.println(queryResponse);
    }

    @Override
    public PayPreResponse prepay(final PayPreRequest request) {
        request.checkVaild();
        //参数说明： https://open.unionpay.com/ajweb/help/file/techFile?productId=3
        final PayPreResponse result = new PayPreResponse();
        try {
            TreeMap<String, String> map = new TreeMap<>();
            if (request.getMap() != null) map.putAll(request.getMap());

            /** *银联全渠道系统，产品参数，除了encoding自行选择外其他不需修改** */
            map.put("version", version);            //版本号 全渠道默认值
            map.put("encoding", "UTF-8");     //字符集编码 可以使用UTF-8,GBK两种方式
            map.put("signMethod", "01");           		 	//签名方法 目前只支持01：RSA方式证书加密
            map.put("txnType", "01");              		 	//交易类型 01：消费
            map.put("txnSubType", "01");           		 	//交易子类 01：消费
            map.put("bizType", "000201");          		 	//填写000201
            map.putIfAbsent("channelType", "08");          		 	//渠道类型，07：PC，08：手机
            /** *商户接入参数** */
            map.put("merId", merchno);   					//商户号码，请改成自己申请的商户号或者open上注册得来的777商户号测试
            map.put("certId", signcertid);                  //设置签名证书中的证书序列号（单证书） 证书的物理编号
            map.put("accessType", "0");            		 	//接入类型，商户接入填0 ，不需修改（0：直连商户， 1： 收单机构 2：平台商户）
            map.put("orderId", request.getPayno());       //商户订单号，8-40位数字字母，不能含“-”或“_”，可以自行定制规则	
            map.put("txnTime", String.format(format, System.currentTimeMillis())); //订单发送时间，取系统时间，格式为YYYYMMDDhhmmss，必须取当前时间，否则会报txnTime无效
            map.put("accType", "01");					 	//账号类型 01：银行卡; 02：存折; 03：IC卡帐号类型(卡介质)
            map.put("txnAmt", "" + request.getPaymoney());//交易金额 单位为分，不能带小数点
            map.put("currencyCode", "156");                 //境内商户CNY固定 156 人民币

            //后台通知地址（需设置为外网能访问 http https均可），支付成功后银联会自动将异步通知报文post到商户上送的该地址
            map.put("backUrl", notifyurl);
            map.put("signature", createSign(map));

            result.responsetext = Utility.postHttpContent(this.createurl, joinMap(map));
            Map<String, String> resultmap = formatTextToMap(result.responsetext);
            result.setResult(resultmap);
            if (!checkSign(resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            if (!"00".equalsIgnoreCase(resultmap.get("respCode"))) {
                return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("respMsg"));
            }

            final Map<String, String> rmap = new TreeMap<>();
            rmap.put("text", resultmap.getOrDefault("tn", ""));
            result.setResult(rmap);
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "prepay_pay_error", e);
        }
        return result;
    }

    @Override
    public PayNotifyResponse notify(PayNotifyRequest request) {
        request.checkVaild();
        final PayNotifyResponse result = new PayNotifyResponse();
        result.setPaytype(request.getPaytype());
        final String rstext = "success";
        Map<String, String> map = request.getMap();
        result.setPayno(map.getOrDefault("orderId", ""));
        if (!checkSign(map)) return result.retcode(RETPAY_FALSIFY_ERROR);
        if (!"00".equalsIgnoreCase(map.get("respCode")) || Long.parseLong(map.getOrDefault("txnAmt", "0")) < 1) {
            return result.retcode(RETPAY_PAY_ERROR).retinfo(map.getOrDefault("respMsg", null));
        }
        return result.result(rstext);
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        request.checkVaild();
        final PayCreatResponse result = new PayCreatResponse();
        try {
            TreeMap<String, String> map = new TreeMap<>();
            if (request.getMap() != null) map.putAll(request.getMap());

            /** *银联全渠道系统，产品参数，除了encoding自行选择外其他不需修改** */
            map.put("version", version);            //版本号 全渠道默认值
            map.put("encoding", "UTF-8");     //字符集编码 可以使用UTF-8,GBK两种方式
            map.put("signMethod", "01");           		 	//签名方法 目前只支持01：RSA方式证书加密
            map.put("txnType", "01");              		 	//交易类型 01：消费
            map.put("txnSubType", "01");           		 	//交易子类 01：消费
            map.put("bizType", "000201");          		 	//填写000201
            map.putIfAbsent("channelType", "08");          		 	//渠道类型，07：PC，08：手机

            /** *商户接入参数** */
            map.put("merId", merchno);   					//商户号码，请改成自己申请的商户号或者open上注册得来的777商户号测试
            map.put("certId", signcertid);                  //设置签名证书中的证书序列号（单证书） 证书的物理编号
            map.put("accessType", "0");            		 	//接入类型，商户接入填0 ，不需修改（0：直连商户， 1： 收单机构 2：平台商户）
            map.put("orderId", request.getPayno());       //商户订单号，8-40位数字字母，不能含“-”或“_”，可以自行定制规则	
            map.put("txnTime", String.format(format, System.currentTimeMillis())); //订单发送时间，取系统时间，格式为YYYYMMDDhhmmss，必须取当前时间，否则会报txnTime无效
            map.put("accType", "01");					 	//账号类型 01：银行卡; 02：存折; 03：IC卡帐号类型(卡介质)
            map.put("txnAmt", "" + request.getPaymoney());//交易金额 单位为分，不能带小数点
            map.put("currencyCode", "156");                 //境内商户CNY固定 156 人民币
            //contentData.put("reqReserved", "透传字段");           //商户自定义保留域，交易应答时会原样返回

            //后台通知地址（需设置为外网能访问 http https均可），支付成功后银联会自动将异步通知报文post到商户上送的该地址，【支付失败的交易银联不会发送后台通知】
            //后台通知参数详见open.unionpay.com帮助中心 下载  产品接口规范  网关支付产品接口规范 消费交易 商户通知
            //注意:1.需设置为外网能访问，否则收不到通知    2.http https均可  3.收单后台通知后需要10秒内返回http200或302状态码 
            //    4.如果银联通知服务器发送通知后10秒内未收到返回状态码或者应答码非http200或302，那么银联会间隔一段时间再次发送。总共发送5次，银联后续间隔1、2、4、5 分钟后会再次通知。
            //    5.后台通知地址如果上送了带有？的参数，例如：http://abc/web?a=b&c=d 在后台通知处理程序验证签名之前需要编写逻辑将这些字段去掉再验签，否则将会验签失败
            if (!notifyurl.isEmpty()) map.put("backUrl", notifyurl);
            map.put("signature", createSign(map));

            result.responsetext = Utility.postHttpContent(this.createurl, joinMap(map));
            Map<String, String> resultmap = formatTextToMap(result.responsetext);
            result.setResult(resultmap);
            if (!checkSign(resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            if (!"00".equalsIgnoreCase(resultmap.get("respCode"))) {
                return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("respMsg"));
            }
            result.setThirdpayno(resultmap.getOrDefault("queryId", ""));
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "create_pay_error", e);
        }
        return result;
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        request.checkVaild();
        final PayQueryResponse result = new PayQueryResponse();
        try {
            TreeMap<String, String> map = new TreeMap<>();

            /** *银联全渠道系统，产品参数，除了encoding自行选择外其他不需修改** */
            map.put("version", version);            //版本号 全渠道默认值
            map.put("encoding", "UTF-8");     //字符集编码 可以使用UTF-8,GBK两种方式
            map.put("signMethod", "01");           		 	//签名方法 目前只支持01：RSA方式证书加密
            map.put("txnType", "00");              		 	//交易类型 00：无
            map.put("txnSubType", "00");           		 	//交易子类 00：无
            map.put("bizType", "000201");          		 	//填写000201
            //map.putIfAbsent("channelType", "08");   查询不需要	//渠道类型，07：PC，08：手机

            /** *商户接入参数** */
            map.put("merId", merchno);   					//商户号码，请改成自己申请的商户号或者open上注册得来的777商户号测试
            map.put("certId", signcertid);                  //设置签名证书中的证书序列号（单证书） 证书的物理编号
            map.put("accessType", "0");            		 	//接入类型，商户接入填0 ，不需修改（0：直连商户， 1： 收单机构 2：平台商户）

            map.put("orderId", request.getPayno());       //商户订单号，8-40位数字字母，不能含“-”或“_”，可以自行定制规则	
            map.put("txnTime", String.format(format, System.currentTimeMillis())); //订单发送时间，取系统时间，格式为YYYYMMDDhhmmss，必须取当前时间，否则会报txnTime无效

            map.put("signature", createSign(map));

            result.responsetext = Utility.postHttpContent(this.queryurl, joinMap(map));
            Map<String, String> resultmap = formatTextToMap(result.responsetext);
            result.setResult(resultmap);
            if (!checkSign(resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            if (!"00".equalsIgnoreCase(resultmap.get("respCode"))) {
                return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("respMsg"));
            }
            //trade_status 交易状态：WAIT_BUYER_PAY（交易创建，等待买家付款）、TRADE_CLOSED（未付款交易超时关闭，或支付完成后全额退款）、TRADE_SUCCESS（交易支付成功）、TRADE_FINISHED（交易结束，不可退款）
            short paystatus = PAYSTATUS_PAYNO;
            switch (resultmap.get("origRespCode")) {
                case "00": paystatus = PAYSTATUS_PAYOK;
                    break;
                case "WAIT_BUYER_PAY": paystatus = PAYSTATUS_UNPAY;
                    break;
                case "TRADE_CLOSED": paystatus = PAYSTATUS_CLOSED;
                    break;
                case "TRADE_FINISHED": paystatus = PAYSTATUS_PAYOK;
                    break;
            }
            result.setPaystatus(paystatus);
            result.setThirdpayno(resultmap.getOrDefault("queryId", ""));
            result.setPayedmoney((long) (Double.parseDouble(resultmap.get("receipt_amount")) * 100));
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "query_pay_error", e);
        }
        return result;
    }

    @Override
    public PayResponse close(PayCloseRequest request) {
        request.checkVaild();
        final PayResponse result = new PayResponse();
        try {
            TreeMap<String, String> map = new TreeMap<>();

            /** *银联全渠道系统，产品参数，除了encoding自行选择外其他不需修改** */
            map.put("version", version);            //版本号 全渠道默认值
            map.put("encoding", "UTF-8");     //字符集编码 可以使用UTF-8,GBK两种方式
            map.put("signMethod", "01");           		 	//签名方法 目前只支持01：RSA方式证书加密
            map.put("txnType", "31");              		 	//交易类型 31：消费撤销
            map.put("txnSubType", "00");           		 	//交易子类 00：无
            map.put("bizType", "000201");          		 	//填写000201
            map.putIfAbsent("channelType", "08");          		 	//渠道类型，07：PC，08：手机

            /** *商户接入参数** */
            map.put("merId", merchno);   					//商户号码，请改成自己申请的商户号或者open上注册得来的777商户号测试
            map.put("certId", signcertid);                  //设置签名证书中的证书序列号（单证书） 证书的物理编号
            map.put("accessType", "0");            		 	//接入类型，商户接入填0 ，不需修改（0：直连商户， 1： 收单机构 2：平台商户）
            map.put("orderId", request.getPayno());       //商户订单号，8-40位数字字母，不能含“-”或“_”，可以自行定制规则	
            map.put("txnTime", String.format(format, System.currentTimeMillis())); //订单发送时间，取系统时间，格式为YYYYMMDDhhmmss，必须取当前时间，否则会报txnTime无效
            map.put("accType", "01");					 	//账号类型 01：银行卡; 02：存折; 03：IC卡帐号类型(卡介质)
            map.put("txnAmt", "" + request.getPaymoney());//交易金额 单位为分，不能带小数点
            map.put("currencyCode", "156");                 //境内商户CNY固定 156 人民币

            map.put("origQryId", request.getThirdpayno());  //【原始交易流水号】，原消费交易返回的的queryId，可以从消费交易后台通知接口中或者交易状态查询接口中获取
            //后台通知地址（需设置为外网能访问 http https均可），支付成功后银联会自动将异步通知报文post到商户上送的该地址，【支付失败的交易银联不会发送后台通知】
            if (!notifyurl.isEmpty()) map.put("backUrl", notifyurl);

            map.put("signature", createSign(map));

            result.responsetext = Utility.postHttpContent(this.closeurl, joinMap(map));
            Map<String, String> resultmap = formatTextToMap(result.responsetext);
            result.setResult(resultmap);
            if (!checkSign(resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            if (!"00".equalsIgnoreCase(resultmap.get("respCode"))) {
                return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("respMsg"));
            }
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "close_pay_error", e);
        }
        return result;
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        request.checkVaild();
        final PayRefundResponse result = new PayRefundResponse();
        try {
            TreeMap<String, String> map = new TreeMap<>();

            /** *银联全渠道系统，产品参数，除了encoding自行选择外其他不需修改** */
            map.put("version", version);            //版本号 全渠道默认值
            map.put("encoding", "UTF-8");     //字符集编码 可以使用UTF-8,GBK两种方式
            map.put("signMethod", "01");           		 	//签名方法 目前只支持01：RSA方式证书加密
            map.put("txnType", "04");              		 	//交易类型 04：退货
            map.put("txnSubType", "00");           		 	//交易子类 00：无
            map.put("bizType", "000201");          		 	//填写000201
            map.putIfAbsent("channelType", "08");          		 	//渠道类型，07：PC，08：手机

            /** *商户接入参数** */
            map.put("merId", merchno);   					//商户号码，请改成自己申请的商户号或者open上注册得来的777商户号测试
            map.put("certId", signcertid);                  //设置签名证书中的证书序列号（单证书） 证书的物理编号
            map.put("accessType", "0");            		 	//接入类型，商户接入填0 ，不需修改（0：直连商户， 1： 收单机构 2：平台商户）
            map.put("orderId", request.getPayno());       //商户订单号，8-40位数字字母，不能含“-”或“_”，可以自行定制规则	
            map.put("txnTime", String.format(format, System.currentTimeMillis())); //订单发送时间，取系统时间，格式为YYYYMMDDhhmmss，必须取当前时间，否则会报txnTime无效
            map.put("txnAmt", "" + request.getRefundmoney());//****退货金额，单位分，不要带小数点。退货金额小于等于原消费金额，当小于的时候可以多次退货至退货累计金额等于原消费金额		
            map.put("currencyCode", "156");                 //境内商户CNY固定 156 人民币

            map.put("origQryId", request.getThirdpayno());  //【原始交易流水号】，原消费交易返回的的queryId，可以从消费交易后台通知接口中或者交易状态查询接口中获取
            //后台通知地址（需设置为外网能访问 http https均可），支付成功后银联会自动将异步通知报文post到商户上送的该地址，【支付失败的交易银联不会发送后台通知】
            if (!notifyurl.isEmpty()) map.put("backUrl", notifyurl);

            map.put("signature", createSign(map));

            result.responsetext = Utility.postHttpContent(this.closeurl, joinMap(map));
            Map<String, String> resultmap = formatTextToMap(result.responsetext);
            result.setResult(resultmap);
            if (!checkSign(resultmap)) return result.retcode(RETPAY_FALSIFY_ERROR);
            if (!"00".equalsIgnoreCase(resultmap.get("respCode"))) {
                return result.retcode(RETPAY_PAY_ERROR).retinfo(resultmap.get("respMsg"));
            }
        } catch (Exception e) {
            result.setRetcode(RETPAY_PAY_ERROR);
            logger.log(Level.WARNING, "close_pay_error", e);
        }
        return result;
    }

    @Override
    public PayRefundResponse queryRefund(PayRequest request) {
        PayQueryResponse queryResponse = query(request);
        final PayRefundResponse response = new PayRefundResponse();
        response.setRetcode(queryResponse.getRetcode());
        response.setRetinfo(queryResponse.getRetinfo());
        response.setResponsetext(queryResponse.getResponsetext());
        response.setResult(queryResponse.getResult());
        if (queryResponse.isSuccess()) {
            response.setRefundedmoney(Long.parseLong(response.getResult().get("txnAmt")));
        }
        return response;
    }

    protected Map<String, String> formatTextToMap(String responseText) {
        Map<String, String> map = new TreeMap<>();
        for (String item : responseText.split("&")) {
            int pos = item.indexOf('=');
            if (pos < 0) return map;
            map.put(item.substring(0, pos), item.substring(pos + 1));
        }
        return map;
    }

    @Override
    protected String createSign(Map<String, String> map) throws Exception { //计算签名
        byte[] digest = MessageDigest.getInstance("SHA-1").digest(joinMap(map).getBytes("UTF-8"));

        Signature signature = Signature.getInstance("SHA1WithRSA");
        signature.initSign(priKey);
        signature.update(Utility.binToHexString(digest).getBytes("UTF-8"));
        return URLEncoder.encode(Base64.getEncoder().encodeToString(signature.sign()), "UTF-8");
    }

    @Override
    protected boolean checkSign(Map<String, String> map) {  //验证签名
        if (!this.verifycertid.equals(map.get("certId"))) return false;
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        try {
            final byte[] sign = Base64.getDecoder().decode(map.remove("signature").getBytes("UTF-8"));
            final byte[] sha1 = MessageDigest.getInstance("SHA-1").digest(joinMap(map).getBytes("UTF-8"));
            final byte[] digest = Utility.binToHexString(sha1).getBytes("UTF-8");

            Signature signature = Signature.getInstance("SHA1WithRSA");
            signature.initVerify(this.pubKey);
            signature.update(digest);
            return signature.verify(sign);
        } catch (Exception e) {
            return false;
        }
    }

}
