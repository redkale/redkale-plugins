/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.weixin;

import org.redkale.util.Utility;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.RetResult;
import org.redkale.util.AutoLoad;
import org.redkale.service.Service;
import static org.redkale.convert.json.JsonConvert.TYPE_MAP_STRING_STRING;
import java.io.*;
import java.security.*;
import java.util.*;
import java.util.logging.*;
import javax.annotation.*;
import org.redkale.service.Local;
import org.redkale.util.*;
import static org.redkale.util.Utility.getHttpContent;

/**
 * 微信服务号Service
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public class WeiXinMPService implements Service {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private final boolean finest = logger.isLoggable(Level.FINEST);

    private final boolean finer = logger.isLoggable(Level.FINER);

    //配置集合, key: appid
    protected Map<String, MpElement> appidElements = new HashMap<>();

    //配置集合 key: clientid
    protected Map<String, MpElement> clientidElements = new HashMap<>();

    @Resource
    protected JsonConvert convert;

    @Resource(name = "property.weixin.mp.conf") //公众号配置文件路径
    protected String conf = "config.properties";

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource(name = "property.weixin.mp.clientid") //客户端ID
    protected String clientid = "";

    @Resource(name = "property.weixin.mp.appid") //公众账号ID
    protected String appid = "";

    @Resource(name = "property.weixin.mp.appsecret") // 
    protected String appsecret = "";

    @Resource(name = "property.weixin.mp.token")
    protected String mptoken = "";

    @Override
    public void init(AnyValue conf) {
        if (this.conf != null && !this.conf.isEmpty()) { //存在微信公众号配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in == null) return;
                Properties properties = new Properties();
                properties.load(in);
                in.close();
                this.appidElements = MpElement.create(logger, properties, home);
                MpElement defElement = this.appidElements.get("");
                if (defElement != null && (this.appid == null || this.appid.isEmpty())) {
                    this.clientid = defElement.clientid;
                    this.appid = defElement.appid;
                    this.appsecret = defElement.appsecret;
                    this.mptoken = defElement.mptoken;
                }
                this.appidElements.values().forEach(element -> clientidElements.put(element.clientid, element));
                
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init weixinmp conf error", e);
            }
        }
    }

    //-----------------------------------微信服务号接口----------------------------------------------------------
    //仅用于 https://open.weixin.qq.com/connect/oauth2/authorize  &scope=snsapi_base
    //需要在 “开发 - 接口权限 - 网页服务 - 网页帐号 - 网页授权获取用户基本信息”的配置选项中，修改授权回调域名
    public RetResult<String> getMPOpenidByCode(String code) throws IOException {
        if (code != null) code = code.replace("\"", "").replace("'", "");
        String url = "https://api.weixin.qq.com/sns/oauth2/access_token?appid=" + appid + "&secret=" + appsecret + "&code=" + code + "&grant_type=authorization_code";
        String json = getHttpContent(url);
        if (finest) logger.finest(url + "--->" + json);
        Map<String, String> jsonmap = convert.convertFrom(TYPE_MAP_STRING_STRING, json);
        return new RetResult<>(jsonmap.get("openid"));
    }

    public RetResult<String> getMPWxunionidByCode(String code) {
        try {
            Map<String, String> wxmap = getMPUserTokenByCode(code);
            final String unionid = wxmap.get("unionid");
            if (unionid != null && !unionid.isEmpty()) return new RetResult<>(unionid);
            return new RetResult<>(1011002);
        } catch (IOException e) {
            return new RetResult<>(1011001);
        }
    }

    public Map<String, String> getMPUserTokenByCode(String code) throws IOException {
        return getMPUserTokenByCode(appid, appsecret, code);
    }

    public Map<String, String> getMPUserTokenByCode(String clientid, String code) throws IOException {
        MpElement element = this.clientidElements.get(clientid);
        return getMPUserTokenByCode(element == null ? appid : element.appid, element == null ? appsecret : element.appsecret, code);
    }

    public Map<String, String> getMPUserTokenByCodeAndAppid(String appid, String code) throws IOException {
        MpElement element = this.appidElements.get(appid);
        return getMPUserTokenByCode(element == null ? appid : element.appid, element == null ? appsecret : element.appsecret, code);
    }

    private Map<String, String> getMPUserTokenByCode(String appid0, String appsecret0, String code) throws IOException {
        if (code != null) code = code.replace("\"", "").replace("'", "");
        String url = "https://api.weixin.qq.com/sns/oauth2/access_token?appid=" + appid0 + "&secret=" + appsecret0 + "&code=" + code + "&grant_type=authorization_code";
        String json = getHttpContent(url);
        if (finest) logger.finest(url + "--->" + json);
        Map<String, String> jsonmap = convert.convertFrom(TYPE_MAP_STRING_STRING, json);
        return getMPUserTokenByOpenid(jsonmap.get("access_token"), jsonmap.get("openid"));
    }

    public Map<String, String> getMPUserTokenByOpenid(String access_token, String openid) throws IOException {
        String url = "https://api.weixin.qq.com/sns/userinfo?access_token=" + access_token + "&openid=" + openid;
        String json = getHttpContent(url);
        if (finest) logger.finest(url + "--->" + json);
        Map<String, String> jsonmap = convert.convertFrom(TYPE_MAP_STRING_STRING, json.replaceFirst("\\[.*\\]", "null"));
        return jsonmap;
    }

    public String verifyMPURL(String msgSignature, String timeStamp, String nonce, String echoStr) {
        return verifyMPURL0(mptoken, msgSignature, timeStamp, nonce, echoStr);
    }

    public String verifyMPURL(String clientid, String msgSignature, String timeStamp, String nonce, String echoStr) {
        MpElement element = this.clientidElements.get(clientid);
        return verifyMPURL0(element == null ? mptoken : element.mptoken, msgSignature, timeStamp, nonce, echoStr);
    }

    public String verifyMPURLByAppid(String appid, String msgSignature, String timeStamp, String nonce, String echoStr) {
        MpElement element = this.appidElements.get(appid);
        return verifyMPURL0(element == null ? mptoken : element.mptoken, msgSignature, timeStamp, nonce, echoStr);
    }

    private String verifyMPURL0(String mptoken, String msgSignature, String timeStamp, String nonce, String echoStr) {
        String signature = sha1(mptoken, timeStamp, nonce);
        if (!signature.equals(msgSignature)) throw new RuntimeException("signature verification error");
        return echoStr;
    }

    /**
     * 用SHA1算法生成安全签名
     * <p>
     * @param strings String[]
     *
     * @return 安全签名
     */
    protected static String sha1(String... strings) {
        try {
            Arrays.sort(strings);
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            for (String s : strings) md.update(s.getBytes());
            return Utility.binToHexString(md.digest());
        } catch (Exception e) {
            throw new RuntimeException("SHA encryption to generate signature failure", e);
        }
    }

    public static class MpElement {

        public String clientid = "";

        public String appid = "";

        public String appsecret = "";

        public String mptoken = "";

        public static Map<String, MpElement> create(Logger logger, Properties properties, File home) {
            String def_clientid = properties.getProperty("weixin.mp.clientid", "").trim();
            String def_appid = properties.getProperty("weixin.mp.appid", "").trim();
            String def_appsecret = properties.getProperty("weixin.mp.appsecret", "").trim();
            String def_mptoken = properties.getProperty("weixin.mp.mptoken", "").trim();

            final Map<String, MpElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("weixin.mp.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String clientid = properties.getProperty(prefix + ".clientid", def_clientid).trim();
                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String appsecret = properties.getProperty(prefix + ".appsecret", def_appsecret).trim();
                String mptoken = properties.getProperty(prefix + ".mptoken", def_mptoken).trim();

                if (appid.isEmpty() || appsecret.isEmpty()) {
                    logger.log(Level.WARNING, properties + "; has illegal weixinmp conf by prefix" + prefix);
                    return;
                }
                MpElement element = new MpElement();
                element.clientid = clientid;
                element.appid = appid;
                element.appsecret = appsecret;
                element.mptoken = mptoken;
                map.put(appid, element);
                if (def_appid.equals(appid)) map.put("", element);

            });
            return map;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }
}
