/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.weixin;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import javax.crypto.Cipher;
import javax.crypto.spec.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.*;
import org.redkale.convert.json.JsonConvert;
import static org.redkale.convert.json.JsonConvert.TYPE_MAP_STRING_STRING;
import org.redkale.service.*;
import org.redkale.util.*;
import static org.redkale.util.Utility.getHttpContentAsync;

/**
 * 微信服务号Service
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public final class WeiXinMPService implements Service {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    //配置集合, key: appid
    protected Map<String, MpElement> appidElements = new HashMap<>();

    //配置集合 key: clientid
    protected Map<String, MpElement> clientidElements = new HashMap<>();

    @Resource
    protected JsonConvert convert;

    @Resource(name = "weixin.mp.conf", required = false) //公众号配置文件路径
    protected String conf = "config.properties";

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource(name = "weixin.mp.clientid") //客户端ID
    protected String clientid = "";

    @Resource(name = "weixin.mp.appid") //公众账号ID
    protected String appid = "";

    @Resource(name = "weixin.mp.appsecret") // 
    protected String appsecret = "";

    @Resource(name = "weixin.mp.token")
    protected String mptoken = "";

    @Resource(name = "weixin.mp.miniprogram")
    protected boolean miniprogram;

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
    public CompletableFuture<RetResult<String>> getMPOpenidByCode(String code) {
        if (code != null) code = code.replace("\"", "").replace("'", "");
        String url = "https://api.weixin.qq.com/sns/oauth2/access_token?appid=" + appid + "&secret=" + appsecret + "&code=" + code + "&grant_type=authorization_code";
        return getHttpContentAsync(url).thenApply(json -> {
            if (logger.isLoggable(Level.FINEST)) logger.finest(url + "--->" + json);
            Map<String, String> jsonmap = convert.convertFrom(TYPE_MAP_STRING_STRING, json);
            return new RetResult<>(jsonmap.get("openid"));
        });
    }

    public CompletableFuture<RetResult<String>> getMPWxunionidByCode(String code) {
        return getMPUserTokenByCode(code).thenApply(wxmap -> {
            final String unionid = wxmap.get("unionid");
            if (unionid != null && !unionid.isEmpty()) return new RetResult<>(unionid);
            return new RetResult<>(1011002);
        });
    }

    public CompletableFuture<Map<String, String>> getMPUserTokenByCode(String code) {
        return getMPUserTokenByCode(miniprogram, appid, appsecret, code, null, null);
    }

    public CompletableFuture<Map<String, String>> getMPUserTokenByCode(String clientid, String code) {
        MpElement element = this.clientidElements.get(clientid);
        return getMPUserTokenByCode(element == null ? false : element.miniprogram, element == null ? appid : element.appid, element == null ? appsecret : element.appsecret, code, null, null);
    }

    public CompletableFuture<Map<String, String>> getMPUserTokenByCodeEncryptedData(String clientid, String code, String encryptedData, String iv) {
        MpElement element = this.clientidElements.get(clientid);
        return getMPUserTokenByCode(element == null ? false : element.miniprogram, element == null ? appid : element.appid, element == null ? appsecret : element.appsecret, code, encryptedData, iv);
    }

    public CompletableFuture<Map<String, String>> getMPUserTokenByCodeAndAppid(String appid, String code) {
        MpElement element = this.appidElements.get(appid);
        return getMPUserTokenByCode(element == null ? false : element.miniprogram, element == null ? appid : element.appid, element == null ? appsecret : element.appsecret, code, null, null);
    }

    private CompletableFuture<Map<String, String>> getMPUserTokenByCode(boolean miniprogram, String appid0, String appsecret0, String code, String encryptedData, String iv) {
        if (code != null) code = code.replace("\"", "").replace("'", "");
        String url0 = "https://api.weixin.qq.com/sns/oauth2/access_token?appid=" + appid0 + "&secret=" + appsecret0 + "&code=" + code + "&grant_type=authorization_code";
        if (miniprogram) url0 = "https://api.weixin.qq.com/sns/jscode2session?appid=" + appid0 + "&secret=" + appsecret0 + "&js_code=" + code + "&grant_type=authorization_code";
        final String url = url0;
        return getHttpContentAsync(url).thenCompose(json -> {
            if (logger.isLoggable(Level.FINEST)) logger.finest("url=" + url + ", encryptedData=" + encryptedData + ", iv=" + iv + "--->" + json);
            Map<String, String> jsonmap = convert.convertFrom(TYPE_MAP_STRING_STRING, json);
            if (miniprogram) {
                if (encryptedData != null && !encryptedData.isEmpty() && iv != null && !iv.isEmpty()) {
                    try {
                        String sessionkey = jsonmap.get("session_key");
                        if (sessionkey == null) return CompletableFuture.completedFuture(jsonmap);  //{"errcode":40163,"errmsg":"code been used, hints: [ req_id: GEbaO6yFe-g8Msfa ]"}
                        // 被加密的数据
                        byte[] dataByte = Base64.getDecoder().decode(encryptedData);
                        // 加密秘钥
                        byte[] keyByte = Base64.getDecoder().decode(sessionkey);
                        // 偏移量
                        byte[] ivByte = Base64.getDecoder().decode(iv);
                        int base = 16;
                        if (keyByte.length % base != 0) {
                            int groups = keyByte.length / base + (keyByte.length % base != 0 ? 1 : 0);
                            byte[] temp = new byte[groups * base];
                            Arrays.fill(temp, (byte) 0);
                            System.arraycopy(keyByte, 0, temp, 0, keyByte.length);
                            keyByte = temp;
                        }
                        SecretKeySpec spec = new SecretKeySpec(keyByte, "AES");
                        AlgorithmParameters parameters = AlgorithmParameters.getInstance("AES");
                        parameters.init(new IvParameterSpec(ivByte));
                        // 解密
                        Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
                        cipher.init(Cipher.DECRYPT_MODE, spec, parameters);// 初始化
                        byte[] resultByte = cipher.doFinal(dataByte);
                        String result = new String(resultByte, StandardCharsets.UTF_8);
                        if (logger.isLoggable(Level.FINEST)) logger.finest("url=" + url + ", session_key=" + sessionkey + ", encryptedData=" + encryptedData + ", iv=" + iv + "， decryptedData=" + result);
                        int pos = result.indexOf("\"watermark\"");
                        if (pos > 0) {
                            final String oldresult = result;
                            int pos1 = result.indexOf('{', pos);
                            int pos2 = result.indexOf('}', pos);
                            result = result.substring(0, pos1) + "null" + result.substring(pos2 + 1);
                            if (logger.isLoggable(Level.FINEST)) logger.finest("olddecrypt=" + oldresult + ", newdescrpty=" + result);
                        }
                        Map<String, String> map = convert.convertFrom(TYPE_MAP_STRING_STRING, result);
                        if (!map.containsKey("unionid") && map.containsKey("unionId")) {
                            map.put("unionid", map.get("unionId"));
                        }
                        if (!map.containsKey("openid") && map.containsKey("openId")) {
                            map.put("openid", map.get("openId"));
                        }
                        if (!map.containsKey("nickname") && map.containsKey("nickName")) {
                            map.put("nickname", map.get("nickName"));
                        }
                        if (!map.containsKey("headimgurl") && map.containsKey("avatarUrl")) {
                            map.put("headimgurl", map.get("avatarUrl"));
                        }
                        if (!map.containsKey("sex") && map.containsKey("gender")) {
                            map.put("sex", map.get("gender"));
                        }
                        jsonmap.putAll(map);
                    } catch (Exception ex) {
                        logger.log(Level.SEVERE, "url=" + url + ", encryptedData=" + encryptedData + ", iv=" + iv + " error", ex);
                    }
                }
                return CompletableFuture.completedFuture(jsonmap);
            }
            return getMPUserTokenByOpenid(jsonmap.get("access_token"), jsonmap.get("openid"));
        });
    }

    public CompletableFuture<Map<String, String>> getMPUserTokenByOpenid(String access_token, String openid) {
        String url = "https://api.weixin.qq.com/sns/userinfo?access_token=" + access_token + "&openid=" + openid;
        return getHttpContentAsync(url).thenApply(json -> {
            if (logger.isLoggable(Level.FINEST)) logger.finest(url + "--->" + json);
            Map<String, String> jsonmap = convert.convertFrom(TYPE_MAP_STRING_STRING, json.replaceFirst("\\[.*\\]", "null"));
            return jsonmap;
        });
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

        public String mptoken = "";

        public String appid = "";

        public String appsecret = "";

        public boolean miniprogram = false;

        public static Map<String, MpElement> create(Logger logger, Properties properties, File home) {
            String def_clientid = properties.getProperty("weixin.mp.clientid", "").trim();
            String def_appid = properties.getProperty("weixin.mp.appid", "").trim();
            String def_appsecret = properties.getProperty("weixin.mp.appsecret", "").trim();
            String def_mptoken = properties.getProperty("weixin.mp.mptoken", "").trim();
            boolean def_miniprogram = "true".equalsIgnoreCase(properties.getProperty("weixin.mp.miniprogram", "false").trim());

            final Map<String, MpElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("weixin.mp.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String clientid = properties.getProperty(prefix + ".clientid", def_clientid).trim();
                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String appsecret = properties.getProperty(prefix + ".appsecret", def_appsecret).trim();
                String mptoken = properties.getProperty(prefix + ".mptoken", def_mptoken).trim();
                boolean miniprogram = "true".equalsIgnoreCase(properties.getProperty(prefix + ".miniprogram", String.valueOf(def_miniprogram)).trim());
                if (appid.isEmpty() || appsecret.isEmpty()) {
                    logger.log(Level.WARNING, properties + "; has illegal weixinmp conf by prefix" + prefix);
                    return;
                }
                MpElement element = new MpElement();
                element.clientid = clientid;
                element.appid = appid;
                element.appsecret = appsecret;
                element.mptoken = mptoken;
                element.miniprogram = miniprogram;
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
