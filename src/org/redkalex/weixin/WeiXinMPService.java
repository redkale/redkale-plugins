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
import org.redkale.service.LocalService;
import org.redkale.service.Service;
import static org.redkale.convert.json.JsonConvert.TYPE_MAP_STRING_STRING;
import static org.redkale.util.Utility.getHttpContent;
import java.io.*;
import java.security.*;
import java.util.*;
import java.util.logging.*;
import javax.annotation.*;

/**
 * 微信服务号Service
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
@AutoLoad(false)
@LocalService
public class WeiXinMPService implements Service {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private final boolean finest = logger.isLoggable(Level.FINEST);

    private final boolean finer = logger.isLoggable(Level.FINER);

    @Resource
    protected JsonConvert convert;

    @Resource(name = "property.weixin.mp.appid") //公众账号ID
    protected String appid = "";

    @Resource(name = "property.weixin.mp.appsecret") // 
    protected String appsecret = "";

    @Resource(name = "property.weixin.mp.token")
    protected String mptoken = "";

    public WeiXinMPService() {
    }

    //-----------------------------------微信服务号接口----------------------------------------------------------
    //仅用于 https://open.weixin.qq.com/connect/oauth2/authorize  &scope=snsapi_base
    //需要在 “开发 - 接口权限 - 网页服务 - 网页帐号 - 网页授权获取用户基本信息”的配置选项中，修改授权回调域名
    public RetResult<String> getMPOpenidByCode(String code) throws IOException {
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
        String url = "https://api.weixin.qq.com/sns/oauth2/access_token?appid=" + appid + "&secret=" + appsecret + "&code=" + code + "&grant_type=authorization_code";
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
        String signature = sha1(mptoken, timeStamp, nonce);
        if (!signature.equals(msgSignature)) throw new RuntimeException("signature verification error");
        return echoStr;
    }

    /**
     * 用SHA1算法生成安全签名
     * <p>
     * @param strings
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
}
