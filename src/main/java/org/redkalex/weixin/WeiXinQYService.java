/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.weixin;

import static org.redkale.util.Utility.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.logging.*;
import javax.crypto.Cipher;
import javax.crypto.spec.*;
import org.redkale.annotation.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Comment;
import org.redkale.annotation.ResourceChanged;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceEvent;
import org.redkale.service.*;
import org.redkale.util.*;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@Component
@AutoLoad(false)
public final class WeiXinQYService extends AbstractService {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private static class Token {

        public String token;

        public long expires = 7100000;

        public long accesstime;
    }

    private static final String BASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private static final Random RANDOM = new Random();

    protected static final Type MAPTYPE = new TypeToken<Map<String, String>>() {}.getType();

    @Resource
    protected JsonConvert convert;

    // ------------------------------------------------------------------------------------------------------
    // http://oa.xxxx.com/pipes/wx/verifyqy
    @Resource(name = "weixin.qy.token", required = false)
    protected String qytoken = "";

    @Resource(name = "weixin.qy.corpid", required = false)
    protected String qycorpid = "wxYYYYYYYYYYYYYYYY";

    @Resource(name = "weixin.qy.aeskey", required = false)
    protected String qyaeskey = "";

    @Resource(name = "weixin.qy.secret", required = false)
    protected String qysecret = "#########################";

    private SecretKeySpec qykeyspec;

    private IvParameterSpec qyivspec;

    private final Token qyAccessToken = new Token();

    // ------------------------------------------------------------------------------------------------------
    public WeiXinQYService() {}

    @ResourceChanged
    @Comment("通过配置中心更改配置后的回调")
    void onResourceChanged(ResourceEvent[] events) {
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            if (event.name().contains("aeskey")) {
                qykeyspec = null;
            }
            sb.append("@Resource = ").append(event.name()).append(" resource changed\r\n");
        }
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
    }

    // -----------------------------------微信企业号接口----------------------------------------------------------
    public CompletableFuture<Map<String, String>> getQYUserCode(String code, String agentid) throws IOException {
        return getQYAccessToken().thenCompose(token -> {
            String url = "https://qyapi.weixin.qq.com/cgi-bin/user/getuserinfo?access_token=" + token + "&code=" + code
                    + "&agentid=" + agentid;
            return getHttpContentAsync(url).thenApply(json -> {
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(url + "--->" + json);
                }
                return (Map<String, String>) convert.convertFrom(MAPTYPE, json);
            });
        });
    }

    public CompletableFuture<Void> sendQYTextMessage(String agentid, String message) {
        return sendQYMessage(new WeiXinQYMessage(agentid, message));
    }

    public CompletableFuture<Void> sendQYTextMessage(String agentid, Supplier<String> contentSupplier) {
        return sendQYMessage(new WeiXinQYMessage(agentid, contentSupplier));
    }

    public CompletableFuture<Void> sendQYMessage(WeiXinQYMessage message) {
        CompletableFuture future = new CompletableFuture();
        runAsync(() -> {
            String result = null;
            try {
                message.supplyContent();
                if (message.getText() == null) { // 没内容不需要发
                    future.complete(null);
                    return;
                }
                String url = "https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token=" + getQYAccessToken();
                result = postHttpContent(url, convert.convertTo(message));
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest("sendQYMessage ok: " + message + " -> " + result);
                }
                future.complete(null);
            } catch (Exception e) {
                future.completeExceptionally(e);
                logger.log(Level.WARNING, "sendQYMessage error: " + message + " -> " + result, e);
            }
        });
        return future;
    }

    public String verifyQYURL(String msgSignature, String timeStamp, String nonce, String echoStr) {
        String signature = sha1(qytoken, timeStamp, nonce, echoStr);
        if (!signature.equals(msgSignature)) {
            throw new RedkaleException("signature verification error");
        }
        return decryptQY(echoStr);
    }

    protected CompletableFuture<String> getQYAccessToken() throws IOException {
        if (qyAccessToken.accesstime < System.currentTimeMillis() - qyAccessToken.expires) {
            qyAccessToken.token = null;
        }
        if (qyAccessToken.token == null) {
            String url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid=" + qycorpid + "&corpsecret=" + qysecret;
            return getHttpContentAsync(url).thenApply(json -> {
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(url + "--->" + json);
                }
                Map<String, String> jsonmap = convert.convertFrom(MAPTYPE, json);
                qyAccessToken.accesstime = System.currentTimeMillis();
                qyAccessToken.token = jsonmap.get("access_token");
                String exp = jsonmap.get("expires_in");
                if (exp != null) {
                    qyAccessToken.expires = (Integer.parseInt(exp) - 100) * 1000;
                }
                return qyAccessToken.token;
            });
        }
        return CompletableFuture.completedFuture(qyAccessToken.token);
    }

    /**
     * 将公众平台回复用户的消息加密打包.
     *
     * <ol>
     *   <li>对要发送的消息进行AES-CBC加密
     *   <li>生成安全签名
     *   <li>将消息密文和安全签名打包成xml格式
     * </ol>
     *
     * <p>
     *
     * @param replyMsg 公众平台待回复用户的消息，xml格式的字符串
     * @param timeStamp 时间戳，可以自己生成，也可以用URL参数的timestamp
     * @param nonce 随机串，可以自己生成，也可以用URL参数的nonce
     *     <p>
     * @return 加密后的可以直接回复用户的密文，包括msg_signature, timestamp, nonce, encrypt的xml格式的字符串
     */
    protected String encryptQYMessage(String replyMsg, String timeStamp, String nonce) {
        // 加密
        String encrypt = encryptQY(random16String(), replyMsg);

        // 生成安全签名
        if (timeStamp == null || timeStamp.isEmpty()) {
            timeStamp = Long.toString(System.currentTimeMillis());
        }
        String signature = sha1(qytoken, timeStamp, nonce, encrypt);

        // 生成发送的xml
        return "<xml>\n<Encrypt><![CDATA[" + encrypt + "]]></Encrypt>\n"
                + "<MsgSignature><![CDATA[" + signature + "]]></MsgSignature>\n"
                + "<TimeStamp>" + timeStamp + "</TimeStamp>\n"
                + "<Nonce><![CDATA[" + nonce + "]]></Nonce>\n</xml>";
    }

    protected String decryptQYMessage(String msgSignature, String timeStamp, String nonce, String postData) {
        // 密钥，公众账号的app secret
        // 提取密文
        String encrypt = postData.substring(
                postData.indexOf("<Encrypt><![CDATA[") + "<Encrypt><![CDATA[".length(),
                postData.indexOf("]]></Encrypt>"));
        // 验证安全签名
        if (!sha1(qytoken, timeStamp, nonce, encrypt).equals(msgSignature)) {
            throw new RedkaleException("signature verification error");
        }
        return decryptQY(encrypt);
    }

    /**
     * 对明文进行加密.
     *
     * <p>
     *
     * @param randomStr String
     * @param text 需要加密的明文
     * @return 加密后base64编码的字符串
     */
    protected String encryptQY(String randomStr, String text) {
        ByteArray bytes = new ByteArray();
        byte[] randomStrBytes = randomStr.getBytes(StandardCharsets.UTF_8);
        byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);
        byte[] corpidBytes = qycorpid.getBytes(StandardCharsets.UTF_8);

        // randomStr + networkBytesOrder + text + qycorpid
        bytes.put(randomStrBytes);
        bytes.putInt(textBytes.length);
        bytes.put(textBytes);
        bytes.put(corpidBytes);

        // ... + pad: 使用自定义的填充方式对明文进行补位填充
        byte[] padBytes = encodePKCS7(bytes.length());
        bytes.put(padBytes);

        // 获得最终的字节流, 未加密
        try {
            // 加密
            byte[] encrypted = createQYCipher(Cipher.ENCRYPT_MODE).doFinal(bytes.content(), 0, bytes.length());
            // 使用BASE64对加密后的字符串进行编码
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            throw new RedkaleException("AES加密失败", e);
        }
    }

    protected String decryptQY(String text) {
        byte[] original;
        try {
            // 使用BASE64对密文进行解码
            original = createQYCipher(Cipher.DECRYPT_MODE)
                    .doFinal(Base64.getDecoder().decode(text));
        } catch (Exception e) {
            throw new RedkaleException("AES解密失败", e);
        }
        try {
            // 去除补位字符
            byte[] bytes = decodePKCS7(original);
            // 分离16位随机字符串,网络字节序和corpid
            int xmlLength =
                    (bytes[16] & 0xFF) << 24 | (bytes[17] & 0xFF) << 16 | (bytes[18] & 0xFF) << 8 | bytes[19] & 0xFF;
            if (!qycorpid.equals(
                    new String(bytes, 20 + xmlLength, bytes.length - 20 - xmlLength, StandardCharsets.UTF_8))) {
                throw new RedkaleException("corpid校验失败");
            }
            return new String(bytes, 20, xmlLength, StandardCharsets.UTF_8);
        } catch (RuntimeException e) {
            if (e.getMessage().contains("corpid")) {
                throw e;
            }
            throw new RedkaleException("解密后得到的buffer非法", e);
        }
    }

    protected Cipher createQYCipher(int mode) throws Exception {
        Cipher cipher = Cipher.getInstance(
                "AES/CBC/NoPadding"); // AES192、256位加密解密 需要将新版 local_policy.jar、US_export_policy.jar两个文件覆盖到
        // ${JDK_HOME}/jre/lib/security/下
        if (qykeyspec == null) {
            byte[] aeskeyBytes = Base64.getDecoder().decode(qyaeskey + "=");
            qykeyspec = new SecretKeySpec(aeskeyBytes, "AES");
            qyivspec = new IvParameterSpec(aeskeyBytes, 0, 16);
        }
        cipher.init(mode, qykeyspec, qyivspec);
        return cipher;
    }

    // -----------------------------------通用接口----------------------------------------------------------
    // 随机生成16位字符串
    protected static String random16String() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 16; i++) {
            sb.append(BASE.charAt(RANDOM.nextInt(BASE.length())));
        }
        return sb.toString();
    }

    /**
     * 用SHA1算法生成安全签名
     *
     * <p>
     *
     * @param strings String[]
     * @return 安全签名
     */
    protected static String sha1(String... strings) {
        try {
            Arrays.sort(strings);
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            for (String s : strings) md.update(s.getBytes());
            return Utility.binToHexString(md.digest());
        } catch (Exception e) {
            throw new RedkaleException("SHA encryption to generate signature failure", e);
        }
    }

    /**
     * 获得对明文进行补位填充的字节.
     *
     * <p>
     *
     * @param count 需要进行填充补位操作的明文字节个数
     * @return 补齐用的字节数组
     */
    private static byte[] encodePKCS7(int count) {
        // 计算需要填充的位数
        int amountToPad = 32 - (count % 32);
        if (amountToPad == 0) {
            amountToPad = 32;
        }
        // 获得补位所用的字符
        char padChr = (char) (byte) (amountToPad & 0xFF);
        StringBuilder tmp = new StringBuilder();
        for (int index = 0; index < amountToPad; index++) {
            tmp.append(padChr);
        }
        return tmp.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 删除解密后明文的补位字符
     *
     * <p>
     *
     * @param decrypted 解密后的明文
     * @return 删除补位字符后的明文
     */
    private static byte[] decodePKCS7(byte[] decrypted) {
        int pad = (int) decrypted[decrypted.length - 1];
        if (pad < 1 || pad > 32) {
            pad = 0;
        }
        return Arrays.copyOfRange(decrypted, 0, decrypted.length - pad);
    }
}
