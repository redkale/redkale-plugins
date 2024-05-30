/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import static org.redkalex.pay.PayRetCodes.*;
import static org.redkalex.pay.Pays.PAYTYPE_GOOGLE;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import org.redkale.annotation.*;
import org.redkale.annotation.AutoLoad;
import org.redkale.annotation.Comment;
import org.redkale.annotation.ResourceChanged;
import org.redkale.convert.json.JsonConvert;
import org.redkale.inject.ResourceEvent;
import org.redkale.service.Local;
import org.redkale.util.*;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public final class GooglePayService extends AbstractPayService {

    protected static final String format = "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS"; // yyyy-MM-dd HH:mm:ss

    // 原始的配置
    protected Properties elementProps = new Properties();

    // 配置对象集合
    protected Map<String, GoogleElement> elements = new HashMap<>();

    @Resource
    @Comment("必须存在全局配置项，@ResourceListener才会起作用")
    protected Environment environment;

    @Resource(name = "pay.google.conf", required = false) // 支付配置文件路径
    protected String conf = "config.properties";

    @Resource(name = "APP_HOME")
    protected File home;

    @Resource
    protected JsonConvert convert;

    //    public static void main(String[] args) throws Throwable {
    //        String json = "{\n"
    //            + "  \"installed\": {\n"
    //            + "    \"client_id\": \"223352421296-8fta4mjf2c49mijhj3n7o3kjkplt7clq.apps.googleusercontent.com\",\n"
    //            + "    \"client_secret\": \"8E7zJlSDniucmceGhThKO0PS\",\n"
    //            + "    \"redirect_uris\": [],\n"
    //            + "    \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
    //            + "    \"token_uri\": \"https://accounts.google.com/o/oauth2/token\"\n"
    //            + "  }\n"
    //            + "}";
    //
    //    }
    //
    //    private static final String DATA_STORE_SYSTEM_PROPERTY = "user.home";
    //    private static final String DATA_STORE_FILE = ".store/android_publisher_api";
    //    private static final File DATA_STORE_DIR =
    //            new File(System.getProperty(DATA_STORE_SYSTEM_PROPERTY), DATA_STORE_FILE);
    //
    //    /** Global instance of the JSON factory. */
    //    private static final com.google.api.client.json.JsonFactory JSON_FACTORY =
    // com.google.api.client.json.jackson2.JacksonFactory.getDefaultInstance();
    //
    //    /** Global instance of the HTTP transport. */
    //    private static HttpTransport HTTP_TRANSPORT;
    //
    //    /** Installed application user ID. */
    //    private static final String INST_APP_USER_ID = "user";
    //
    //    /**
    //     * Global instance of the {@link DataStoreFactory}. The best practice is to
    //     * make it a single globally shared instance across your application.
    //     */
    //    private static FileDataStoreFactory dataStoreFactory;
    //
    //    private Credential authorizeWithServiceAccount(String serviceAccountEmail) throws GeneralSecurityException,
    // IOException {
    //        logger.info(String.format("Authorizing using Service Account: %s", serviceAccountEmail));
    //
    //        // Build service account credential.
    //        GoogleCredential credential = new GoogleCredential.Builder()
    //            .setTransport(HTTP_TRANSPORT)
    //            .setJsonFactory(JSON_FACTORY)
    //            .setServiceAccountId(serviceAccountEmail)
    //            .setServiceAccountScopes(
    //                Collections.singleton(AndroidPublisherScopes.ANDROIDPUBLISHER))
    //            .setServiceAccountPrivateKeyFromP12File(new File(SRC_RESOURCES_KEY_P12))
    //            .build();
    //        return credential;
    //    }
    //
    //    /**
    //     * Authorizes the installed application to access user's protected data.
    //     *
    //     * @throws IOException
    //     * @throws GeneralSecurityException
    //     */
    //    private Credential authorizeWithInstalledApplication() throws IOException {
    //        logger.info("Authorizing using installed application");
    //
    //        // load client secrets
    //        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
    //            new InputStreamReader(
    //                AndroidPublisherHelper.class
    //                    .getResourceAsStream(RESOURCES_CLIENT_SECRETS_JSON)));
    //        // Ensure file has been filled out.
    //        checkClientSecretsFile(clientSecrets);
    //
    //        dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
    //
    //        // set up authorization code flow
    //        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT,
    //            JSON_FACTORY, clientSecrets,
    // Collections.singleton(AndroidPublisherScopes.ANDROIDPUBLISHER))
    //            .setDataStoreFactory(dataStoreFactory).build();
    //        // authorize
    //        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize(INST_APP_USER_ID);
    //    }
    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) {
            this.convert = JsonConvert.root();
        }
        this.reloadConfig(Pays.PAYTYPE_GOOGLE);
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short payType) {
        return payType == PAYTYPE_GOOGLE && !elements.isEmpty();
    }

    @Override
    @Comment("重新加载本地文件配置")
    public void reloadConfig(short payType) {
        Properties properties = new Properties();
        if (this.conf != null && !this.conf.isEmpty()) { // 存在Google支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0)
                        ? new File(this.conf)
                        : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead())
                        ? new FileInputStream(file)
                        : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in != null) {
                    properties.load(in);
                    in.close();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init google conf error", e);
            }
        }
        this.environment.forEach(k -> k.startsWith("pay.google."), (k, v) -> properties.put(k, v));
        this.elements = GoogleElement.create(logger, properties);
        this.elementProps = properties;
    }

    @ResourceChanged //     //
    @Comment("通过配置中心更改配置后的回调")
    void onResourceChanged(ResourceEvent[] events) {
        Properties changeProps = new Properties();
        changeProps.putAll(this.elementProps);
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            if (event.name().startsWith("pay.google.")) {
                changeProps.put(event.name(), event.newValue().toString());
                sb.append("@Resource change '")
                        .append(event.name())
                        .append("' to '")
                        .append(event.coverNewValue())
                        .append("'\r\n");
            }
        }
        if (sb.length() < 1) {
            return; // 无相关配置变化
        }
        logger.log(Level.INFO, sb.toString());
        this.elements = GoogleElement.create(logger, changeProps);
        this.elementProps = changeProps;
    }

    public void setPayElements(Map<String, GoogleElement> elements) {
        this.elements = elements;
    }

    public void putPayElements(Map<String, GoogleElement> elements) {
        this.elements.putAll(elements);
    }

    @Override
    public GoogleElement getPayElement(String appid) {
        return this.elements.get(appid);
    }

    public void setPayElement(String appid, GoogleElement element) {
        this.elements.put(appid, element);
    }

    public boolean existsPayElement(String appid) {
        return this.elements != null && this.elements.containsKey(appid);
    }

    @Override
    public CompletableFuture<PayPreResponse> prepayAsync(PayPreRequest request) {
        request.checkVaild();
        final PayPreResponse result = new PayPreResponse();
        final GoogleElement element = elements.get(request.getAppid());
        if (element == null) {
            return result.retcode(RETPAY_CONF_ERROR).toFuture();
        }
        result.setAppid(element.appid);
        //        final Map<String, String> rmap = new TreeMap<>();
        //        rmap.put("content", request.getPayno());
        //        result.setResult(rmap);
        return result.toFuture();
    }

    @Override
    public CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request) {
        String strbean;
        if (request.getAttach() != null && request.getAttach().containsKey("bean")) {
            strbean = request.attach("bean");
        } else {
            strbean = request.getBody();
        }
        if (strbean != null && !strbean.isEmpty()) {
            final GoogleElement element = elements.get(request.getAppid());
            GoogleNotifyBean bean = JsonConvert.root().convertFrom(GoogleNotifyBean.class, strbean);
            PayNotifyResponse resp = new PayNotifyResponse();
            resp.setResponseText(strbean);
            resp.setPayno(bean.payno());
            resp.setPayType(request.getPayType());
            if (!checkSign(element, bean.purchaseData, bean.signature)) {
                return resp.retcode(RETPAY_FALSIFY_ERROR).notifytext(strbean).toFuture();
            }
            return CompletableFuture.completedFuture(resp);
        }
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayQueryResponse> queryAsync(PayRequest request) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<PayResponse> closeAsync(PayCloseRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayRefundResponse> refundAsync(PayRefundRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayRefundResponse> queryRefundAsync(PayRefundQryReq request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public PayPreResponse prepay(final PayPreRequest request) {
        return prepayAsync(request).join();
    }

    @Override
    public PayNotifyResponse notify(PayNotifyRequest request) {
        return notifyAsync(request).join();
    }

    @Override
    public PayCreatResponse create(PayCreatRequest request) {
        return createAsync(request).join();
    }

    @Override
    public PayQueryResponse query(PayRequest request) {
        return queryAsync(request).join();
    }

    @Override
    public PayResponse close(PayCloseRequest request) {
        return closeAsync(request).join();
    }

    @Override
    public PayRefundResponse refund(PayRefundRequest request) {
        return refundAsync(request).join();
    }

    @Override
    public PayRefundResponse queryRefund(PayRefundQryReq request) {
        return queryRefundAsync(request).join();
    }

    @Override
    protected String createSign(AbstractPayService.PayElement element, Map<String, ?> map, String text) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    protected boolean checkSign(
            AbstractPayService.PayElement element,
            Map<String, ?> map,
            String text,
            Map<String, Serializable> respHeaders) {
        return checkSign((GoogleElement) element, (String) map.get("purchaseData"), (String) map.get("signature"));
    }

    protected boolean checkSign(GoogleElement element, String purchaseData, String sign) {
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PublicKey pubKey = keyFactory.generatePublic(new X509EncodedKeySpec(element.pubkey));
            Signature signature = Signature.getInstance("SHA1WithRSA");
            signature.initVerify(pubKey);
            signature.update(purchaseData.getBytes(StandardCharsets.UTF_8));
            return signature.verify(Base64.getDecoder().decode(sign));
        } catch (Exception e) {
            return false;
        }
    }

    public static class GoogleNotifyBean implements Serializable {

        public String payno; // payno

        public String purchaseData;

        public String signature;

        public String signature() {
            return signature;
        }

        public String payno() {
            return payno;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class GoogleElement extends AbstractPayService.PayElement {

        // pay.google.[x].appid
        public String appid = ""; // APP应用ID
        // pay.google.[x].publickey

        public byte[] pubkey; // publicKey

        // pay.google.[x].certpath
        public String certpath = ""; // 签名算法需要用到的密钥

        public static Map<String, GoogleElement> create(Logger logger, Properties properties) {
            String def_appid = properties.getProperty("pay.google.appid", "").trim();
            String def_publickey =
                    properties.getProperty("pay.google.publickey", "").trim();
            String def_certpath =
                    properties.getProperty("pay.google.certpath", "").trim();
            String def_notifyurl =
                    properties.getProperty("pay.google.notifyurl", "").trim();

            final Map<String, GoogleElement> map = new HashMap<>();
            properties.keySet().stream()
                    .filter(x -> x.toString().startsWith("pay.google.")
                            && x.toString().endsWith(".appid"))
                    .forEach(appid_key -> {
                        final String prefix = appid_key
                                .toString()
                                .substring(0, appid_key.toString().length() - ".appid".length());

                        String appid = properties
                                .getProperty(prefix + ".appid", def_appid)
                                .trim();
                        String publickey = properties
                                .getProperty(prefix + ".publickey", def_publickey)
                                .trim();
                        String certpath = properties
                                .getProperty(prefix + ".certpath", def_certpath)
                                .trim();
                        String notifyurl = properties
                                .getProperty(prefix + ".notifyurl", def_notifyurl)
                                .trim();

                        if (appid.isEmpty() || certpath.isEmpty()) {
                            logger.log(Level.WARNING, properties + "; has illegal google conf by prefix" + prefix);
                            return;
                        }
                        GoogleElement element = new GoogleElement();
                        element.appid = appid;
                        element.pubkey = Base64.getDecoder().decode(publickey);
                        element.certpath = certpath;
                        element.notifyurl = notifyurl;
                        if (element.initElement(logger, null)) {
                            map.put(appid, element);
                            if (def_appid.equals(appid)) {
                                map.put("", element);
                            }
                        }
                    });
            return map;
        }

        @Override
        public boolean initElement(Logger logger, File home) {
            return true;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }
}
