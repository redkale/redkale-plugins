/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import javax.annotation.Resource;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.redkale.convert.json.*;
import org.redkale.service.Local;
import org.redkale.util.*;
import static org.redkalex.pay.PayRetCodes.*;
import static org.redkalex.pay.Pays.PAYTYPE_GOOGLE;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Local
@AutoLoad(false)
public final class GooglePayService extends AbstractPayService {

    protected static final String format = "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS"; //yyyy-MM-dd HH:mm:ss

    //配置集合
    protected Map<String, GoogleElement> elements = new HashMap<>();

    @Resource(name = "property.pay.google.conf") //支付配置文件路径
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
//    private static final com.google.api.client.json.JsonFactory JSON_FACTORY =  com.google.api.client.json.jackson2.JacksonFactory.getDefaultInstance();
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
//    private Credential authorizeWithServiceAccount(String serviceAccountEmail) throws GeneralSecurityException, IOException {
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
//            JSON_FACTORY, clientSecrets,            Collections.singleton(AndroidPublisherScopes.ANDROIDPUBLISHER))
//            .setDataStoreFactory(dataStoreFactory).build();
//        // authorize
//        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize(INST_APP_USER_ID);
//    }
    @Override
    public void init(AnyValue conf) {
        if (this.convert == null) this.convert = JsonConvert.root();
        this.reloadConfig(Pays.PAYTYPE_GOOGLE);
    }

    @Override
    @Comment("判断是否支持指定支付类型")
    public boolean supportPayType(final short paytype) {
        return paytype == PAYTYPE_GOOGLE && !elements.isEmpty();
    }

    @Override
    @Comment("重新加载配置")
    public void reloadConfig(short paytype) {
        if (this.conf != null && !this.conf.isEmpty()) { //存在Google支付配置
            try {
                File file = (this.conf.indexOf('/') == 0 || this.conf.indexOf(':') > 0) ? new File(this.conf) : new File(home, "conf/" + this.conf);
                InputStream in = (file.isFile() && file.canRead()) ? new FileInputStream(file) : getClass().getResourceAsStream("/META-INF/" + this.conf);
                if (in == null) return;
                Properties properties = new Properties();
                properties.load(in);
                in.close();
                this.elements = GoogleElement.create(logger, properties);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "init google conf error", e);
            }
        }
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
        if (element == null) return result.retcode(RETPAY_CONF_ERROR).toFuture();
        result.setAppid(element.appid);
        final Map<String, String> rmap = new TreeMap<>();
        rmap.put("content", request.getPayno());
        result.setResult(rmap);
        return result.toFuture();
    }

    @Override
    public CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request) {
        if (request.getAttach() != null && request.getAttach().containsKey("bean")) {
            final GoogleElement element = elements.get(request.getAppid());
            String strbean = request.attach("bean");
            GoogleNotifyBean bean = JsonConvert.root().convertFrom(GoogleNotifyBean.class, strbean);
            PayNotifyResponse resp = new PayNotifyResponse();
            resp.setResponsetext(strbean);
            resp.setPayno(bean.payno());
            resp.setPaytype(request.getPaytype());
            return CompletableFuture.completedFuture(resp);
        }
        //待实现
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
    }

    @Override
    public CompletableFuture<PayQueryResponse> queryAsync(PayRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Not supported yet."));
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
    public CompletableFuture<PayRefundResponse> queryRefundAsync(PayRequest request) {
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
    public PayRefundResponse queryRefund(PayRequest request) {
        return queryRefundAsync(request).join();
    }

    @Override
    protected String createSign(AbstractPayService.PayElement element, Map<String, ?> map, String text) {
        throw new UnsupportedOperationException("Not supported yet."); 
    }

    @Override
    protected boolean checkSign(AbstractPayService.PayElement element, Map<String, ?> map, String text) {
        String[] signatures = text.split("\\.");
        byte[] sig = Base64.getDecoder().decode(signatures[0].replace('-', '+').replace('_', '/'));
        Map<String, String> smp = JsonConvert.root().convertFrom(JsonConvert.TYPE_MAP_STRING_STRING, Base64.getDecoder().decode(signatures[1]));
        if (!"HMAC-SHA256".equals(smp.get("algorithm"))) return false;
        byte[] keyBytes = ((GoogleElement) element).secret.getBytes();
        SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA256");
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(signingKey);
            byte[] rawHmac = mac.doFinal(signatures[1].getBytes());
            return Arrays.equals(sig, rawHmac);
        } catch (Exception e) {
            return false;
        }
    }

    public static class GoogleNotifyBean implements Serializable {

        // ----------- 第一种 -----------
        public String developerPayload;//payno

        public String paymentID;

        public String productID;

        public String purchaseTime;

        public String purchaseToken;

        public String signedRequest;

        // ----------- 第二种 -----------
        public String payment_id;

        public String amount; //5.00

        public String currency; //USD

        public String quantity; //1

        public String request_id; //payno

        public String status;

        public String signed_request;

        public String signature() {
            return signed_request == null || signed_request.isEmpty() ? signedRequest : signed_request;
        }

        public String payno() {
            return request_id == null || request_id.isEmpty() ? developerPayload : request_id;
        }

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static class GoogleElement extends AbstractPayService.PayElement {

        // pay.google.[x].appid
        public String appid = "";  //APP应用ID

        // pay.google.[x].secret
        public String secret = ""; //签名算法需要用到的密钥

        public static Map<String, GoogleElement> create(Logger logger, Properties properties) {
            String def_appid = properties.getProperty("pay.google.appid", "").trim();
            String def_secret = properties.getProperty("pay.google.secret", "").trim();
            String def_notifyurl = properties.getProperty("pay.google.notifyurl", "").trim();

            final Map<String, GoogleElement> map = new HashMap<>();
            properties.keySet().stream().filter(x -> x.toString().startsWith("pay.google.") && x.toString().endsWith(".appid")).forEach(appid_key -> {
                final String prefix = appid_key.toString().substring(0, appid_key.toString().length() - ".appid".length());

                String appid = properties.getProperty(prefix + ".appid", def_appid).trim();
                String secret = properties.getProperty(prefix + ".secret", def_secret).trim();
                String notifyurl = properties.getProperty(prefix + ".notifyurl", def_notifyurl).trim();

                if (appid.isEmpty() || notifyurl.isEmpty() || secret.isEmpty()) {
                    logger.log(Level.WARNING, properties + "; has illegal google conf by prefix" + prefix);
                    return;
                }
                GoogleElement element = new GoogleElement();
                element.appid = appid;
                element.secret = secret;
                element.notifyurl = notifyurl;
                if (element.initElement(logger, null)) {
                    map.put(appid, element);
                    if (def_appid.equals(appid)) map.put("", element);
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
