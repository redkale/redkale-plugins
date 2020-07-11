/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.*;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.*;
import org.redkale.util.*;
import static org.redkale.util.Utility.remoteHttpContent;

/**
 * 支付抽象类
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Comment("支付服务抽象类")
public abstract class AbstractPayService implements Service {

    protected static final Charset UTF8 = Charset.forName("UTF-8");

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected final boolean fine = logger.isLoggable(Level.FINE);

    protected final boolean finer = logger.isLoggable(Level.FINER);

    protected final boolean finest = logger.isLoggable(Level.FINEST);

    @Comment("判断是否支持指定支付类型")
    public abstract boolean supportPayType(short paytype);

    @Comment("重新加载配置")
    public abstract void reloadConfig(short paytype);

    //--------------------------- 同步方法 ------------------------------
    @Comment("手机预支付")
    public abstract PayPreResponse prepay(PayPreRequest request);

    @Comment("手机支付回调")
    public abstract PayNotifyResponse notify(PayNotifyRequest request);

    @Comment("请求支付")
    public abstract PayCreatResponse create(PayCreatRequest request);

    @Comment("请求查询")
    public abstract PayQueryResponse query(PayRequest request);

    @Comment("请求关闭")
    public abstract PayResponse close(PayCloseRequest request);

    @Comment("请求退款")
    public abstract PayRefundResponse refund(PayRefundRequest request);

    @Comment("查询退款")
    public abstract PayRefundResponse queryRefund(PayRequest request);

    //--------------------------- 异步方法 ------------------------------
    @Comment("手机预支付")
    public abstract CompletableFuture<PayPreResponse> prepayAsync(PayPreRequest request);

    @Comment("手机支付回调")
    public abstract CompletableFuture<PayNotifyResponse> notifyAsync(PayNotifyRequest request);

    @Comment("请求支付")
    public abstract CompletableFuture<PayCreatResponse> createAsync(PayCreatRequest request);

    @Comment("请求查询")
    public abstract CompletableFuture<PayQueryResponse> queryAsync(PayRequest request);

    @Comment("请求关闭")
    public abstract CompletableFuture<PayResponse> closeAsync(PayCloseRequest request);

    @Comment("请求退款")
    public abstract CompletableFuture<PayRefundResponse> refundAsync(PayRefundRequest request);

    @Comment("查询退款")
    public abstract CompletableFuture<PayRefundResponse> queryRefundAsync(PayRequest request);

    //------------------------------------------------------------------
    @Comment("计算签名")
    protected abstract String createSign(final PayElement element, Map<String, ?> map);

    @Comment("验证签名")
    protected abstract boolean checkSign(final PayElement element, Map<String, ?> map);

    @Comment("获取配置项")
    public abstract PayElement getPayElement(String appid);

    protected String postHttpContent(String url, String body) {
        try {
            return remoteHttpContent(null, "POST", url, 10, null, body).toString("UTF-8");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected String postHttpContent(String url, Charset charset, String body) {
        return postHttpContent(null, url, charset, body);
    }

    protected String postHttpContent(SSLContext ssl, String url, Charset charset, String body) {
        try {
            return remoteHttpContent(ssl, "POST", url, 10, null, body).toString(charset.name());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    //待实现
    protected CompletableFuture<String> postHttpContentAsync(SSLContext ssl, String url, Charset charset, String body) {
        return CompletableFuture.completedFuture(postHttpContent(ssl, url, charset, body));
    }

    protected CompletableFuture<String> postHttpContentAsync(SSLContext ssl, String url, String body) {
        return CompletableFuture.completedFuture(postHttpContent(ssl, url, StandardCharsets.UTF_8, body));
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, Charset charset, String body) {
        return postHttpContentAsync(null, url, charset, body);
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, String body) {
        return postHttpContentAsync(url, StandardCharsets.UTF_8, body);
    }

    @Comment("map对象转换成 key1=value1&key2=value2&key3=value3")
    protected String joinMap(Map<String, ?> map) {
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        return map.entrySet().stream().map((e -> e.getKey() + "=" + e.getValue())).collect(Collectors.joining("&"));
    }

    protected String urlEncodeUTF8(Object val) {
        if (val == null) return null;
        try {
            return URLEncoder.encode(val.toString(), "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Comment("支付配置信息抽象类")
    public static abstract class PayElement {

        public String notifyurl = ""; //回调url

        public abstract boolean initElement(Logger logger, File home);

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static void main(String[] args) throws Throwable {
        File file = new File("apiclient_cert.p12");
        System.out.println(Base64.getEncoder().encodeToString(Utility.readBytesThenClose(new FileInputStream(file))));
    }
}
