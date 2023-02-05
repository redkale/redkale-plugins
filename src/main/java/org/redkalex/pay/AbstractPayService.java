/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.io.*;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.*;
import org.redkale.annotation.Comment;
import org.redkale.convert.json.JsonConvert;
import org.redkale.service.Service;
import org.redkale.util.*;

/**
 * 支付抽象类
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Comment("支付服务抽象类")
public abstract class AbstractPayService implements Service {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    @Comment("判断是否支持指定支付类型")
    public abstract boolean supportPayType(short paytype);

    @Comment("重新加载本地文件配置")
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
    public abstract PayRefundResponse queryRefund(PayRefundQryReq request);

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
    public abstract CompletableFuture<PayRefundResponse> queryRefundAsync(PayRefundQryReq request);

    //------------------------------------------------------------------
    @Comment("计算签名; map和text只会存在一个有值")
    protected abstract String createSign(final PayElement element, Map<String, ?> map, String text);

    @Comment("验证签名; map和text只会存在一个有值")
    protected abstract boolean checkSign(final PayElement element, Map<String, ?> map, String text, Map<String, String> respHeaders);

    @Comment("获取配置项")
    public abstract PayElement getPayElement(String appid);

    protected String postHttpContent(String url, String body) {
        try {
            return Utility.remoteHttpContent("POST", url, 10_000, null, body).toString(StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new RedkaleException(ex);
        }
    }

    protected String postHttpContent(String url, Charset charset, String body) {
        try {
            return Utility.remoteHttpContent("POST", url, 10_000, null, body).toString(charset.name());
        } catch (IOException ex) {
            throw new RedkaleException(ex);
        }
    }

    protected CompletableFuture<byte[]> postHttpBytesContentAsync(String url, Map<String, String> headers, String body) {
        return Utility.postHttpBytesContentAsync(url, headers, body);
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, Charset charset, String body) {
        return Utility.postHttpContentAsync(url, charset, body);
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, String body) {
        return Utility.postHttpContentAsync(url, StandardCharsets.UTF_8, body);
    }

    protected CompletableFuture<String> postHttpContentAsync(HttpClient client, String url, String body) {
        return Utility.postHttpContentAsync(client, url, StandardCharsets.UTF_8, body);
    }

    protected CompletableFuture<String> postHttpContentAsync(HttpClient client, String url, Map<String, String> headers, String body) {
        return Utility.postHttpContentAsync(client, url, StandardCharsets.UTF_8, headers, body);
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, Charset charset, Map<String, String> headers, String body) {
        return Utility.postHttpContentAsync(url, charset, headers, body);
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, Map<String, String> headers, String body) {
        return Utility.postHttpContentAsync(url, StandardCharsets.UTF_8, headers, body);
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, Charset charset, String body, Map<String, String> respHeaders) {
        return Utility.postHttpContentAsync(url, charset, body, respHeaders);
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, String body, Map<String, String> respHeaders) {
        return Utility.postHttpContentAsync(url, StandardCharsets.UTF_8, body, respHeaders);
    }

    protected CompletableFuture<String> postHttpContentAsync(HttpClient client, String url, String body, Map<String, String> respHeaders) {
        return Utility.postHttpContentAsync(client, url, StandardCharsets.UTF_8, body, respHeaders);
    }

    protected CompletableFuture<String> getHttpContentAsync(HttpClient client, String url, String body, Map<String, String> respHeaders) {
        return Utility.getHttpContentAsync(client, url, StandardCharsets.UTF_8, body, respHeaders);
    }

    protected CompletableFuture<String> postHttpContentAsync(HttpClient client, String url, Map<String, String> headers, String body, Map<String, String> respHeaders) {
        return Utility.postHttpContentAsync(client, url, StandardCharsets.UTF_8, headers, body, respHeaders);
    }

    protected static CompletableFuture<String> postHttpContentAsync(String url, int timeout, Map<String, String> headers, String body, Map<String, String> respHeaders) {
        return Utility.postHttpContentAsync(url, timeout, StandardCharsets.UTF_8, headers, body, respHeaders);
    }

    protected CompletableFuture<String> getHttpContentAsync(HttpClient client, String url, Map<String, String> headers, String body, Map<String, String> respHeaders) {
        return Utility.getHttpContentAsync(client, url, StandardCharsets.UTF_8, headers, body, respHeaders);
    }

    protected CompletableFuture<String> postHttpContentAsync(String url, Charset charset, Map<String, String> headers, String body, Map<String, String> respHeaders) {
        return Utility.postHttpContentAsync(url, charset, headers, body, respHeaders);
    }

    protected static CompletableFuture<String> postHttpContentAsync(String url, Map<String, String> headers, String body, Map<String, String> respHeaders) {
        return Utility.postHttpContentAsync(url, StandardCharsets.UTF_8, headers, body, respHeaders);
    }

    protected static CompletableFuture<String> getHttpContentAsync(String url, Map<String, String> headers, String body, Map<String, String> respHeaders) {
        return Utility.getHttpContentAsync(url, StandardCharsets.UTF_8, headers, body, respHeaders);
    }

    protected static CompletableFuture<String> getHttpContentAsync(String url, int timeout, Map<String, String> headers, String body, Map<String, String> respHeaders) {
        return Utility.getHttpContentAsync(url, timeout, StandardCharsets.UTF_8, headers, body, respHeaders);
    }

    @Comment("map对象转换成 key1=value1&key2=value2&key3=value3")
    protected final String joinMap(Map<String, ?> map) {
        return joinMap(map, null);
    }

    @Comment("map对象转换成 key1=value1&key2=value2&key3=value3")
    protected String joinMap(Map<String, ?> map, Set<String> keys) {
        if (!(map instanceof SortedMap)) map = new TreeMap<>(map);
        Stream<Map.Entry<String, ?>> stream = (Stream) map.entrySet().stream();
        if (keys != null && !keys.isEmpty()) stream = stream.filter(e -> keys.contains(e.getKey()));
        return stream.map((e -> e.getKey() + "=" + e.getValue())).collect(Collectors.joining("&"));
    }

    protected String urlEncodeUTF8(Object val) {
        if (val == null) return null;
        return URLEncoder.encode(val.toString(), StandardCharsets.UTF_8);
    }

    protected String urlEncode(Object val, Charset charset) {
        if (val == null) return null;
        return URLEncoder.encode(val.toString(), charset);
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

}
