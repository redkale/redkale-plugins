/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.redkale.service.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public abstract class PayRetCodes {

    protected PayRetCodes() {
    }

    //--------------------------------------------- 支付模块结果码 ----------------------------------------------
    @RetLabel("支付失败")
    public static final int RETPAY_PAY_ERROR = 20010001;

    @RetLabel("第三方支付失败")
    public static final int RETPAY_PAY_FAILED = 20010002;

    @RetLabel("支付配置异常")
    public static final int RETPAY_CONF_ERROR = 20010003;

    @RetLabel("重复支付")
    public static final int RETPAY_PAY_REPEAT = 20010004;

    @RetLabel("等待用户支付")
    public static final int RETPAY_PAY_WAITING = 20010005;

    @RetLabel("交易签名被篡改")
    public static final int RETPAY_FALSIFY_ERROR = 20010011;

    @RetLabel("支付状态异常")
    public static final int RETPAY_STATUS_ERROR = 20010012;

    @RetLabel("退款异常")
    public static final int RETPAY_REFUND_ERROR = 20010013;

    @RetLabel("用户标识缺失")
    public static final int RETPAY_OPENID_ERROR = 20010021;

    //-----------------------------------------------------------------------------------------------------------
    protected static final Map<String, Map<Integer, String>> rets = RetLabel.RetLoader.loadMap(PayRetCodes.class);

    protected static final Map<Integer, String> defret = rets.get("");

    public static RetResult retResult(int retcode) {
        if (retcode == 0) return RetResult.success();
        return new RetResult(retcode, retInfo(retcode));
    }

    public static RetResult retResult(String locale, int retcode) {
        if (retcode == 0) return RetResult.success();
        return new RetResult(retcode, retInfo(locale, retcode));
    }

    public static RetResult retResult(int retcode, Object... args) {
        if (retcode == 0) return RetResult.success();
        if (args == null || args.length < 1) return new RetResult(retcode, retInfo(retcode));
        String info = MessageFormat.format(retInfo(retcode), args);
        return new RetResult(retcode, info);
    }

    public static RetResult retResult(String locale, int retcode, Object... args) {
        if (retcode == 0) return RetResult.success();
        if (args == null || args.length < 1) return new RetResult(retcode, retInfo(locale, retcode));
        String info = MessageFormat.format(retInfo(locale, retcode), args);
        return new RetResult(retcode, info);
    }

    public static <T> CompletableFuture<RetResult<T>> retResultFuture(int retcode) {
        return CompletableFuture.completedFuture(retResult(retcode));
    }

    public static <T> CompletableFuture<RetResult<T>> retResultFuture(String locale, int retcode) {
        return CompletableFuture.completedFuture(retResult(locale, retcode));
    }

    public static <T> CompletableFuture<RetResult<T>> retResultFuture(int retcode, Object... args) {
        return CompletableFuture.completedFuture(retResult(retcode, args));
    }

    public static <T> CompletableFuture<RetResult<T>> retResultFuture(String locale, int retcode, Object... args) {
        return CompletableFuture.completedFuture(retResult(locale, retcode, args));
    }

    public static RetResult set(RetResult result, int retcode, Object... args) {
        if (retcode == 0) return result.retcode(0).retinfo("");
        if (args == null || args.length < 1) return result.retcode(retcode).retinfo(retInfo(retcode));
        String info = MessageFormat.format(retInfo(retcode), args);
        return result.retcode(retcode).retinfo(info);
    }

    public static RetResult set(RetResult result, String locale, int retcode, Object... args) {
        if (retcode == 0) return result.retcode(0).retinfo("");
        if (args == null || args.length < 1) return result.retcode(retcode).retinfo(retInfo(locale, retcode));
        String info = MessageFormat.format(retInfo(locale, retcode), args);
        return result.retcode(retcode).retinfo(info);
    }

    public static String retInfo(int retcode) {
        if (retcode == 0) return "Success";
        return defret.getOrDefault(retcode, "Error");
    }

    public static String retInfo(String locale, int retcode) {
        if (locale == null || locale.isEmpty()) return retInfo(retcode);
        if (retcode == 0) return "Success";
        String key = locale == null ? "" : locale;
        Map<Integer, String> map = rets.get(key);
        if (map == null) return "Error";
        return map.getOrDefault(retcode, "Error");
    }
}
