/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;
import org.redkale.service.RetResult;

/**
 *
 * @see http://redkale.org
 * @author zhangjx
 */
public abstract class Pays {

    //--------------------- 支付类型 -----------------------------
    //银联支付
    public static final short PAYTYPE_UNION = 10;

    //微信支付
    public static final short PAYTYPE_WEIXIN = 20;

    //支付宝支付
    public static final short PAYTYPE_ALIPAY = 30;

    //--------------------- 支付渠道 -----------------------------
    //网页支付
    public static final short PAYWAY_WEB = 10;

    //APP支付
    public static final short PAYWAY_APP = 20;

    //机器支付
    public static final short PAYWAY_NATIVE = 30;

    //--------------------- 支付状态 -----------------------------
    //待支付
    public static final short PAYSTATUS_UNPAY = 10;

    //支付中
    public static final short PAYSTATUS_PAYING = 20;

    //已支付
    public static final short PAYSTATUS_PAYOK = 30;

    //支付失败
    public static final short PAYSTATUS_PAYNO = 40;

    //待退款
    public static final short PAYSTATUS_UNREFUND = 50;

    //退款中
    public static final short PAYSTATUS_REFUNDING = 60;

    //已退款
    public static final short PAYSTATUS_REFUNDOK = 70;

    //退款失败
    public static final short PAYSTATUS_REFUNDNO = 80;

    //已取消
    public static final short PAYSTATUS_CLOSED = 90;

    //已取消
    public static final short PAYSTATUS_CANCELED = 95;

    //--------------------------------------------- 结果码 ----------------------------------------------
    @RetInfo("支付失败")
    public static final int RETPAY_PAY_ERROR = 20001001;

    @RetInfo("交易签名被篡改")
    public static final int RETPAY_FALSIFY_ERROR = 20001002;

    @RetInfo("支付状态异常")
    public static final int RETPAY_STATUS_ERROR = 20001003;
    
    @RetInfo("退款异常")
    public static final int RETPAY_REFUND_ERROR = 20001004;

    //---------------------------------------------- 微信结果码 -----------------------------------------
    @RetInfo("微信支付失败")
    public static final int RETPAY_WEIXIN_ERROR = 20010001;

    //---------------------------------------------- 支付宝结果码 -----------------------------------------
    @RetInfo("支付宝支付失败")
    public static final int RETPAY_ALIPAY_ERROR = 20020001;

    //---------------------------------------------------------------------------------------------------
    private static final Map<Integer, String> rets = new HashMap<>();

    static {
        for (Field field : Pays.class.getFields()) {
            if (!Modifier.isStatic(field.getModifiers())) continue;
            if (field.getType() != int.class) continue;
            RetInfo info = field.getAnnotation(RetInfo.class);
            if (info == null) continue;
            int value;
            try {
                value = field.getInt(null);
            } catch (Exception ex) {
                ex.printStackTrace();
                continue;
            }
            rets.put(value, info.value());
        }

    }

    public static RetResult create(int retcode) {
        if (retcode == 0) return RetResult.SUCCESS;
        return new RetResult(retcode, getRetInfo(retcode));
    }

    public static String getRetInfo(int retcode) {
        if (retcode == 0) return "成功";
        return rets.getOrDefault(retcode, "未知错误");
    }

    @Target(value = {ElementType.FIELD})
    @Retention(value = RetentionPolicy.RUNTIME)
    protected @interface RetInfo {

        String value();
    }
}
