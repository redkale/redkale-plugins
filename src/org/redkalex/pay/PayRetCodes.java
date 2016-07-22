/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.lang.reflect.*;
import java.util.*;
import org.redkale.service.*;

/**
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

    @RetLabel("交易签名被篡改")
    public static final int RETPAY_FALSIFY_ERROR = 20010003;

    @RetLabel("支付状态异常")
    public static final int RETPAY_STATUS_ERROR = 20010004;

    @RetLabel("退款异常")
    public static final int RETPAY_REFUND_ERROR = 20010005;

    //-----------------------------------------------------------------------------------------------------------
    protected static final Map<Integer, String> rets = new HashMap<>();

    static {
        load(Pays.class);
    }

    protected static void load(Class clazz) {
        for (Field field : clazz.getFields()) {
            if (!Modifier.isStatic(field.getModifiers())) continue;
            if (field.getType() != int.class) continue;
            RetLabel info = field.getAnnotation(RetLabel.class);
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

    public static RetResult retResult(int retcode) {
        if (retcode == 0) return RetResult.SUCCESS;
        return new RetResult(retcode, retInfo(retcode));
    }

    public static String retInfo(int retcode) {
        if (retcode == 0) return "成功";
        return rets.getOrDefault(retcode, "未知错误");
    }
}
