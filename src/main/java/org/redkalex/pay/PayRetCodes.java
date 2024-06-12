/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.util.concurrent.atomic.AtomicBoolean;
import org.redkale.annotation.AutoLoad;
import org.redkale.service.*;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@AutoLoad(false)
public abstract class PayRetCodes extends RetCodes {

    protected PayRetCodes() {}

    // --------------------------------------------- 支付模块结果码 ----------------------------------------------
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

    @RetLabel("不支持的支付类型")
    public static final int RETPAY_PAY_TYPEILLEGAL = 20010006;

    @RetLabel("支付超时")
    public static final int RETPAY_PAY_EXPIRED = 20010007;

    @RetLabel("支付信息不存在")
    public static final int RETPAY_PAY_RECORD_ILLEGAL = 20010008;

    @RetLabel("交易签名被篡改")
    public static final int RETPAY_FALSIFY_ERROR = 20010011;

    @RetLabel("支付状态异常")
    public static final int RETPAY_STATUS_ERROR = 20010012;

    @RetLabel("退款异常")
    public static final int RETPAY_REFUND_ERROR = 20010013;

    @RetLabel("退款失败")
    public static final int RETPAY_REFUND_FAILED = 20010014;

    @RetLabel("用户标识缺失")
    public static final int RETPAY_PARAM_ERROR = 20010021;

    protected static AtomicBoolean loaded = new AtomicBoolean();

    static void init() {
        if (loaded.compareAndSet(false, true)) {
            RetCodes.load(PayRetCodes.class);
        }
    }
}
