/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

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
    
    //--------------------- 支付状态 -----------------------------
    //待支付
    public static final short PAYSTATUS_UNPAY = 10;

    //已支付
    public static final short PAYSTATUS_PAYOK = 30;
    

}
