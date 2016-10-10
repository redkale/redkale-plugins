/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.email;

import java.lang.reflect.*;
import java.util.*;
import org.redkale.service.*;
import org.redkalex.pay.Pays;

/**
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public abstract class EmailCodes {

    //--------------------------------------------- 结果码 ----------------------------------------------
    @RetLabel("邮箱参数不正确")
    public static final int RETMAIL_PARAM_ILLEAL = 20110001;

    @RetLabel("邮箱发送失败")
    public static final int RETMAIL_SEND_ERROR = 20110002;

    //---------------------------------------------------------------------------------------------------
    private static final Map<Integer, String> rets = new HashMap<>();

    static {
        for (Field field : Pays.class.getFields()) {
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

    //---------------------------------------------------------------------------------------------------
    private EmailCodes() {
    }

    public static RetResult retResult(int retcode) {
        if (retcode == 0) return RetResult.success();
        return new RetResult(retcode, retInfo(retcode));
    }

    public static String retInfo(int retcode) {
        if (retcode == 0) return "成功";
        return rets.getOrDefault(retcode, "未知错误");
    }
}
