/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.pay;

import java.lang.annotation.*;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 本注解只能标注在AbstractPayService的子类上。
 *
 * <p>
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@Inherited
@Documented
@Target({TYPE})
@Retention(RUNTIME)
public @interface DIYPayService {

    /**
     * 支付类型， 必须大于50
     *
     * @return short
     */
    short paytype();
}
