/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import java.lang.annotation.*;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 只能依附在Service类的方法的参数上
 * <p>
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
@Inherited
@Documented
@Target({PARAMETER})
@Retention(RUNTIME)
public @interface RestParam {

    String value() default ""; //参数名, 默认值为方法反射得来的名称

    String defvalue() default ""; //默认值 如果类型是数值型， 默认值为0， 其他对象为null
}
