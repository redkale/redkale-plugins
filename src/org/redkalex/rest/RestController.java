/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import java.lang.annotation.*;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 只能依附在Service类上，value默认为"/" + Service的类名去掉Service字样的字符串 (如HelloService，的默认路径为/hello)。
 * <p>
 * <p>
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
@Inherited
@Documented
@Target({TYPE})
@Retention(RUNTIME)
public @interface RestController {

    boolean ignore() default false; //是否屏蔽该类的转换

    String[] value() default {}; //模块的路径

    boolean repair() default true; //同&#64;WebServlet的repair属性

    int module() default 0; //模块ID值，鉴权时用到

}
