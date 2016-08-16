/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import java.io.IOException;
import java.util.logging.*;
import javax.annotation.Resource;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.http.*;
import org.redkale.service.*;

/**
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 * @param <T>
 */
public abstract class RestHttpServlet<T> extends BasedHttpServlet {

    @RetLabel("Method Error")
    public static final int RET_REST_METHOD_ERROR = 21010001;

    @RetLabel("Server Error")
    public static final int RET_REST_SERVER_ERROR = 21010002;

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    protected final boolean fine = logger.isLoggable(Level.FINE);

    protected final boolean finer = logger.isLoggable(Level.FINER);

    protected final boolean finest = logger.isLoggable(Level.FINEST);

    @Resource
    protected JsonConvert convert;

    protected abstract T currentUser(HttpRequest req) throws IOException;

    /**
     * 检测Method是否合法，返回true表示合法
     *
     * @param req     HTTP请求对象
     * @param resp    HTTP响应对象
     * @param methods 有效方法名数组
     *
     * @return
     */
    protected boolean checkMethod(HttpRequest req, HttpResponse resp, String[] methods) {
        if (methods == null || methods.length == 0) return true;
        for (String m : methods) {
            if (req.getMethod().equalsIgnoreCase(m)) return true;
        }
        resp.finishJson(new RetResult(RET_REST_METHOD_ERROR, "Method Error"));
        return false;
    }

    /**
     * 异常输出
     *
     * @param req  HTTP请求对象
     * @param resp HTTP响应对象
     * @param exp  异常
     */
    protected void sendExceptionResult(HttpRequest req, HttpResponse resp, Throwable exp) {
        logger.log(Level.SEVERE, "request = " + req, exp);
        resp.finishJson(new RetResult(RET_REST_SERVER_ERROR, "Server Error"));
    }

}
