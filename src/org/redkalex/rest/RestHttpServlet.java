/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import java.io.IOException;
import javax.annotation.Resource;
import org.redkale.convert.json.JsonConvert;
import org.redkale.net.http.*;
import org.redkale.service.RetResult;
import org.redkale.source.Flipper;

/**
 *
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 * @param <T>
 */
public abstract class RestHttpServlet<T> extends BasedHttpServlet {

    @Resource
    protected JsonConvert convert;

    protected abstract T currentUser(HttpRequest req) throws IOException;

    protected abstract Class<T> sessionUserType();

    /**
     * 异常输出
     *
     * @param resp HTTP响应对象
     * @param exp  异常
     */
    protected void sendExceptionResult(HttpResponse resp, Throwable exp) {
        sendRetResult(resp, new RetResult(1, exp.toString()));
    }

    /**
     * 将对象以js方式输出
     *
     * @param resp   HTTP响应对象
     * @param var    对象名
     * @param result 对象
     */
    protected void sendJsResult(HttpResponse resp, String var, Object result) {
        resp.setContentType("application/javascript; charset=utf-8");
        resp.finish("var " + var + " = " + convert.convertTo(result) + ";");
    }

    /**
     * 将结果对象输出， 异常的结果在HTTP的header添加retcode值
     *
     * @param resp HTTP响应对象
     * @param ret  结果对象
     */
    protected void sendRetResult(HttpResponse resp, RetResult ret) {
        if (!ret.isSuccess()) {
            resp.addHeader("retcode", ret.getRetcode());
            resp.addHeader("retinfo", ret.getRetinfo());
        }
        resp.finishJson(ret);
    }

    /**
     * 将结果对象输出， 异常的结果在HTTP的header添加retcode值
     *
     * @param resp    HTTP响应对象
     * @param retcode 结果码
     */
    protected void sendRetcode(HttpResponse resp, int retcode) {
        if (retcode != 0) resp.addHeader("retcode", retcode);
        resp.finish("{\"retcode\":" + retcode + ", \"success\": " + (retcode == 0) + "}");
    }

    /**
     * 将结果对象输出， 异常的结果在HTTP的header添加retcode值
     *
     * @param resp    HTTP响应对象
     * @param retcode 结果码
     * @param retinfo 结果信息
     */
    protected void sendRetcode(HttpResponse resp, int retcode, String retinfo) {
        if (retcode != 0) resp.addHeader("retcode", retcode);
        if (retinfo != null && !retinfo.isEmpty()) resp.addHeader("retinfo", retinfo);
        resp.finish("{\"retcode\":" + retcode + ", \"success\": " + (retcode == 0) + "}");
    }

    /**
     * 获取翻页对象 http://demo.redkale.org/pipes/records/list/page:1/size:20  <br>
     * http://demo.redkale.org/pipes/records/list?flipper={'page':1,'size':20}  <br>
     * 以上两种接口都可以获取到翻页对象
     *
     * @param request HTTP请求对象
     *
     * @return
     */
    protected Flipper findFlipper(HttpRequest request) {
        return findFlipper(request, 0);
    }

    protected Flipper findFlipper(HttpRequest request, int defaultSize) {
        Flipper flipper = request.getJsonParameter(Flipper.class, "flipper");
        if (flipper == null) {
            int size = request.getRequstURIPath("size:", defaultSize);
            int page = request.getRequstURIPath("page:", 0);
            if (size > 0) flipper = page > 0 ? new Flipper(size, page) : new Flipper(size);
        }
        if (flipper == null) flipper = defaultSize > 0 ? new Flipper(defaultSize) : new Flipper();
        return flipper;
    }

}
