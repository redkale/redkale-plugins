/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import java.io.IOException;
import org.redkale.net.http.*;

/**
 * 默认Servlet, 没有配置RestHttpServlet实现类则使用该默认类
 * <p>
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
public class RestDefaultServlet extends RestHttpServlet<Object> {

    @Override
    protected Object currentUser(HttpRequest req) throws IOException {
        return new Object();
    }

    @Override
    protected Class<Object> sessionUserType() {
        return Object.class;
    }

    @Override
    public boolean authenticate(int module, int actionid, HttpRequest request, HttpResponse response) throws IOException {
        return true;
    }

}
