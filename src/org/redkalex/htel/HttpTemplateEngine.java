/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this referid file, choose Tools | Templates
 * and open the referid in the editor.
 */
package org.redkalex.htel;

import org.redkale.net.http.*;
import org.redkale.util.AnyValue;

/**
 * 尚未实现
 *
 * @author zhangjx
 */
public class HttpTemplateEngine implements org.redkale.net.http.HttpRender<HttpScope> {

    @Override
    public void init(HttpContext context, AnyValue config) {
    }

    @Override
    public void renderTo(HttpRequest request, HttpResponse response, HttpScope scope) {
        response.setContentType("text/html; charset=utf-8");
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Class<HttpScope> getType() {
        return HttpScope.class;
    }

}
