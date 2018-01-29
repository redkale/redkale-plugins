/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.htel;

import org.redkale.net.http.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class HttpTemplateEngine implements org.redkale.net.http.HttpTemplateEngine {

    @Override
    public void init(HttpContext context, AnyValue config) {
    }

    @Override
    public void renderTo(HttpRequest request, HttpResponse response, HttpScope scope) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
