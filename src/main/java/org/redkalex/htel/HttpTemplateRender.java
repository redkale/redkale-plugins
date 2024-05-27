/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this referid file, choose Tools | Templates
 * and open the referid in the editor.
 */
package org.redkalex.htel;

import java.io.*;
import org.redkale.annotation.Resource;
import org.redkale.convert.Convert;
import org.redkale.net.http.*;
import org.redkale.util.AnyValue;

/**
 * Http Template Express Language
 *
 * <p>尚未实现
 *
 * @author zhangjx
 */
public class HttpTemplateRender implements org.redkale.net.http.HttpRender {

    @Resource(name = "APP_HOME")
    protected File home;

    @Override
    public void init(HttpContext context, AnyValue config) {}

    @Override
    public void renderTo(HttpRequest request, HttpResponse response, Convert convert, HttpScope scope) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
