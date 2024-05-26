/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.htel;

import java.io.*;
import org.redkale.annotation.Resource;
import org.redkale.convert.Convert;
import org.redkale.net.http.*;
import org.redkale.util.*;

/** @author zhangjx */
public class FreemarkerTemplateRender implements org.redkale.net.http.HttpRender {

    @Resource(name = "APP_HOME")
    private File home;

    private freemarker.template.Configuration engine;

    @Override
    public void init(HttpContext context, AnyValue config) {
        this.engine = new freemarker.template.Configuration(freemarker.template.Configuration.VERSION_2_3_31);
        String path = config == null
                ? new File(home, "templates").getPath()
                : config.getOrDefault("path", new File(home, "templates").getPath());
        try {
            this.engine.setDirectoryForTemplateLoading(new File(path));
        } catch (IOException e) {
            throw new RedkaleException(e);
        }
    }

    @Override
    public void renderTo(HttpRequest request, HttpResponse response, Convert convert, HttpScope scope) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            engine.getTemplate(scope.getReferid()).process(scope.getAttributes(), new OutputStreamWriter(out));
        } catch (Exception e) {
            throw new RedkaleException(e);
        }
        response.finish(out.toByteArray());
    }
}
