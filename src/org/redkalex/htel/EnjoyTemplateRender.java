/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.htel;

import java.io.*;
import java.util.logging.*;
import javax.annotation.Resource;
import org.redkale.convert.Convert;
import org.redkale.net.http.*;
import org.redkale.util.AnyValue;

/**
 * JFinal 的 Enjoy 4.9.08模板引擎
 *
 * @author zhangjx
 */
public class EnjoyTemplateRender implements org.redkale.net.http.HttpRender {

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());

    @Resource(name = "APP_HOME")
    private File home;

    private com.jfinal.template.Engine engine;

    @Override
    public void init(HttpContext context, AnyValue config) {
        this.engine = com.jfinal.template.Engine.use();
        this.engine.setDevMode(logger.isLoggable(Level.FINE));
        String path = config == null ? new File(home, "templates").getPath() : config.getOrDefault("path", new File(home, "templates").getPath());
        this.engine.getEngineConfig().setBaseTemplatePath(path);
        if (config != null) {
            for (AnyValue kit : config.getAnyValues("sharekit")) {
                String name = kit.getValue("name");
                if (name == null || name.isEmpty()) continue;
                String resname = kit.getValue("resname");
                String resvalue = kit.getValue("resvalue");
                if (resvalue == null || resvalue.isEmpty()) {
                    logger.log(Level.WARNING, "sharekit resvalue is empty");
                    continue;
                }
                try {
                    Class clazz = Class.forName(resvalue);
                    Object val = context.getResourceFactory().find(resname == null ? "" : resname, clazz);
                    this.engine.getEngineConfig().addSharedObject(name, val);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "sharekit name=" + name + " inject error", e);
                }
            }
        }
        this.engine.getEngineConfig().addDirective("brescape", BrEscape.class);
    }

    @Override
    public void renderTo(HttpRequest request, HttpResponse response, Convert convert, HttpScope scope) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        engine.getTemplate(scope.getReferid()).render(scope.getAttributes(), out);
        response.finish(out.toByteArray());
    }

    public static class BrEscape extends com.jfinal.template.Directive {

        @Override
        public void exec(com.jfinal.template.Env env, com.jfinal.template.stat.Scope scope, com.jfinal.template.io.Writer writer) {
            try {
                Object value = exprList.eval(scope);
                if (value instanceof String) {
                    escape((String) value, writer);
                } else if (value instanceof Number) {
                    Class<?> c = value.getClass();
                    if (c == Integer.class) {
                        writer.write((Integer) value);
                    } else if (c == Long.class) {
                        writer.write((Long) value);
                    } else if (c == Double.class) {
                        writer.write((Double) value);
                    } else if (c == Float.class) {
                        writer.write((Float) value);
                    } else {
                        writer.write(value.toString());
                    }
                } else if (value != null) {
                    escape(value.toString(), writer);
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new com.jfinal.template.TemplateException(e.getMessage(), location, e);
            }
        }

        private void escape(String str, com.jfinal.template.io.Writer w) throws IOException {
            for (int i = 0, len = str.length(); i < len; i++) {
                char cur = str.charAt(i);
                switch (cur) {
                    case '<':
                        w.write("&lt;");
                        break;
                    case '>':
                        w.write("&gt;");
                        break;
                    case '"':
                        w.write("&quot;");
                        break;
                    case '\'':
                        // w.write("&apos;");	// IE 不支持 &apos; 考虑 &#39;
                        w.write("&#39;");
                        break;
                    case '&':
                        w.write("&amp;");
                        break;
                    case '\n':
                        w.write("<br/>");
                        break;
                    default:
                        w.write(str, i, 1);
                        break;
                }
            }
        }

    }
}
