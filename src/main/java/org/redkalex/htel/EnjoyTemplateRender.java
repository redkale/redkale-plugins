/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.htel;

import java.io.*;
import java.util.Collection;
import java.util.logging.*;
import org.redkale.annotation.Resource;
import org.redkale.convert.Convert;
import org.redkale.net.http.*;
import org.redkale.util.*;

/**
 * JFinal 的 Enjoy 4.9.08模板引擎
 *
 * @author zhangjx
 */
public class EnjoyTemplateRender implements org.redkale.net.http.HttpRender {

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());

    @Resource(name = "APP_HOME")
    private File home;

    @Resource(name = "SERVER_ROOT")
    private String root;

    private com.jfinal.template.Engine engine;

    @Override
    public void init(HttpContext context, AnyValue config) {
        String engineName = "engine:" + context.getServerAddress().getHostString() + ":"
                + context.getServerAddress().getPort();
        this.engine = com.jfinal.template.Engine.use(engineName);
        if (this.engine == null) this.engine = com.jfinal.template.Engine.create(engineName);
        this.engine.setDevMode(logger.isLoggable(Level.FINE));
        final com.jfinal.template.EngineConfig engineConfig = this.engine.getEngineConfig();
        String defroot = root == null ? new File(home, "templates").getPath() : root;
        String path = config == null ? defroot : config.getOrDefault("path", defroot);
        if (path.isEmpty()) path = defroot; // path可能会配置为""
        engineConfig.setBaseTemplatePath(path);
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
                if (engineConfig.getSharedObjectMap() != null
                        && engineConfig.getSharedObjectMap().containsKey(name)) continue;
                try {
                    Class clazz = Thread.currentThread().getContextClassLoader().loadClass(resvalue);
                    Object val = context.getResourceFactory().find(resname == null ? "" : resname, clazz);
                    RedkaleClassLoader.putReflectionPublicConstructors(clazz, clazz.getName());
                    if (val == null) { // class可能不是Service，不会被自动注入
                        val = clazz.getConstructor().newInstance();
                    }
                    engineConfig.addSharedObject(name, val);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "sharekit name=" + name + " inject error", e);
                }
            }
        }
        if (engineConfig.getDirective("size") == null) {
            engineConfig.addDirective("size", Size.class);
            RedkaleClassLoader.putReflectionPublicConstructors(Size.class, Size.class.getName());
            engineConfig.addSharedMethod(new Size());
        }
        if (engineConfig.getDirective("brescape") == null) {
            engineConfig.addDirective("brescape", BrEscape.class);
            RedkaleClassLoader.putReflectionPublicConstructors(BrEscape.class, BrEscape.class.getName());
            {
                Class clz = com.jfinal.template.ext.directive.EscapeDirective.class;
                RedkaleClassLoader.putReflectionPublicConstructors(clz, clz.getName());
                clz = com.jfinal.kit.StrKit.class;
                engineConfig.addSharedMethod(new com.jfinal.kit.StrKit());
                RedkaleClassLoader.putReflectionPublicConstructors(clz, clz.getName());
                clz = com.jfinal.kit.HashKit.class;
                engineConfig.addSharedMethod(new com.jfinal.kit.HashKit());
                RedkaleClassLoader.putReflectionPublicConstructors(clz, clz.getName());
                clz = com.jfinal.kit.PropKit.class;
                RedkaleClassLoader.putReflectionPublicConstructors(clz, clz.getName());
            }
        }
    }

    @Override
    public void renderTo(HttpRequest request, HttpResponse response, Convert convert, HttpScope scope) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        engine.getTemplate(scope.getReferid()).render(scope.getAttributes(), out);
        response.finish(out.toByteArray());
    }

    public static class Size extends com.jfinal.template.Directive {

        @Override
        public void exec(
                com.jfinal.template.Env env,
                com.jfinal.template.stat.Scope scope,
                com.jfinal.template.io.Writer writer) {
            try {
                writer.write(size(exprList.eval(scope)));
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new com.jfinal.template.TemplateException(e.getMessage(), location, e);
            }
        }

        public static int size(Object value) {
            if (value == null) {
                return (0);
            } else if (value instanceof String) {
                return (value.toString().length());
            } else if (value instanceof boolean[]) {
                return (((boolean[]) value).length);
            } else if (value instanceof byte[]) {
                return (((byte[]) value).length);
            } else if (value instanceof short[]) {
                return (((short[]) value).length);
            } else if (value instanceof char[]) {
                return (((char[]) value).length);
            } else if (value instanceof int[]) {
                return (((int[]) value).length);
            } else if (value instanceof long[]) {
                return (((long[]) value).length);
            } else if (value instanceof float[]) {
                return (((float[]) value).length);
            } else if (value instanceof double[]) {
                return (((double[]) value).length);
            } else if (value instanceof Collection) {
                return (((Collection) value).size());
            } else {
                return (((Object[]) value).length);
            }
        }
    }

    public static class BrEscape extends com.jfinal.template.Directive {

        @Override
        public void exec(
                com.jfinal.template.Env env,
                com.jfinal.template.stat.Scope scope,
                com.jfinal.template.io.Writer writer) {
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
