/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import java.util.*;
import java.util.logging.*;
import org.redkale.boot.*;
import static org.redkale.boot.NodeServer.LINE_SEPARATOR;
import org.redkale.net.http.*;
import org.redkale.util.AnyValue;
import org.redkale.util.AnyValue.DefaultAnyValue;

/**
 * REST服务拦截器 待实现
 * <p>
 * 详情见: http://redkale.org
 *
 * @author zhangjx
 */
public class RestNodeInterceptor extends NodeInterceptor {

    @Override
    public void preStart(final NodeServer nodeServer) {
        final HttpServer server = (HttpServer) nodeServer.getServer();
        final AnyValue serverConf = nodeServer.getServerConf();
        final String prefix = serverConf == null ? "" : serverConf.getValue("path", "");
        AnyValue restConf = serverConf == null ? null : serverConf.getAnyValue("rest");
        if (restConf == null) restConf = new DefaultAnyValue();
        try {
            final Logger logger = nodeServer.getLogger();
            final StringBuilder sb = logger.isLoggable(Level.INFO) ? new StringBuilder() : null;
            final String threadName = "[" + Thread.currentThread().getName() + "] ";
            final List<AbstractMap.SimpleEntry<String, String[]>> ss = sb == null ? null : new ArrayList<>();

            final Class<? extends RestHttpServlet> superClass = (Class<? extends RestHttpServlet>) Class.forName(restConf.getValue("servlet", DefaultRestServlet.class.getName()));
            nodeServer.getLocalServiceWrappers().forEach((wrapper) -> {
                RestHttpServlet servlet = ServletBuilder.createRestServlet(superClass, wrapper);
                if (servlet == null) return;
                server.addHttpServlet(servlet, prefix, wrapper.getConf());
                if (ss != null) {
                    String[] mappings = servlet.getClass().getAnnotation(WebServlet.class).value();
                    for (int i = 0; i < mappings.length; i++) {
                        mappings[i] = prefix + mappings[i];
                    }
                    ss.add(new AbstractMap.SimpleEntry<>(servlet.getClass().getName(), mappings));
                }
            });
            //输出信息
            if (ss != null) {
                Collections.sort(ss, (AbstractMap.SimpleEntry<String, String[]> o1, AbstractMap.SimpleEntry<String, String[]> o2) -> o1.getKey().compareTo(o2.getKey()));
                int max = 0;
                for (AbstractMap.SimpleEntry<String, String[]> as : ss) {
                    if (as.getKey().length() > max) max = as.getKey().length();
                }
                for (AbstractMap.SimpleEntry<String, String[]> as : ss) {
                    sb.append(threadName).append(" Loaded ").append(as.getKey());
                    for (int i = 0; i < max - as.getKey().length(); i++) {
                        sb.append(' ');
                    }
                    sb.append("  mapping to  ").append(Arrays.toString(as.getValue())).append(LINE_SEPARATOR);
                }
            }
            if (sb != null && sb.length() > 0) logger.log(Level.INFO, sb.toString());
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preShutdown(NodeServer nodeServer) {

    }
}
