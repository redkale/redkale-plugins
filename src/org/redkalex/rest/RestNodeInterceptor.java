/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import java.lang.reflect.Field;
import java.util.*;
import java.util.logging.*;
import java.util.regex.Pattern;
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
        final AnyValue servletsConf = serverConf == null ? null : serverConf.getAnyValue("servlets");
        final String prefix = servletsConf == null ? "" : servletsConf.getValue("path", "");
        AnyValue restConf = serverConf == null ? null : serverConf.getAnyValue("rest");
        if (restConf == null) restConf = new DefaultAnyValue();
        try {
            final Logger logger = nodeServer.getLogger();
            final StringBuilder sb = logger.isLoggable(Level.INFO) ? new StringBuilder() : null;
            final String threadName = "[" + Thread.currentThread().getName() + "] ";
            final List<AbstractMap.SimpleEntry<String, String[]>> ss = sb == null ? null : new ArrayList<>();

            final Class<? extends RestHttpServlet> superClass = (Class<? extends RestHttpServlet>) Class.forName(restConf.getValue("servlet", DefaultRestServlet.class.getName()));

            final boolean autoload = restConf.getBoolValue("autoload", true);
            final boolean mustsign = restConf.getBoolValue("mustsign", true); //是否只加载标记@RestController的Service类
            final Pattern[] includes = ClassFilter.toPattern(restConf.getValue("includes", "").split(";"));
            final Pattern[] excludes = ClassFilter.toPattern(restConf.getValue("excludes", "").split(";"));
            final Set<String> hasServices = new HashSet<>();
            for (AnyValue item : restConf.getAnyValues("service")) {
                hasServices.add(item.getValue("value", ""));
            }

            nodeServer.getInterceptorServiceWrappers().forEach((wrapper) -> {
                if (!wrapper.getName().isEmpty()) return;  //只加载resourceName为空的service
                final Class stype = wrapper.getType();
                if (mustsign && stype.getAnnotation(RestController.class) == null) return;

                final String stypename = stype.getName();
                if (stypename.startsWith("org.redkalex.")) return;
                if (!autoload && !hasServices.contains(stypename)) return;
                if (excludes != null && !hasServices.contains(stypename)) {
                    for (Pattern reg : excludes) {
                        if (reg.matcher(stypename).matches()) return;
                    }
                }
                if (includes != null && !hasServices.contains(stypename)) {
                    boolean match = false;
                    for (Pattern reg : includes) {
                        if (reg.matcher(stypename).matches()) {
                            match = true;
                            break;
                        }
                    }
                    if (!match) return;
                }

                RestHttpServlet servlet = RestServletBuilder.createRestServlet(superClass, wrapper.getName(), stype);
                if (servlet == null) return;
                try {
                    Field serviceField = servlet.getClass().getDeclaredField("_service");
                    serviceField.setAccessible(true);
                    serviceField.set(servlet, wrapper.getService());
                } catch (Exception e) {
                    throw new RuntimeException(wrapper.getType() + " generate rest servlet error", e);
                }
                server.addHttpServlet(servlet, prefix, (AnyValue) null);
                if (ss != null) {
                    String[] mappings = servlet.getClass().getAnnotation(WebServlet.class).value();
                    for (int i = 0; i < mappings.length; i++) {
                        mappings[i] = prefix + mappings[i];
                    }
                    ss.add(new AbstractMap.SimpleEntry<>(servlet.getClass().getName(), mappings));
                }
            });
            //输出信息
            if (ss != null && sb != null) {
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
