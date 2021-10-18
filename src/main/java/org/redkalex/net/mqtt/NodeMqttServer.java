/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.net.mqtt;

import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import org.redkale.boot.*;
import org.redkale.boot.ClassFilter.FilterEntry;
import org.redkale.mq.MessageAgent;
import org.redkale.net.*;
import org.redkale.service.*;
import org.redkale.util.*;
import org.redkale.util.AnyValue.DefaultAnyValue;

/**
 * MQTT Server节点的配置Server
 *
 * <p>
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
@NodeProtocol("MQTT")
public class NodeMqttServer extends NodeServer {

    protected final MqttServer mqttServer;

    private NodeMqttServer(Application application, AnyValue serconf) {
        super(application, createServer(application, serconf));
        this.mqttServer = (MqttServer) this.server;
        this.consumer = mqttServer == null || application.isSingletonMode() ? null : (agent, x) -> {//singleton模式下不生成MqttServlet
            if (x.getClass().getAnnotation(Local.class) != null) return; //本地模式的Service不生成MqttServlet
            MqttDynServlet servlet = mqttServer.addMqttServlet(x);
            dynServletMap.put(x, servlet);
            //if (agent != null) agent.putService(this, x, servlet);
        };
    }

    public static NodeServer createNodeServer(Application application, AnyValue serconf) {
        return new NodeMqttServer(application, serconf);
    }

    private static Server createServer(Application application, AnyValue serconf) {
        return new MqttServer(application, application.getStartTime(), serconf, application.getResourceFactory().createChild());
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return mqttServer == null ? null : mqttServer.getSocketAddress();
    }

    public void consumerAccept(MessageAgent messageAgent, Service service) {
        if (this.consumer != null) this.consumer.accept(messageAgent, service);
    }

    @Override
    public void init(AnyValue config) throws Exception {
        super.init(config);
        //-------------------------------------------------------------------
        if (mqttServer == null) return; //调试时server才可能为null
        final StringBuilder sb = logger.isLoggable(Level.FINE) ? new StringBuilder() : null;
        final String localThreadName = "[" + Thread.currentThread().getName() + "] ";
        List<MqttServlet> servlets = mqttServer.getMqttServlets();
        Collections.sort(servlets);
        for (MqttServlet en : servlets) {
            if (sb != null) sb.append(localThreadName).append(" Load ").append(en).append(LINE_SEPARATOR);
        }
        if (sb != null && sb.length() > 0) logger.log(Level.FINE, sb.toString());
    }

    @Override
    public boolean isSNCP() {
        return true;
    }

    public MqttServer getMqttServer() {
        return mqttServer;
    }

    @Override
    protected void loadFilter(ClassFilter<? extends Filter> filterFilter, ClassFilter otherFilter) throws Exception {
        if (mqttServer != null) loadMqttFilter(this.serverConf.getAnyValue("fliters"), filterFilter);
    }

    @SuppressWarnings("unchecked")
    protected void loadMqttFilter(final AnyValue servletsConf, final ClassFilter<? extends Filter> classFilter) throws Exception {
        final StringBuilder sb = logger.isLoggable(Level.INFO) ? new StringBuilder() : null;
        final String localThreadName = "[" + Thread.currentThread().getName() + "] ";
        List<FilterEntry<? extends Filter>> list = new ArrayList(classFilter.getFilterEntrys());
        for (FilterEntry<? extends Filter> en : list) {
            Class<MqttFilter> clazz = (Class<MqttFilter>) en.getType();
            if (Modifier.isAbstract(clazz.getModifiers())) continue;
            RedkaleClassLoader.putReflectionDeclaredConstructors(clazz, clazz.getName());
            final MqttFilter filter = clazz.getDeclaredConstructor().newInstance();
            resourceFactory.inject(filter, this);
            DefaultAnyValue filterConf = (DefaultAnyValue) en.getProperty();
            this.mqttServer.addMqttFilter(filter, filterConf);
            if (sb != null) sb.append(localThreadName).append(" Load ").append(clazz.getName()).append(LINE_SEPARATOR);
        }
        if (sb != null && sb.length() > 0) logger.log(Level.INFO, sb.toString());
    }

    @Override
    protected void loadServlet(ClassFilter<? extends Servlet> servletFilter, ClassFilter otherFilter) throws Exception {
        RedkaleClassLoader.putReflectionPublicClasses(MqttServlet.class.getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ClassFilter<Filter> createFilterClassFilter() {
        return createClassFilter(null, null, MqttFilter.class, null, null, "filters", "filter");
    }

    @Override
    protected ClassFilter<Servlet> createServletClassFilter() {
        return null;
    }

}
