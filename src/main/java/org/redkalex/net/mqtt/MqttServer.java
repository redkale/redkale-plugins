/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.net.mqtt;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.*;
import org.redkale.boot.Application;
import org.redkale.net.*;
import org.redkale.service.Service;
import org.redkale.util.*;
import org.redkalex.net.mqtt.MqttContext.MqttContextConfig;

/**
 *
 * @author zhangjx
 */
public class MqttServer extends Server<String, MqttContext, MqttRequest, MqttResponse, MqttServlet> {

    private final AtomicInteger maxTypeLength = new AtomicInteger();

    private final AtomicInteger maxNameLength = new AtomicInteger();

    public MqttServer() {
        this(null, System.currentTimeMillis(), null, ResourceFactory.create());
    }

    public MqttServer(ResourceFactory resourceFactory) {
        this(null, System.currentTimeMillis(), null, resourceFactory);
    }

    public MqttServer(Application application, long serverStartTime, AnyValue serconf, ResourceFactory resourceFactory) {
        super(application, serverStartTime, netprotocol(serconf), resourceFactory, new MqttPrepareServlet());
    }

    private static String netprotocol(AnyValue serconf) {
        if (serconf == null) return "TCP";
        String protocol = serconf.getValue("protocol", "").toUpperCase();
        if (protocol.endsWith(".UDP")) return "UDP";
        return "TCP";
    }

    @Override
    public void init(AnyValue config) throws Exception {
        super.init(config);
    }

    public List<MqttServlet> getMqttServlets() {
        return this.prepare.getServlets();
    }

    public List<MqttFilter> getMqttFilters() {
        return this.prepare.getFilters();
    }

    /**
     * 删除MqttFilter
     *
     * @param <T>         泛型
     * @param filterClass MqttFilter类
     *
     * @return MqttFilter
     */
    public <T extends MqttFilter> T removeMqttFilter(Class<T> filterClass) {
        return (T) this.prepare.removeFilter(filterClass);
    }

    /**
     * 添加MqttFilter
     *
     * @param filter MqttFilter
     * @param conf   AnyValue
     *
     * @return MqttServer
     */
    public MqttServer addMqttFilter(MqttFilter filter, AnyValue conf) {
        this.prepare.addFilter(filter, conf);
        return this;
    }

    /**
     * 删除MqttServlet
     *
     * @param sncpService Service
     *
     * @return MqttServlet
     */
    public MqttServlet removeMqttServlet(Service sncpService) {
        return null; //((MqttPrepareServlet) this.prepare).removeMqttServlet(sncpService);
    }

    public MqttDynServlet addMqttServlet(Service sncpService) {
        
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected MqttContext createContext() {
        this.bufferCapacity = Math.max(this.bufferCapacity, 8 * 1024);

        final MqttContextConfig contextConfig = new MqttContextConfig();
        initContextConfig(contextConfig);

        return new MqttContext(contextConfig);
    }

    @Override
    protected ObjectPool<ByteBuffer> createBufferPool(LongAdder createCounter, LongAdder cycleCounter, int bufferPoolSize) {
        if (createCounter == null) createCounter = new LongAdder();
        if (cycleCounter == null) cycleCounter = new LongAdder();
        final int rcapacity = this.bufferCapacity;
        ObjectPool<ByteBuffer> bufferPool = ObjectPool.createSafePool(createCounter, cycleCounter, bufferPoolSize,
            (Object... params) -> ByteBuffer.allocateDirect(rcapacity), null, (e) -> {
                if (e == null || e.isReadOnly() || e.capacity() != rcapacity) return false;
                e.clear();
                return true;
            });
        return bufferPool;
    }

    @Override
    protected ObjectPool<Response> createResponsePool(LongAdder createCounter, LongAdder cycleCounter, int responsePoolSize) {
        Creator<Response> creator = (Object... params) -> new MqttResponse(this.context, new MqttRequest(this.context));
        ObjectPool<Response> pool = ObjectPool.createSafePool(createCounter, cycleCounter, responsePoolSize, creator, (x) -> ((MqttResponse) x).prepare(), (x) -> ((MqttResponse) x).recycle());
        return pool;
    }
}