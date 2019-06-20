/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.socks;

import java.io.*;
import org.redkale.util.AnyValue;
import org.redkale.net.Server;
import org.redkale.util.ObjectPool;
import org.redkale.net.Response;
import java.nio.*;
import java.util.concurrent.atomic.*;
import org.redkale.net.http.HttpContext;
import org.redkale.util.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public final class SocksServer extends Server<Serializable, SocksContext, SocksRequest, SocksResponse, SocksServlet> {

    public SocksServer() {
        this(System.currentTimeMillis(), ResourceFactory.root());
    }

    public SocksServer(long serverStartTime, ResourceFactory resourceFactory) {
        super(serverStartTime, "TCP", resourceFactory, new SocksPrepareServlet());
    }

    @Override
    public void init(AnyValue config) throws Exception {
        super.init(config);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected SocksContext createContext() {
        if (this.readTimeoutSeconds < 1) this.readTimeoutSeconds = 6;
        if (this.writeTimeoutSeconds < 1) this.writeTimeoutSeconds = 6;
        AtomicLong createBufferCounter = new AtomicLong();
        AtomicLong cycleBufferCounter = new AtomicLong();
        int rcapacity = Math.max(this.bufferCapacity, 8 * 1024);
        ObjectPool<ByteBuffer> bufferPool = new ObjectPool<>(createBufferCounter, cycleBufferCounter, this.bufferPoolSize,
            (Object... params) -> ByteBuffer.allocateDirect(rcapacity), null, (e) -> {
                if (e == null || e.isReadOnly() || e.capacity() != rcapacity) return false;
                e.clear();
                return true;
            });
        AtomicLong createResponseCounter = new AtomicLong();
        AtomicLong cycleResponseCounter = new AtomicLong();
        ObjectPool<Response> responsePool = SocksResponse.createPool(createResponseCounter, cycleResponseCounter, this.responsePoolSize, null);

        final HttpContext.HttpContextConfig contextConfig = new HttpContext.HttpContextConfig();
        contextConfig.serverStartTime = this.serverStartTime;
        contextConfig.logger = this.logger;
        contextConfig.executor = this.executor;
        contextConfig.sslContext = this.sslContext;
        contextConfig.bufferCapacity = rcapacity;
        contextConfig.maxbody = this.maxbody;
        contextConfig.charset = this.charset;
        contextConfig.address = this.address;
        contextConfig.prepare = this.prepare;
        contextConfig.resourceFactory = this.resourceFactory;
        contextConfig.aliveTimeoutSeconds = this.aliveTimeoutSeconds;
        contextConfig.readTimeoutSeconds = this.readTimeoutSeconds;
        contextConfig.writeTimeoutSeconds = this.writeTimeoutSeconds;

        return new SocksContext(contextConfig);
    }

    @Override
    protected ObjectPool<ByteBuffer> createBufferPool(AtomicLong createCounter, AtomicLong cycleCounter, int bufferPoolSize) {
        AtomicLong createBufferCounter = new AtomicLong();
        AtomicLong cycleBufferCounter = new AtomicLong();
        final int rcapacity = this.bufferCapacity;
        ObjectPool<ByteBuffer> bufferPool = new ObjectPool<>(createBufferCounter, cycleBufferCounter, bufferPoolSize,
            (Object... params) -> ByteBuffer.allocateDirect(rcapacity), null, (e) -> {
                if (e == null || e.isReadOnly() || e.capacity() != rcapacity) return false;
                e.clear();
                return true;
            });
        return bufferPool;
    }

    @Override
    protected ObjectPool<Response> createResponsePool(AtomicLong createCounter, AtomicLong cycleCounter, int responsePoolSize) {
        return SocksResponse.createPool(createCounter, cycleCounter, responsePoolSize, null);
    }

    @Override
    protected Creator<Response> createResponseCreator(ObjectPool<ByteBuffer> bufferPool, ObjectPool<Response> responsePool) {
        return (Object... params) -> new SocksResponse(this.context, new SocksRequest(this.context, bufferPool), responsePool);
    }
}
