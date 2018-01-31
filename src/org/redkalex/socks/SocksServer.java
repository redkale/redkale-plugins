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
        if (this.readTimeoutSecond < 1) this.readTimeoutSecond = 6;
        if (this.writeTimeoutSecond < 1) this.writeTimeoutSecond = 6;
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
        SocksContext localcontext = new SocksContext(this.serverStartTime, this.logger, executor, this.sslContext, rcapacity, bufferPool, responsePool,
            this.maxbody, this.charset, this.address, this.resourceFactory, this.prepare, this.readTimeoutSecond, this.writeTimeoutSecond);
        responsePool.setCreator((Object... params) -> new SocksResponse(localcontext, new SocksRequest(localcontext)));
        return localcontext;
    }
}
