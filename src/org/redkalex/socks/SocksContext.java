/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.socks;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.*;
import java.util.concurrent.*;
import java.util.logging.*;
import javax.net.ssl.SSLContext;
import org.redkale.net.*;
import org.redkale.net.http.*;
import org.redkale.util.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class SocksContext extends HttpContext {

    protected final AsynchronousChannelGroup group;

    public SocksContext(long serverStartTime, Logger logger, ThreadPoolExecutor executor, SSLContext sslContext,
        int bufferCapacity, ObjectPool<ByteBuffer> bufferPool, ObjectPool<Response> responsePool,
        int maxbody, Charset charset, InetSocketAddress address, ResourceFactory resourceFactory,
        PrepareServlet prepare, int aliveTimeoutSecond, int readTimeoutSecond, int writeTimeoutSecond) {
        super(serverStartTime, logger, executor, sslContext, bufferCapacity, bufferPool, responsePool,
            maxbody, charset, address, resourceFactory, prepare, aliveTimeoutSecond, readTimeoutSecond, writeTimeoutSecond);
        AsynchronousChannelGroup g = null;
        try {
            g = AsynchronousChannelGroup.withThreadPool(executor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.group = g;
    }

    protected AsynchronousChannelGroup getAsynchronousChannelGroup() {
        return group;
    }

}
