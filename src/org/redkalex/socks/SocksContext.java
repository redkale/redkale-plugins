/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.socks;

import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.function.*;
import org.redkale.net.http.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class SocksContext extends HttpContext {

    protected final AsynchronousChannelGroup group;

    public SocksContext(HttpContextConfig config) {
        super(config);
        AsynchronousChannelGroup g = null;
        try {
            g = AsynchronousChannelGroup.withThreadPool(executor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.group = g;
    }

    @Override
    protected Consumer<ByteBuffer> getBufferConsumer() {
        return super.getBufferConsumer();
    }

    @Override
    protected void offerBuffer(ByteBuffer buffer) {
        super.offerBuffer(buffer);
    }

    @Override
    protected void offerBuffer(ByteBuffer... buffers) {
        super.offerBuffer(buffers);
    }

    protected AsynchronousChannelGroup getAsynchronousChannelGroup() {
        return group;
    }

}
