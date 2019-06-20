/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.socks;

import java.nio.ByteBuffer;
import org.redkale.net.AsyncConnection;
import org.redkale.util.ObjectPool;
import org.redkale.util.Creator;
import org.redkale.net.Response;
import java.util.concurrent.atomic.*;
import org.redkale.net.http.*;
import org.redkale.net.http.HttpResponse.HttpResponseConfig;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class SocksResponse extends Response<SocksContext, SocksRequest> {

    private final HttpxResponse httpResponse;

    protected SocksResponse(SocksContext context, SocksRequest request, ObjectPool<Response> responsePool) {
        super(context, request, responsePool);
        this.httpResponse = new HttpxResponse(context, request.getHttpxRequest(), responsePool, new HttpResponseConfig(), this);
    }

    public static ObjectPool<Response> createPool(AtomicLong creatCounter, AtomicLong cycleCounter, int max, Creator<Response> creator) {
        return new ObjectPool<>(creatCounter, cycleCounter, max, creator, (x) -> ((SocksResponse) x).prepare(), (x) -> ((SocksResponse) x).recycle());
    }

    @Override
    public AsyncConnection removeChannel() {
        this.httpResponse.setChannel(null);
        return super.removeChannel();
    }

    public AsyncConnection getChannel() {
        return super.channel;
    }

    HttpxResponse getHttpxResponse() {
        return httpResponse;
    }

    @Override
    protected boolean recycle() {
        this.httpResponse.setChannel(null);
        this.httpResponse.recycle();
        return super.recycle();
    }

    protected ObjectPool<ByteBuffer> getBufferPool() {
        return this.bufferPool;
    }
}

class HttpxResponse extends HttpResponse {

    private final SocksResponse socksResponse;

    public HttpxResponse(HttpContext context, HttpRequest request, ObjectPool<Response> responsePool, HttpResponseConfig config, SocksResponse socksResponse) {
        super(context, request, responsePool, config);
        this.socksResponse = socksResponse;
    }

    @Override
    public void finish(boolean kill) {
        socksResponse.finish(kill);
    }

    protected void setChannel(AsyncConnection channel) {
        super.channel = channel;
    }

    @Override
    protected AsyncConnection getChannel() {
        return super.channel;
    }

    @Override
    protected void offerBuffer(ByteBuffer... buffers) {
        super.offerBuffer(buffers);
    }

    protected ObjectPool<ByteBuffer> getBufferPool() {
        return this.bufferPool;
    }

    @Override
    protected boolean recycle() {
        return super.recycle();
    }
}
