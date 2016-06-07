/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkale.plugins.socks;

import java.net.*;
import org.redkale.net.AsyncConnection;
import org.redkale.util.ObjectPool;
import org.redkale.util.Creator;
import org.redkale.net.Response;
import java.util.concurrent.atomic.*;
import org.redkale.net.http.*;

/**
 *
 * @see http://www.redkale.org
 * @author zhangjx
 */
public class SocksResponse extends Response<SocksContext, SocksRequest> {

    private final HttpxResponse httpResponse;

    protected SocksResponse(SocksContext context, SocksRequest request) {
        super(context, request);
        this.httpResponse = new HttpxResponse(context, request.getHttpxRequest(), null, null, null, this);
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
}

class HttpxResponse extends HttpResponse {

    private final SocksResponse socksResponse;

    public HttpxResponse(HttpContext context, HttpRequest request, String[][] defaultAddHeaders, String[][] defaultSetHeaders, HttpCookie defcookie, SocksResponse socksResponse) {
        super(context, request, defaultAddHeaders, defaultSetHeaders, defcookie);
        this.socksResponse = socksResponse;
    }

    @Override
    public void finish(boolean kill) {
        socksResponse.finish(kill);
    }

    protected void setChannel(AsyncConnection channel) {
        super.channel = channel;
    }

    protected AsyncConnection getChannel() {
        return super.channel;
    }

    @Override
    protected boolean recycle() {
        return super.recycle();
    }
}
