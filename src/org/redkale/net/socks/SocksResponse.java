/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkale.net.socks;

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

    private final ProxyResponse httpResponse;

    protected SocksResponse(SocksContext context, SocksRequest request) {
        super(context, request);
        this.httpResponse = new ProxyResponse(context, request.getProxyRequest(), null, null, null);
    }

    public static ObjectPool<Response> createPool(AtomicLong creatCounter, AtomicLong cycleCounter, int max, Creator<Response> creator) {
        return new ObjectPool<>(creatCounter, cycleCounter, max, creator, (x) -> ((SocksResponse) x).prepare(), (x) -> ((SocksResponse) x).recycle());
    }

    @Override
    public AsyncConnection removeChannel() {
        return super.removeChannel();
    }

    ProxyResponse getProxyResponse() {
        return httpResponse;
    }
}

class ProxyResponse extends HttpResponse {

    public ProxyResponse(HttpContext context, HttpRequest request, String[][] defaultAddHeaders, String[][] defaultSetHeaders, HttpCookie defcookie) {
        super(context, request, defaultAddHeaders, defaultSetHeaders, defcookie);
    }

}
