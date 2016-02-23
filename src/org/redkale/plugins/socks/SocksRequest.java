/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkale.plugins.socks;

import org.redkale.net.AsyncConnection;
import org.redkale.net.http.HttpRequest;
import java.net.*;
import java.nio.*;
import org.redkale.net.*;
import org.redkale.net.http.*;

/**
 *
 * @see http://www.redkale.org
 * @author zhangjx
 */
public class SocksRequest extends Request<SocksContext> {

    private final ProxyRequest httpRequest;

    private boolean http;

    protected SocksRequest(SocksContext context) {
        super(context);
        this.httpRequest = new ProxyRequest(context, null);
    }

    @Override
    protected int readHeader(ByteBuffer buffer) {
        if (buffer.get(0) > 0x05 && buffer.remaining() > 3) {
            this.http = true;
            return httpRequest.readHeader(buffer);
        }
        this.http = false;
        if (buffer.get() != 0x05) return -1;
        if (buffer.get() != 0x01) return -1;
        if (buffer.get() != 0x00) return -1;
        return 0;
    }

    @Override
    protected int readBody(ByteBuffer buffer) {
        return buffer.remaining();
    }

    @Override
    protected void prepare() {
        httpRequest.prepare();
    }

    @Override
    protected void recycle() {
        this.http = false;
        super.recycle();
    }

    public boolean isHttp() {
        return http;
    }

    public void setHttp(boolean http) {
        this.http = http;
    }

    ProxyRequest getProxyRequest() {
        return httpRequest;
    }

}

class ProxyRequest extends HttpRequest {

    public ProxyRequest(HttpContext context, String remoteAddrHeader) {
        super(context, remoteAddrHeader);
    }

    protected InetSocketAddress getURLSocketAddress() {
        return parseSocketAddress(super.getRequestURI());
    }

    protected InetSocketAddress getHostSocketAddress() {
        return parseSocketAddress(getHost());
    }

    private InetSocketAddress parseSocketAddress(String host) {
        if (host == null || host.isEmpty()) return null;
        int pos = host.indexOf(':');
        String hostname = pos < 0 ? host : host.substring(0, pos);
        int port = pos < 0 ? 80 : Integer.parseInt(host.substring(pos + 1));
        return new InetSocketAddress(hostname, port);
    }

    @Override
    protected int readHeader(final ByteBuffer buffer) {
        return super.readHeader(buffer);
    }

    @Override
    protected void prepare() {
        super.prepare();
    }

    @Override
    protected AsyncConnection getChannel() {
        return super.getChannel();
    }
}
