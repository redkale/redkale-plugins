/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.socks;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.redkale.net.*;
import org.redkale.net.http.*;
import org.redkale.util.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public class SocksRequest extends Request<SocksContext> {

    private final HttpxRequest httpRequest;

    private boolean http;

    protected SocksRequest(SocksContext context, ObjectPool<ByteBuffer> bufferPool) {
        super(context, bufferPool);
        this.httpRequest = new HttpxRequest(context, null);
    }

    @Override
    protected int readHeader(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (buffer.get(0) > 0x05 && remaining > 4) {
            this.http = true;
            return httpRequest.readHeader(buffer);
        }
        this.http = false;
        // 05 01 00 共3字节，这种是要求匿名代理
        // 05 01 02 共3字节，这种是要求以用户名密码方式验证代理
        // 05 02 00 02 共4字节，这种是要求以匿名或者用户名密码方式代理        
        if (remaining == 4) {
            if (buffer.get() != 0x05) return -1;
            if (buffer.get() != 0x02) return -1;
            if (buffer.get() != 0x00) return -1;
            if (buffer.get() != 0x02) return -1;
        } else { //3  05 01 02 共3字节，这种是要求以用户名密码方式验证代理, 暂时不支持
            if (buffer.get() != 0x05) return -1;
            if (buffer.get() != 0x01) return -1;
            if (buffer.get() != 0x00) return -1;
        }
        return 0;
    }

    @Override
    protected int readBody(ByteBuffer buffer) {
        return http ? httpRequest.readBody(buffer) : buffer.remaining();
    }

    @Override
    protected void prepare() {
        httpRequest.prepare();
    }

    @Override
    protected void recycle() {
        this.http = false;
        httpRequest.setChannel(null);
        httpRequest.recycle();
        super.recycle();
    }

    public boolean isHttp() {
        return http;
    }

    public void setHttp(boolean http) {
        this.http = http;
    }

    HttpxRequest getHttpxRequest() {
        return httpRequest;
    }

}

class HttpxRequest extends HttpRequest {

    public HttpxRequest(HttpContext context, ObjectPool<ByteBuffer> bufferPool) {
        super(context, bufferPool);
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
        super.skipBodyParse();
        return super.readHeader(buffer);
    }

    @Override
    protected int readBody(ByteBuffer buffer) {
        super.skipBodyParse();
        return super.readBody(buffer);
    }

    protected ByteArray getDirectBody() {
        return super.getDirectBody();
    }

    @Override
    protected void prepare() {
        super.prepare();
    }

    @Override
    protected void recycle() {
        super.recycle();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    protected AsyncConnection getChannel() {
        return super.getChannel();
    }

    protected void setChannel(AsyncConnection channel) {
        super.channel = channel;
    }
}
