/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkale.net.socks;

import org.redkale.util.AnyValue;
import org.redkale.net.PrepareServlet;
import java.io.*;
import org.redkale.net.*;

/**
 *
 * @see http://www.redkale.org
 * @author zhangjx
 */
public final class SocksPrepareServlet extends PrepareServlet<Serializable, SocksContext, SocksRequest, SocksResponse> {

    private SocksServlet socksServlet = new SocksConnectServlet();

    private SocksProxyServlet proxyServlet = new SocksProxyServlet();

    public SocksPrepareServlet() {
    }

    @Override
    public void init(SocksContext context, AnyValue config) {
        if (socksServlet != null) socksServlet.init(context, getServletConf(socksServlet) == null ? config : getServletConf(socksServlet));
    }

    // 
    @Override
    public void execute(SocksRequest request, SocksResponse response) throws IOException {
        if (request.isHttp()) {
            proxyServlet.execute(request, response);
        } else {
            socksServlet.execute(request, response);
        }
    }

    @Override
    public <S extends Servlet<SocksContext, SocksRequest, SocksResponse>> void addServlet(S servlet, Object attachment, AnyValue conf, Serializable... mappings) {
        setServletConf(servlet, conf);
        if (servlet != null) this.socksServlet = (SocksServlet) servlet;
    }

}
