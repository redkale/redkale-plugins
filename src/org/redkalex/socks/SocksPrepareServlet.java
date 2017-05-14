/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.socks;

import org.redkale.util.AnyValue;
import org.redkale.net.PrepareServlet;
import java.io.*;

/**
 *
 * 详情见: https://redkale.org
 *
 * @author zhangjx
 */
public final class SocksPrepareServlet extends PrepareServlet<Serializable, SocksContext, SocksRequest, SocksResponse, SocksServlet> {

    private SocksServlet socks5Servlet = new Socks5Servlet();

    private final SocksHttpxServlet httpxServlet = new SocksHttpxServlet();

    public SocksPrepareServlet() {
    }

    @Override
    public void init(SocksContext context, AnyValue config) {
        super.init(context, config); //必须要执行
        if (socks5Servlet != null) socks5Servlet.init(context, getServletConf(socks5Servlet) == null ? config : getServletConf(socks5Servlet));
    }

    // 
    @Override
    public void execute(SocksRequest request, SocksResponse response) throws IOException {
        if (request.isHttp()) {
            httpxServlet.execute(request, response);
        } else {
            socks5Servlet.execute(request, response);
        }
    }

    @Override
    public void addServlet(SocksServlet servlet, Object attachment, AnyValue conf, Serializable... mappings) {
        setServletConf(servlet, conf);
        if (servlet != null) this.socks5Servlet = (SocksServlet) servlet;
    }

}
