/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.socks;

import org.redkale.net.Servlet;

/**
 *
 * 详情见: https://redkale.org
 * @author zhangjx
 */
public abstract class SocksServlet extends Servlet<SocksContext, SocksRequest, SocksResponse> {

    @Override
    public final boolean equals(Object obj) {
        return obj != null && obj.getClass() == this.getClass();
    }

    @Override
    public final int hashCode() {
        return this.getClass().hashCode();
    }
}
