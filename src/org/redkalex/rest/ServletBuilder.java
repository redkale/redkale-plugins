/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.rest;

import org.redkale.net.sncp.ServiceWrapper;

/**
 *
 * 详情见: http://redkale.org
 * @author zhangjx
 */
final class ServletBuilder {

    private ServletBuilder() {
    }

    //待实现
    public static <T extends RestHttpServlet> T createRestServlet(final Class<T> servletClass, final ServiceWrapper wrapper) {
        return null;
    }
}
