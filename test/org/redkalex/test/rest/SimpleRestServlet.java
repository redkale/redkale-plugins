package org.redkalex.test.rest;

import java.io.IOException;


import org.redkale.net.http.HttpRequest;
import org.redkale.net.http.HttpResponse;
import org.redkalex.rest.RestHttpServlet;

public class SimpleRestServlet extends RestHttpServlet<UserInfo> {

    @Override
    protected UserInfo currentUser(HttpRequest req) throws IOException {

        return null;
    }

    @Override
    public boolean authenticate(int module, int actionid, HttpRequest request, HttpResponse response) throws IOException {

        return true;
    }
    
    public static void main(String[] args) throws Throwable {
        System.out.println(getSuperServletType( SimpleRestServlet.class));
    }
    
    private static java.lang.reflect.Type getSuperServletType(Class servletClass) {
        java.lang.reflect.Type type = servletClass.getGenericSuperclass();
        if(type instanceof Class) return getSuperServletType((Class)type);
        if(type instanceof  java.lang.reflect.ParameterizedType) {
            java.lang.reflect.ParameterizedType pt = (java.lang.reflect.ParameterizedType)type;
            if(pt.getRawType() == RestHttpServlet.class) return pt.getActualTypeArguments()[0];
        }
        return null;
    }
}
