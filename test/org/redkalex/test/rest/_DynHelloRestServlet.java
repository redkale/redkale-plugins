package org.redkalex.test.rest;

import java.io.IOException;
import javax.annotation.Resource;
import org.redkale.net.http.*;
import org.redkale.service.RetResult;

@WebServlet(value = {"/hello/*"}, moduleid = 201, repair = true)
public class _DynHelloRestServlet extends SimpleRestServlet {

    @Resource
    private HelloService _service;

    @AuthIgnore
    @WebAction(url = "/hello/query")
    public void query(HttpRequest req, HttpResponse resp) throws IOException {
        resp.finishJson(_service.queryHello(req.getJsonParameter(HelloBean.class, "bean"), findFlipper(req)));
    }

    @AuthIgnore
    @WebAction(url = "/hello/list")
    public void list(HttpRequest req, HttpResponse resp) throws IOException {
        resp.finishJson(_service.queryHello(req.getJsonParameter(HelloBean.class, "bean")));
    }

    @AuthIgnore
    @WebAction(url = "/hello/find/")
    public void find(HttpRequest req, HttpResponse resp) throws IOException {
        resp.finishJson(_service.findHello(Integer.parseInt(req.getRequstURILastPath())));
    }

    @AuthIgnore
    @WebAction(url = "/hello/findname/")
    public void findname(HttpRequest req, HttpResponse resp) throws IOException {
        resp.finish(_service.findHelloName(Integer.parseInt(req.getRequstURILastPath())));
    }

    @AuthIgnore
    @WebAction(url = "/hello/findtime/")
    public void findtime(HttpRequest req, HttpResponse resp) throws IOException {
        resp.finish(String.valueOf(_service.findHelloTime(Integer.parseInt(req.getRequstURILastPath()))));
    }

    @AuthIgnore
    @WebAction(url = "/hello/jsfind/")
    public void jsfind(HttpRequest req, HttpResponse resp) throws IOException {
        sendJsResult(resp, "varhello", _service.findHello(Integer.parseInt(req.getRequstURILastPath())));
    }

    @WebAction(url = "/hello/create")
    public void create(HttpRequest req, HttpResponse resp) throws IOException {
        sendRetResult(resp, _service.createHello(req.getJsonParameter(HelloEntity.class, "bean")));
    }

    @WebAction(url = "/hello/update")
    public void update(HttpRequest req, HttpResponse resp) throws IOException {
        _service.updateHello(req.getJsonParameter(HelloEntity.class, "bean"));
        resp.finishJson(RetResult.SUCCESS);
    }

    @WebAction(url = "/hello/partupdate")
    public void partupdate(HttpRequest req, HttpResponse resp) throws IOException {
        HelloEntity bean = req.getJsonParameter(HelloEntity.class, "bean");
        String[] columns = req.getJsonParameter(String[].class, "columns");
        _service.updateHello(currentUser(req), bean, columns);
        resp.finishJson(RetResult.SUCCESS);
    }

    @WebAction(url = "/hello/delete/")
    public void delete(HttpRequest req, HttpResponse resp) throws IOException {
        _service.deleteHello(Integer.parseInt(req.getRequstURILastPath()));
        resp.finishJson(RetResult.SUCCESS);
    }

    @WebAction(url = "/hello/remove")
    public void remove(HttpRequest req, HttpResponse resp) throws IOException {
        _service.deleteHello(req.getJsonParameter(HelloEntity.class, "bean"));
        resp.finishJson(RetResult.SUCCESS);
    }
}
