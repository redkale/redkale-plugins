package org.redkalex.test.rest;

import java.io.IOException;
import java.util.List;
import javax.annotation.Resource;
import org.redkale.net.http.*;
import org.redkale.service.RetResult;
import org.redkale.source.Flipper;
import org.redkale.util.Sheet;

@WebServlet(value = {"/hello/*"}, moduleid = 201, repair = true)
public class _DynHelloRestServlet extends SimpleRestServlet {

    @Resource
    private HelloService _service;

    @AuthIgnore
    @WebAction(url = "/hello/query", actionid = 2001)
    public void query(HttpRequest req, HttpResponse resp) throws IOException {
        HelloBean bean = req.getJsonParameter(HelloBean.class, "bean");
        Flipper flipper = findFlipper(req);
        Sheet<HelloEntity> result = _service.queryHello(bean, flipper);
        resp.finishJson(result);
    }

    @AuthIgnore
    @WebAction(url = "/hello/list", actionid = 2001)
    public void list(HttpRequest req, HttpResponse resp) throws IOException {
        HelloBean bean = req.getJsonParameter(HelloBean.class, "bean");
        List<HelloEntity> result = _service.queryHello(bean);
        resp.finishJson(result);
    }

    @AuthIgnore
    @WebAction(url = "/hello/find/", actionid = 2001)
    public void find(HttpRequest req, HttpResponse resp) throws IOException {
        int id = Integer.parseInt(req.getRequstURILastPath());
        HelloEntity result = _service.findHello(id);
        resp.finishJson(result);
    }

    @AuthIgnore
    @WebAction(url = "/hello/findname/", actionid = 2001)
    public void findname(HttpRequest req, HttpResponse resp) throws IOException {
        int id = Integer.parseInt(req.getRequstURILastPath());
        String result = _service.findHelloName(id);
        resp.finish(result);
    }

    @AuthIgnore
    @WebAction(url = "/hello/findtime/", actionid = 2001)
    public void findtime(HttpRequest req, HttpResponse resp) throws IOException {
        int id = Integer.parseInt(req.getRequstURILastPath());
        long result = _service.findHelloTime(id);
        resp.finish(String.valueOf(result));
    }

    @AuthIgnore
    @WebAction(url = "/hello/jsfind/", actionid = 2001)
    public void jsfind(HttpRequest req, HttpResponse resp) throws IOException {
        int id = Integer.parseInt(req.getRequstURILastPath());
        HelloEntity result = _service.findHello(id);
        sendJsResult(resp, "varhello", result);
    }

    @WebAction(url = "/hello/create", actionid = 2002)
    public void create(HttpRequest req, HttpResponse resp) throws IOException {
        HelloEntity bean = req.getJsonParameter(HelloEntity.class, "bean");
        RetResult<HelloEntity> result = _service.createHello(bean);
        sendRetResult(resp, result);
    }

    @WebAction(url = "/hello/update", actionid = 2003)
    public void update(HttpRequest req, HttpResponse resp) throws IOException {
        HelloEntity bean = req.getJsonParameter(HelloEntity.class, "bean");
        _service.updateHello(bean);
        resp.finishJson(RetResult.SUCCESS);
    }

    @WebAction(url = "/hello/partupdate", actionid = 2003)
    public void partupdate(HttpRequest req, HttpResponse resp) throws IOException {
        HelloEntity bean = req.getJsonParameter(HelloEntity.class, "bean");
        String[] columns = req.getJsonParameter(String[].class, "columns");
        _service.updateHello(currentUser(req), bean, columns);
        resp.finishJson(RetResult.SUCCESS);
    }

    @WebAction(url = "/hello/delete/", actionid = 2004)
    public void delete(HttpRequest req, HttpResponse resp) throws IOException {
        int id = Integer.parseInt(req.getRequstURILastPath());
        _service.deleteHello(id);
        resp.finishJson(RetResult.SUCCESS);
    }

    @WebAction(url = "/hello/remove", actionid = 2004)
    public void remove(HttpRequest req, HttpResponse resp) throws IOException {
        HelloEntity bean = req.getJsonParameter(HelloEntity.class, "bean");
        _service.deleteHello(bean);
        resp.finishJson(RetResult.SUCCESS);
    }
}
