package org.redtest.rest.base;

import java.io.IOException;
import javax.annotation.Resource;

import org.redkale.net.http.HttpRequest;
import org.redkale.net.http.HttpResponse;
import org.redkale.service.RetResult;
import org.redkalex.rest.RestHttpServlet;
import org.redtest.rest.user.UserService;

public class SimpleRestServlet extends RestHttpServlet<UserInfo> {

    protected static final RetResult RET_UNLOGIN = RetCodes.retResult(RetCodes.RET_USER_UNLOGIN);

    protected static final RetResult RET_AUTHILLEGAL = RetCodes.retResult(RetCodes.RET_USER_AUTH_ILLEGAL);

    @Resource
    private UserService userService;

    //获取当前用户信息
    @Override
    protected UserInfo currentUser(HttpRequest req) throws IOException {
        String sessionid = req.getSessionid(false);
        if (sessionid == null || sessionid.isEmpty()) return null;
        return userService.current(sessionid);
    }

    //普通鉴权
    @Override
    public boolean authenticate(int module, int actionid, HttpRequest request, HttpResponse response) throws IOException {
        UserInfo info = currentUser(request);
        if (info == null) {
            response.finishJson(RET_UNLOGIN);
            return false;
        } else if (!info.checkAuth(module, actionid)) {
            response.finishJson(RET_AUTHILLEGAL);
            return false;
        }
        return true;
    }

}
