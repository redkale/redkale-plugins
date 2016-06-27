package org.redkalex.test.rest;

import org.redkale.convert.json.JsonFactory;

/**
 * 当前用户对象
 *
 * @author zhangjx
 */
public class UserInfo {

    private int userid;

    private String username = "";

    public int getUserid() {
        return userid;
    }

    public void setUserid(int userid) {
        this.userid = userid;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String toString() {
        return JsonFactory.root().getConvert().convertTo(this);
    }
}
