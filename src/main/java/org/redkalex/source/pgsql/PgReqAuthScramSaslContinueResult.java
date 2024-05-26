/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.source.pgsql;

import org.redkale.convert.json.JsonConvert;

/** @author zhangjx */
public class PgReqAuthScramSaslContinueResult {

    public String saslmsg; // r=hu~1A]pOoftNMKg<~YWKp`1^F2avWJEdla6IQr3BiNBORfiy,s=1uyIEWcUJVLPlhSivlUMZg==,i=4096

    protected String username;

    public String password;

    public String clientNonce; // hu~1A]pOoftNMKg<~YWKp`1^

    public String serverNonce; // F2avWJEdla6IQr3BiNBORfiy

    public String salt; // 1uyIEWcUJVLPlhSivlUMZg==

    public int iteration; // 4096

    public PgReqAuthScramSaslContinueResult(PgReqAuthScramPassword req, String saslmsg) {
        String[] msgs = saslmsg.split(",");
        this.saslmsg = saslmsg;
        this.username = req.username;
        this.password = req.password;
        this.clientNonce = req.clientNonce;
        if (!msgs[0].startsWith("r=" + clientNonce)) {
            throw new IllegalArgumentException("parsed serverNonce does not start with client serverNonce");
        }
        if (!msgs[1].startsWith("s=")) {
            throw new IllegalArgumentException("not found salt value");
        }
        if (!msgs[2].startsWith("i=")) {
            throw new IllegalArgumentException("not found iteration value");
        }
        this.serverNonce = msgs[0].substring(clientNonce.length() + 2);
        this.salt = msgs[1].substring(2);
        this.iteration = Integer.parseInt(msgs[2].substring(2));
    }

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }
}
