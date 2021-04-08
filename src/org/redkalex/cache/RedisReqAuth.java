/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache;

import java.nio.charset.StandardCharsets;

/**
 *
 * @author zhangjx
 */
public class RedisReqAuth extends RedisClientRequest {

    public RedisReqAuth(String password) {
        super("AUTH", null, null, false, null, password.getBytes(StandardCharsets.UTF_8));
    }
}
