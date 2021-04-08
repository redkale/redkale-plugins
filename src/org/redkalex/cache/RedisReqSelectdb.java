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
public class RedisReqSelectdb extends RedisClientRequest {

    public RedisReqSelectdb(String db) {
        super("SELECT", null, null, false, null, db.getBytes(StandardCharsets.UTF_8));
    }
}
