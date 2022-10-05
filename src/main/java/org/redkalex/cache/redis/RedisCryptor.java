/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.redkalex.cache.redis;

import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public interface RedisCryptor {

    public String encrypt(String key, String value);

    public String decrypt(String key, String value);

    public void init(AnyValue conf);

    public void destroy(AnyValue conf);

}
