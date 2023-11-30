/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public interface RedisCryptor {

    /**
     * 初始化
     *
     * @param conf 配置
     */
    public void init(AnyValue conf);

    /**
     * 加密, 无需加密的key对应的值需要直接返回value
     *
     * @param key   key
     * @param value 明文
     *
     * @return 密文
     */
    public String encrypt(String key, String value);

    /**
     * 解密, 无需解密的key对应的值需要直接返回value
     *
     * @param key   key
     * @param value 密文
     *
     * @return 明文
     */
    public String decrypt(String key, String value);

    /**
     * 销毁
     *
     * @param conf 配置
     */
    public void destroy(AnyValue conf);

}
