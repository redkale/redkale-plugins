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
