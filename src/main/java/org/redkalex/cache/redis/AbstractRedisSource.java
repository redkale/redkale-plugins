/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.redkalex.cache.redis;

import javax.annotation.Resource;
import org.redkale.convert.json.JsonConvert;
import org.redkale.source.AbstractCacheSource;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 *
 * @since 2.8.0
 */
public abstract class AbstractRedisSource extends AbstractCacheSource {

    public static final String CACHE_SOURCE_CRYPTOR = "cryptor";

    @Resource
    protected ResourceFactory resourceFactory;

    @Resource
    protected JsonConvert defaultConvert;

    @Resource(name = "$_convert")
    protected JsonConvert convert;

    protected int db;

    protected RedisCryptor cryptor;

    @Override
    public void init(AnyValue conf) {
        super.init(conf);
        if (this.convert == null) this.convert = this.defaultConvert;
        if (conf != null) {
            String cryptStr = conf.getValue(CACHE_SOURCE_CRYPTOR, "").trim();
            if (!cryptStr.isEmpty()) {
                try {
                    Class<RedisCryptor> cryptClass = (Class) getClass().getClassLoader().loadClass(cryptStr);
                    this.cryptor = cryptClass.getConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (cryptor != null) {
            if (resourceFactory != null) {
                resourceFactory.inject(cryptor);
            }
            cryptor.init(conf);
        }
    }

    @Override
    public void destroy(AnyValue conf) {
        super.destroy(conf);
        if (cryptor != null) {
            cryptor.destroy(conf);
        }
    }

    @Override
    public void close() throws Exception {  //在 Application 关闭时调用
        destroy(null);
    }

    @Override
    public String resourceName() {
        Resource res = this.getClass().getAnnotation(Resource.class);
        return res == null ? "" : res.name();
    }

}
