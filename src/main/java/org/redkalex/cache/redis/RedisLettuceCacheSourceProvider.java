/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import javax.annotation.Priority;
import org.redkale.source.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
@Priority(-100)
public class RedisLettuceCacheSourceProvider implements CacheSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            Object.class.isAssignableFrom(io.lettuce.core.support.BoundedPoolConfig.class); //试图加载Lettuce相关类
            RedisLettuceCacheSource source = RedisLettuceCacheSource.class.getConstructor().newInstance();
            return source.acceptsConf(config);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public Class<? extends CacheSource> sourceClass() {
        return RedisLettuceCacheSource.class;
    }

}
