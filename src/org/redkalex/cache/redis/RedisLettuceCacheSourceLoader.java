/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import org.redkale.source.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class RedisLettuceCacheSourceLoader implements CacheSourceLoader {

    @Override
    public boolean match(AnyValue config) {
        try {
            RedisLettuceCacheSource source = RedisLettuceCacheSource.class.getConstructor().newInstance();
            return source.match(config);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public Class<? extends CacheSource> sourceClass() {
        return RedisLettuceCacheSource.class;
    }

}
