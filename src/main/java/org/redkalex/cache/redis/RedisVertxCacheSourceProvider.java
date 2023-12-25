/*
 */
package org.redkalex.cache.redis;

import org.redkale.annotation.Priority;
import org.redkale.source.*;
import org.redkale.source.spi.CacheSourceProvider;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
@Priority(-300)
public class RedisVertxCacheSourceProvider implements CacheSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            Object.class.isAssignableFrom(io.vertx.redis.client.RedisOptions.class); //试图加载vertx-redis相关类
            return new RedisVertxCacheSource().acceptsConf(config);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public CacheSource createInstance() {
        return new RedisVertxCacheSource();
    }

}
