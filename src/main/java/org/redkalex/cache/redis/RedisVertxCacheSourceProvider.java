/*
 */
package org.redkalex.cache.redis;

import javax.annotation.Priority;
import org.redkale.source.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
@Priority(-200)
public class RedisVertxCacheSourceProvider implements CacheSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            Object.class.isAssignableFrom(io.vertx.redis.client.RedisOptions.class); //试图加载vertx-redis相关类
            RedisVertxCacheSource source = RedisVertxCacheSource.class.getConstructor().newInstance();
            return source.acceptsConf(config);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public Class<? extends CacheSource> sourceClass() {
        return RedisVertxCacheSource.class;
    }

}
