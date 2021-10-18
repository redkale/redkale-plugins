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
@Priority(-900)
public class RedisCacheSourceProvider implements CacheSourceProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        return new RedisCacheSource().acceptsConf(config);
    }

    @Override
    public Class<? extends CacheSource> sourceClass() {
        return RedisCacheSource.class;
    }

}
