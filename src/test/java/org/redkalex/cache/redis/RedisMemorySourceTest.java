/*
 *
 */
package org.redkalex.cache.redis;

import static org.redkalex.cache.redis.RedisAbstractTest.run;

import org.redkale.source.CacheMemorySource;

/** @author zhangjx */
public class RedisMemorySourceTest extends RedisAbstractTest {

    public static void main(String[] args) throws Exception {

        CacheMemorySource source = new CacheMemorySource("");
        source.init(null);
        try {
            run(source, true);
        } finally {
            source.close();
        }
    }
}
