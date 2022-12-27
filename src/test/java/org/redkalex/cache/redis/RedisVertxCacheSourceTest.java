/*
 */
package org.redkalex.cache.redis;

import org.redkale.convert.json.JsonFactory;
import static org.redkale.source.AbstractCacheSource.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class RedisVertxCacheSourceTest extends RedisAbstractTest {

    public static void main(String[] args) throws Exception {
        AnyValue.DefaultAnyValue conf = new AnyValue.DefaultAnyValue().addValue(CACHE_SOURCE_MAXCONNS, "1");
        conf.addValue(CACHE_SOURCE_NODE, new AnyValue.DefaultAnyValue().addValue(CACHE_SOURCE_URL, "redis://127.0.0.1:6363"));
        final ResourceFactory factory = ResourceFactory.create();

        RedisVertxCacheSource source = new RedisVertxCacheSource();
        factory.inject(source);
        source.defaultConvert = JsonFactory.root().getConvert();
        source.init(conf);
        try {
            run(source);
        } finally {
            source.close();
        }
    }

}
