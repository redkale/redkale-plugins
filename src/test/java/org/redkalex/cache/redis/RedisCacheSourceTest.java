/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import org.redkale.convert.json.JsonFactory;
import org.redkale.net.AsyncIOGroup;
import static org.redkale.source.AbstractCacheSource.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class RedisCacheSourceTest extends RedisAbstractTest {

    public static void main(String[] args) throws Exception {
        AnyValue.DefaultAnyValue conf = new AnyValue.DefaultAnyValue().addValue(CACHE_SOURCE_MAXCONNS, "1");
        conf.addValue(CACHE_SOURCE_NODE, new AnyValue.DefaultAnyValue().addValue(CACHE_SOURCE_URL, "redis://127.0.0.1:6363"));
        final ResourceFactory factory = ResourceFactory.create();
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup);

        RedisCacheSource source = new RedisCacheSource();
        factory.inject(source);
        source.defaultConvert = JsonFactory.root().getConvert();
        source.init(conf);
        try {
            run(source, true);
        } finally {
            source.close();
        }
    }

}
