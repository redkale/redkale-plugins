/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import org.redkale.convert.json.*;
import static org.redkale.source.AbstractCacheSource.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class RedissionCacheSourceTest extends RedisAbstractTest {

    public static void main(String[] args) throws Exception {
        AnyValue.DefaultAnyValue conf = new AnyValue.DefaultAnyValue();
        conf.addValue(CACHE_SOURCE_NODE, new AnyValue.DefaultAnyValue().addValue(CACHE_SOURCE_URL, "redis://127.0.0.1:6363"));

        RedissionCacheSource source = new RedissionCacheSource();
        source.defaultConvert = JsonFactory.root().getConvert();
        source.init(conf);
        try {
            run(source);
        } finally {
            source.close();
        }
    }

}