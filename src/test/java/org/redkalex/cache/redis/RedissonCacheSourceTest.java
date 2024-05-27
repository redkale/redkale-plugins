/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import static org.redkale.source.AbstractCacheSource.*;

import org.redkale.convert.json.JsonFactory;
import org.redkale.util.AnyValueWriter;

/** @author zhangjx */
public class RedissonCacheSourceTest extends RedisAbstractTest {

    public static void main(String[] args) throws Exception {
        AnyValueWriter conf = new AnyValueWriter()
                .addValue(CACHE_SOURCE_MAXCONNS, "1")
                .addValue(CACHE_SOURCE_NODES, "redis://127.0.0.1:6363");

        RedissonCacheSource source = new RedissonCacheSource();
        source.defaultConvert = JsonFactory.root().getConvert();
        source.init(conf);
        try {
            run(source, true);
        } finally {
            source.close();
        }
    }
}
