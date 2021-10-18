/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import org.redkale.convert.json.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class RedissionCacheSourceTest extends RedisAbstractTest {

    public static void main(String[] args) throws Exception {
        AnyValue.DefaultAnyValue conf = new AnyValue.DefaultAnyValue().addValue("maxconns", "1");
        conf.addValue("node", new AnyValue.DefaultAnyValue().addValue("addr", "redis://127.0.0.1:6363"));

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
