/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.nio.charset.StandardCharsets;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import org.redkale.convert.json.JsonFactory;
import org.redkale.inject.ResourceFactory;
import org.redkale.net.AsyncIOGroup;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_MAXCONNS;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_NODES;
import org.redkale.source.CacheEventListener;
import org.redkale.util.AnyValueWriter;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class PubSubRedisTest {

    private static final String TOPIC = "channel001";

    public static void main(String[] args) throws Exception {
        AnyValueWriter conf = new AnyValueWriter()
                .addValue(CACHE_SOURCE_MAXCONNS, "1")
                .addValue(CACHE_SOURCE_NODES, "redis://127.0.0.1:6363");
        final ResourceFactory factory = ResourceFactory.create();
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        asyncGroup.start();
        factory.register(RESNAME_APP_CLIENT_ASYNCGROUP, asyncGroup);

        RedisVertxCacheSource source = new RedisVertxCacheSource();
        factory.inject(source);
        source.defaultConvert = JsonFactory.root().getConvert();
        source.init(conf);
        run(source);
    }

    public static void run(RedisSource source) throws Exception {
        // ----------------------------------------------
        TestEventListener listener = new TestEventListener();
        source.subscribe(listener, TOPIC);
        source.publish(TOPIC, "这是一个推送消息");
        System.in.read();
        Utility.sleep(1000);
        source.publish(TOPIC, "这是一个推送消息");
        // ----------------------------------------------
        Utility.sleep(3000);
        source.close();
    }

    public static class TestEventListener implements CacheEventListener<byte[]> {

        @Override
        public void onMessage(String topic, byte[] message) {
            System.out.println("收到topic(" + topic + ")消息: " + new String(message, StandardCharsets.UTF_8));
        }
    }
}
