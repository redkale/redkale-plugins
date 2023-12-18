/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import org.redkale.convert.json.JsonFactory;
import org.redkale.inject.ResourceFactory;
import org.redkale.net.AsyncIOGroup;
import org.redkale.net.client.*;
import static org.redkale.source.AbstractCacheSource.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class RedisCacheSourceTest extends RedisAbstractTest {

    public static void main(String[] args) throws Exception {
        AnyValueWriter conf = new AnyValueWriter()
            .addValue(CACHE_SOURCE_MAXCONNS, "1")
            .addValue(CACHE_SOURCE_NODES, "redis://127.0.0.1:6363");
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

    @Test
    public void run() throws Exception {
        final AsyncIOGroup asyncGroup = new AsyncIOGroup(8192, 16);
        RedisCacheClient client = new RedisCacheClient("test", "test", asyncGroup, "", new ClientAddress(new InetSocketAddress("127.0.0.1", 3389)), 2, 2, null, null);
        RedisCacheConnection conn = (RedisCacheConnection) client.createClientConnection(asyncGroup.newTCPClientConnection());
        RedisCacheCodec codec = new RedisCacheCodec(conn);
        ByteArray array = new ByteArray();

        List<ClientResponse<RedisCacheRequest, RedisCacheResult>> respResults = new ArrayList();
        try {
            Field respResultsField = ClientCodec.class.getDeclaredField("respResults");
            respResultsField.setAccessible(true);
            respResults = (List) respResultsField.get(codec);
        } catch (Exception e) {
            e.printStackTrace();
        }
        {
            respResults.clear();
            byte[] data = "*2  $1  9  *4  $4  key1  $2  10  $4  key2  $2  30 ".replaceAll("\\s+", "\r\n").getBytes();
            ByteBuffer buf = ByteBuffer.wrap(data);
            codec.decodeMessages(buf, array);
            System.out.println("had result 1: " + respResults.size());
            Assertions.assertEquals(9, respResults.get(0).getMessage().getCursor());
            Assertions.assertEquals(4, respResults.get(0).getMessage().frameList.size());
            Assertions.assertTrue(!respResults.isEmpty());
        }
        {
            respResults.clear();
            codec.decodeMessages(ByteBuffer.wrap("*2  $1".replaceAll("\\s+", "\r\n").getBytes()), array);
            codec.decodeMessages(ByteBuffer.wrap("  9  *4  $4  key1  $2  10  $4  key2  $2  30 ".replaceAll("\\s+", "\r\n").getBytes()), array);
            System.out.println("had result 1: " + respResults.size());
            Assertions.assertEquals(9, respResults.get(0).getMessage().getCursor());
            Assertions.assertEquals(4, respResults.get(0).getMessage().frameList.size());
            Assertions.assertTrue(!respResults.isEmpty());
        }
        {
            respResults.clear();
            codec.decodeMessages(ByteBuffer.wrap("*2  $".replaceAll("\\s+", "\r\n").getBytes()), array);
            codec.decodeMessages(ByteBuffer.wrap("1  9  *4  $4  key1  $2  10  $4  key2  $2  30 ".replaceAll("\\s+", "\r\n").getBytes()), array);
            System.out.println("had result 1: " + respResults.size());
            Assertions.assertEquals(9, respResults.get(0).getMessage().getCursor());
            Assertions.assertEquals(4, respResults.get(0).getMessage().frameList.size());
            Assertions.assertTrue(!respResults.isEmpty());
        }
    }
}
