/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.*;
import org.junit.jupiter.api.Assertions;
import org.redkale.convert.json.JsonConvert;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public abstract class RedisAbstractTest {

    protected static void run(CacheSource source) throws Exception {
        System.out.println("------------------------------------");
        source.remove("stritem1");
        source.remove("stritem2");
        source.setString("stritem1", "value1");
        source.setString("stritem2", "value2");

        List<String> list = source.queryKeysStartsWith("stritem");
        System.out.println("stritem开头的key有两个: " + list);
        Assertions.assertTrue(Utility.equalsElement(list, List.of("stritem2", "stritem1")));

        Map<String, String> map = source.getStringMap("stritem1", "stritem2");
        System.out.println("[有值] MGET : " + map);
        Assertions.assertTrue(Utility.equalsElement(map, Utility.ofMap("stritem1", "value1", "stritem2", "value2")));

        String[] array = source.getStringArray("stritem1", "stritem2");
        System.out.println("[有值] MGET : " + Arrays.toString(array));
        Assertions.assertTrue(Utility.equalsElement(array, new String[]{"value1", "value2"}));

        source.remove("intitem1");
        source.remove("intitem2");
        source.setLong("intitem1", 333);
        source.setLong("intitem2", 444);

        map = source.getStringMap("intitem1", "intitem22", "intitem2");
        System.out.println("[有值] MGET : " + map);
        Assertions.assertTrue(Utility.equalsElement(map, Utility.ofMap("intitem1", "333", "intitem2", "444")));

        array = source.getStringArray("intitem1", "intitem22", "intitem2");
        System.out.println("[有值] MGET : " + Arrays.toString(array));
        Assertions.assertTrue(Utility.equalsElement(array, new Object[]{"333", null, "444"}));

        source.remove("objitem1");
        source.remove("objitem2");
        source.set("objitem1", Flipper.class, new Flipper(10));
        source.set("objitem2", Flipper.class, new Flipper(20));

        Map<String, Flipper> flippermap = source.getMap(Flipper.class, "objitem1", "objitem2");
        System.out.println("[有值] MGET : " + flippermap);
        Assertions.assertTrue(Utility.equalsElement(flippermap, Utility.ofMap("objitem1", new Flipper(10), "objitem2", new Flipper(20))));

        source.remove("key1");
        source.remove("key2");
        source.remove("300");
        source.set(1000, "key1", String.class, "value1");
        source.set("key1", String.class, "value1");
        source.setString("keystr1", "strvalue1");
        source.setLong("keylong1", 333L);
        source.set("300", String.class, "4000");
        Object obj = source.getAndRefresh("key1", 3500, String.class);
        System.out.println("[有值] key1 GET : " + obj);
        Assertions.assertEquals("value1", obj);

        obj = source.get("300", String.class);
        System.out.println("[有值] 300 GET : " + obj);
        Assertions.assertEquals("4000", obj);

        obj = source.get("key1", String.class);
        System.out.println("[有值] key1 GET : " + obj);
        Assertions.assertEquals("value1", obj);

        obj = source.get("key2", String.class);
        System.out.println("[无值] key2 GET : " + obj);
        Assertions.assertNull(obj);

        obj = source.getString("keystr1");
        System.out.println("[有值] keystr1 GET : " + obj);
        Assertions.assertEquals("strvalue1", obj);

        long num = source.getLong("keylong1", 0L);
        System.out.println("[有值] keylong1 GET : " + null);
        Assertions.assertEquals(333L, num);

        boolean bool = source.exists("key1");
        System.out.println("[有值] key1 EXISTS : " + bool);
        Assertions.assertTrue(bool);

        bool = source.exists("key2");
        System.out.println("[无值] key2 EXISTS : " + bool);
        Assertions.assertFalse(bool);

        source.remove("keys3");
        source.appendListItem("keys3", String.class, "vals1");
        source.appendListItem("keys3", String.class, "vals2");
        System.out.println("-------- keys3 追加了两个值 --------");

        Collection col = source.getCollection("keys3", String.class);
        System.out.println("[两值] keys3 VALUES : " + col);
        Assertions.assertTrue(Utility.equalsElement(col, List.of("vals1", "vals2")));

        bool = source.exists("keys3");
        System.out.println("[有值] keys3 EXISTS : " + bool);
        Assertions.assertTrue(bool);

        source.removeListItem("keys3", String.class, "vals1");
        col = source.getCollection("keys3", String.class);
        System.out.println("[一值] keys3 VALUES : " + col);
        Assertions.assertIterableEquals(col, List.of("vals2"));

        col = source.getCollectionAndRefresh("keys3", 3000, String.class);
        System.out.println("[一值] keys3 VALUES : " + col);
        Assertions.assertIterableEquals(col, List.of("vals2"));

        source.remove("stringmap");
        source.appendSetItem("stringmap", JsonConvert.TYPE_MAP_STRING_STRING, Utility.ofMap("a", "aa", "b", "bb"));
        source.appendSetItem("stringmap", JsonConvert.TYPE_MAP_STRING_STRING, Utility.ofMap("c", "cc", "d", "dd"));
        col = source.getCollection("stringmap", JsonConvert.TYPE_MAP_STRING_STRING);
        System.out.println("[两值] stringmap VALUES : " + col);
        Assertions.assertIterableEquals(col, List.of(Utility.ofMap("c", "cc", "d", "dd"), Utility.ofMap("a", "aa", "b", "bb")));

        source.remove("sets3");
        source.remove("sets4");
        source.appendSetItem("sets3", String.class, "setvals1");
        source.appendSetItem("sets3", String.class, "setvals2");
        source.appendSetItem("sets3", String.class, "setvals1");
        source.appendSetItem("sets4", String.class, "setvals2");
        source.appendSetItem("sets4", String.class, "setvals1");
        col = source.getCollection("sets3", String.class);
        System.out.println("[两值] sets3 VALUES : " + col);
        Assertions.assertIterableEquals(col, List.of("setvals2", "setvals1"));

        bool = source.exists("sets3");
        System.out.println("[有值] sets3 EXISTS : " + bool);
        Assertions.assertTrue(bool);

        bool = source.existsSetItem("sets3", String.class, "setvals2");
        System.out.println("[有值] sets3-setvals2 EXISTSITEM : " + bool);
        Assertions.assertTrue(bool);

        bool = source.existsSetItem("sets3", String.class, "setvals3");
        System.out.println("[无值] sets3-setvals3 EXISTSITEM : " + bool);
        Assertions.assertFalse(bool);

        source.removeSetItem("sets3", String.class, "setvals1");
        col = source.getCollection("sets3", String.class);
        System.out.println("[一值] sets3 VALUES : " + col);
        Assertions.assertIterableEquals(col, List.of("setvals2"));

        int size = source.getCollectionSize("sets3");
        System.out.println("sets3 大小 : " + size);
        Assertions.assertEquals(1, size);

        col = source.queryKeys();
        System.out.println("all keys: " + col);

        col = source.queryKeysStartsWith("key");
        Collections.sort((List<String>) col);
        System.out.println("key startkeys: " + col);
        Assertions.assertIterableEquals(col, List.of("key1", "keylong1", "keys3", "keystr1"));

        num = source.incr("newnum");
        System.out.println("newnum 值 : " + num);
        Assertions.assertEquals(1, num);

        num = source.decr("newnum");
        System.out.println("newnum 值 : " + num);
        Assertions.assertEquals(0, num);

        Map<String, Collection> mapcol = (Map) source.getStringCollectionMap(true, "sets3", "sets4");
        System.out.println("sets3&sets4:  " + mapcol);
        Assertions.assertEquals(mapcol.toString(), Utility.ofMap("sets3", Set.of("setvals2"), "sets4", Set.of("setvals2", "setvals1")).toString());

        System.out.println("------------------------------------");
        InetSocketAddress addr88 = new InetSocketAddress("127.0.0.1", 7788);
        InetSocketAddress addr99 = new InetSocketAddress("127.0.0.1", 7799);
        source.set("myaddr", InetSocketAddress.class, addr88);

        obj = source.getString("myaddr");
        System.out.println("myaddrstr:  " + obj);
        Assertions.assertEquals("\"127.0.0.1:7788\"", obj);

        obj = source.get("myaddr", InetSocketAddress.class);
        System.out.println("myaddr:  " + obj);
        Assertions.assertEquals(addr88, obj);

        source.remove("myaddrs");
        source.remove("myaddrs2");
        source.appendSetItem("myaddrs", InetSocketAddress.class, addr88);
        source.appendSetItem("myaddrs", InetSocketAddress.class, addr99);

        col = source.getCollection("myaddrs", InetSocketAddress.class);
        System.out.println("myaddrs:  " + col);
        Assertions.assertIterableEquals(col, List.of(addr88, addr99));

        source.removeSetItem("myaddrs", InetSocketAddress.class, addr88);
        col = source.getCollection("myaddrs", InetSocketAddress.class);
        System.out.println("myaddrs:  " + col);
        Assertions.assertIterableEquals(col, List.of(addr99));

        source.appendSetItem("myaddrs2", InetSocketAddress.class, addr88);
        source.appendSetItem("myaddrs2", InetSocketAddress.class, addr99);
        mapcol = (Map) source.getCollectionMap(true, InetSocketAddress.class, "myaddrs", "myaddrs2");
        System.out.println("myaddrs&myaddrs2:  " + mapcol);
        Assertions.assertEquals(mapcol.toString(), Utility.ofMap("myaddrs", List.of(addr99), "myaddrs2", List.of(addr88, addr99)).toString());

        System.out.println("------------------------------------");
        source.remove("myaddrs");
        Type mapType = new TypeToken<Map<String, Integer>>() {
        }.getType();
        Map<String, Integer> paramap = new HashMap<>();
        paramap.put("a", 1);
        paramap.put("b", 2);
        source.set("mapvals", mapType, paramap);

        map = source.get("mapvals", mapType);
        System.out.println("mapvals:  " + map);
        Assertions.assertEquals(map.toString(), Utility.ofMap("a", 1, "b", 2).toString());

        source.remove("byteskey");
        source.setBytes("byteskey", new byte[]{1, 2, 3});
        byte[] bs = source.getBytes("byteskey");
        System.out.println("byteskey 值 : " + Arrays.toString(bs));
        Assertions.assertEquals(Arrays.toString(new byte[]{1, 2, 3}), Arrays.toString(bs));
        //h
        source.remove("hmap");
        source.hincr("hmap", "key1");
        num = source.hgetLong("hmap", "key1", -1);
        System.out.println("hmap.key1 值 : " + num);
        Assertions.assertEquals(1L, num);

        source.hmset("hmap", "key2", "haha", "key3", 333);
        source.hmset("hmap", "sm", (HashMap) Utility.ofMap("a", "aa", "b", "bb"));

        map = source.hget("hmap", "sm", JsonConvert.TYPE_MAP_STRING_STRING);
        System.out.println("hmap.sm 值 : " + map);
        Assertions.assertEquals(map.toString(), Utility.ofMap("a", "aa", "b", "bb").toString());

        col = source.hmget("hmap", String.class, "key1", "key2", "key3");
        System.out.println("hmap.[key1,key2,key3] 值 : " + col);
        Assertions.assertIterableEquals(col, List.of("1", "haha", "333"));

        col = source.hkeys("hmap");
        System.out.println("hmap.keys 四值 : " + col);
        Assertions.assertIterableEquals(col, List.of("key1", "key2", "key3", "sm"));

        source.hremove("hmap", "key1", "key3");

        col = source.hkeys("hmap");
        System.out.println("hmap.keys 两值 : " + col);
        Assertions.assertIterableEquals(col, List.of("key2", "sm"));

        obj = source.hgetString("hmap", "key2");
        System.out.println("hmap.key2 值 : " + obj);
        Assertions.assertEquals("haha", obj);

        size = source.hsize("hmap");
        System.out.println("hmap列表(2)大小 : " + size);
        Assertions.assertEquals(2, size);

        source.remove("hmaplong");
        source.hincr("hmaplong", "key1", 10);
        source.hsetLong("hmaplong", "key2", 30);

        Map<String, Long> longmap = source.hmap("hmaplong", long.class, 0, 10);
        System.out.println("hmaplong.所有两值 : " + longmap);
        Assertions.assertEquals(longmap.toString(), Utility.ofMap("key1", 10, "key2", 30).toString());

        source.remove("hmapstr");
        source.hsetString("hmapstr", "key1", "str10");
        source.hsetString("hmapstr", "key2", null);
        map = source.hmap("hmapstr", String.class, 0, 10);
        System.out.println("hmapstr.所有一值 : " + map);
        Assertions.assertEquals(map.toString(), Utility.ofMap("key1", "str10").toString());

        source.remove("hmapstrmap");
        source.hset("hmapstrmap", "key1", JsonConvert.TYPE_MAP_STRING_STRING, (HashMap) Utility.ofMap("ks11", "vv11"));
        source.hset("hmapstrmap", "key2", JsonConvert.TYPE_MAP_STRING_STRING, null);
        map = source.hmap("hmapstrmap", JsonConvert.TYPE_MAP_STRING_STRING, 0, 10, "key2*");
        System.out.println("hmapstrmap.无值 : " + map);
        Assertions.assertEquals(map.toString(), Utility.ofMap().toString());

        source.remove("popset");
        source.appendStringSetItem("popset", "111");
        source.appendStringSetItem("popset", "222");
        source.appendStringSetItem("popset", "333");
        source.appendStringSetItem("popset", "444");
        source.appendStringSetItem("popset", "555");

        obj = source.spopStringSetItem("popset");
        System.out.println("SPOP一个元素：" + obj);
        Assertions.assertTrue(List.of("111", "222", "333", "444", "555").contains(obj));

        col = source.spopStringSetItem("popset", 2);
        System.out.println("SPOP两个元素：" + col);
        System.out.println("SPOP五个元素：" + source.spopStringSetItem("popset", 5));

        source.appendLongSetItem("popset", 111);
        source.appendLongSetItem("popset", 222);
        source.appendLongSetItem("popset", 333);
        source.appendLongSetItem("popset", 444);
        source.appendLongSetItem("popset", 555);
        System.out.println("SPOP一个元素：" + source.spopLongSetItem("popset"));
        System.out.println("SPOP两个元素：" + source.spopLongSetItem("popset", 2));
        System.out.println("SPOP五个元素：" + source.spopLongSetItem("popset", 5));
        System.out.println("SPOP一个元素：" + source.spopLongSetItem("popset"));

        //清除
        int rs = source.remove("stritem1");
        System.out.println("删除stritem1个数: " + rs);
        source.remove("popset");
        source.remove("stritem2");
        source.remove("intitem1");
        source.remove("intitem2");
        source.remove("keylong1");
        source.remove("keystr1");
        source.remove("mapvals");
        source.remove("myaddr");
        source.remove("myaddrs2");
        source.remove("newnum");
        source.remove("objitem1");
        source.remove("objitem2");
        source.remove("key1");
        source.remove("key2");
        source.remove("keys3");
        source.remove("sets3");
        source.remove("sets4");
        source.remove("myaddrs");
        source.remove("300");
        source.remove("stringmap");
        source.remove("hmap");
        source.remove("hmaplong");
        source.remove("hmapstr");
        source.remove("hmapstrmap");
        source.remove("byteskey");
        System.out.println("------------------------------------");
//        System.out.println("--------------测试大文本---------------");
//        HashMap<String, String> bigmap = new HashMap<>();
//        StringBuilder sb = new StringBuilder();
//        sb.append("起始");
//        for (int i = 0; i < 1024 * 1024; i++) {
//            sb.append("abcde");
//        }
//        sb.append("结束");
//        bigmap.put("val", sb.toString());
//        System.out.println("文本长度: " + sb.length());
//        source.set("bigmap", JsonConvert.TYPE_MAP_STRING_STRING, bigmap);
//        System.out.println("写入完成");
//        for (int i = 0; i < 1; i++) {
//            HashMap<String, String> fs = (HashMap) source.get("bigmap", JsonConvert.TYPE_MAP_STRING_STRING);
//            System.out.println("内容长度: " + fs.get("val").length());
//        }
        source.remove("bigmap");
    }
}
