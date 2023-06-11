/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.cache.redis;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Assertions;
import static org.redkale.boot.Application.RESNAME_APP_CLIENT_ASYNCGROUP;
import org.redkale.convert.json.*;
import org.redkale.net.AsyncIOGroup;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_MAXCONNS;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_NODE;
import static org.redkale.source.AbstractCacheSource.CACHE_SOURCE_URL;
import org.redkale.source.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public abstract class RedisAbstractTest {

    protected static void run(CacheSource source, boolean press) throws Exception {
        System.out.println("------------------------------------");
        source.del("stritem1", "stritem2", "stritem1x");
        source.setString("stritem1", "value1");
        source.setString("stritem2", "value2");

        List<String> list = source.keysStartsWith("stritem");
        System.out.println("stritem开头的key有两个: " + list);
        Assertions.assertTrue(Utility.equalsElement(list, List.of("stritem2", "stritem1")));

        Map<String, String> map = source.mgetString("stritem1", "stritem2");
        System.out.println("[有值] MGET : " + map);
        Assertions.assertTrue(Utility.equalsElement(map, Utility.ofMap("stritem1", "value1", "stritem2", "value2")));

        String[] array = source.mgetsString("stritem1", "stritem2");
        System.out.println("[有值] MGET : " + Arrays.toString(array));
        Assertions.assertTrue(Utility.equalsElement(array, new String[]{"value1", "value2"}));

        Assertions.assertFalse(source.persist("stritem1"));
        Assertions.assertTrue(source.rename("stritem1", "stritem1x"));
        Assertions.assertEquals("value1", source.getString("stritem1x"));
        Assertions.assertEquals(null, source.getString("stritem1"));
        Assertions.assertFalse(source.renamenx("stritem1x", "stritem2"));
        Assertions.assertEquals("value2", source.getString("stritem2"));
        Assertions.assertTrue(source.renamenx("stritem1x", "stritem1"));

        source.del("intitem1", "intitem2");
        source.setLong("intitem1", 333);
        source.setLong("intitem2", 444);

        map = source.mgetString("intitem1", "intitem22", "intitem2");
        System.out.println("[有值] MGET : " + map);
        Assertions.assertTrue(Utility.equalsElement(map, Utility.ofMap("intitem1", "333", "intitem2", "444")));

        array = source.mgetsString("intitem1", "intitem22", "intitem2");
        System.out.println("[有值] MGET : " + Arrays.toString(array));
        Assertions.assertTrue(Utility.equalsElement(array, new Object[]{"333", null, "444"}));

        source.del("objitem1", "objitem2");
        source.mset(Utility.ofMap("objitem1", new Flipper(10), "objitem2", new Flipper(20)));

        Map<String, Flipper> flippermap = source.mget(Flipper.class, "objitem1", "objitem2");
        System.out.println("[有值] MGET : " + flippermap);
        Assertions.assertTrue(Utility.equalsElement(flippermap, Utility.ofMap("objitem1", new Flipper(10), "objitem2", new Flipper(20))));

        source.del("key1", "key2", "300");
        source.setex("key1", 1000, String.class, "value1");
        source.set("key1", String.class, "value1");
        source.setString("keystr1", "strvalue1");
        source.setLong("keylong1", 333L);
        source.set("300", String.class, "4000");
        Object obj = source.getex("key1", 3500, String.class);
        System.out.println("[有值] key1 GET : " + obj);
        Assertions.assertEquals("value1", obj);

        obj = source.get("300", String.class);
        System.out.println("[有值] 300 GET : " + obj);
        Assertions.assertEquals("4000", obj);

        obj = source.get("key1", String.class);
        System.out.println("[有值] key1 GET : " + obj);
        Assertions.assertEquals("value1", obj);

        obj = source.getSet("key1", String.class, "value11");
        System.out.println("[旧值] key1 GETSET : " + obj);
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

        source.del("keys3");
        source.rpush("keys3", String.class, "vals1");
        source.rpush("keys3", String.class, "vals2");
        System.out.println("-------- keys3 追加了两个值 --------");

        Collection col = source.lrangeString("keys3");
        System.out.println("[两值] keys3 VALUES : " + col);
        Assertions.assertTrue(Utility.equalsElement(col, List.of("vals1", "vals2")));

        bool = source.exists("keys3");
        System.out.println("[有值] keys3 EXISTS : " + bool);
        Assertions.assertTrue(bool);

        source.lrem("keys3", String.class, "vals1");
        col = source.lrangeString("keys3");
        System.out.println("[一值] keys3 VALUES : " + col);
        Assertions.assertIterableEquals(col, List.of("vals2"));
        source.rpush("keys3", String.class, "vals2");
        source.rpush("keys3", String.class, "vals3");
        source.rpush("keys3", String.class, "vals4");
        Assertions.assertEquals("vals4", source.rpopString("keys3"));
        source.lpush("keys3", String.class, "vals0");
        Assertions.assertEquals("vals0", source.lpopString("keys3"));
        String rlv = source.rpoplpush("keys3", "keys3-2", String.class);
        Assertions.assertEquals("vals3", rlv);

        source.del("stringmap");
        source.sadd("stringmap", JsonConvert.TYPE_MAP_STRING_STRING, Utility.ofMap("a", "aa", "b", "bb"));
        source.sadd("stringmap", JsonConvert.TYPE_MAP_STRING_STRING, Utility.ofMap("c", "cc", "d", "dd"));
        col = source.smembers("stringmap", JsonConvert.TYPE_MAP_STRING_STRING);
        System.out.println("[两值] stringmap VALUES : " + col);
        Assertions.assertIterableEquals(col, List.of(Utility.ofMap("c", "cc", "d", "dd"), Utility.ofMap("a", "aa", "b", "bb")));

        source.del("sets3");
        source.del("sets4");
        source.sadd("sets3", String.class, "setvals1");
        source.sadd("sets3", String.class, "setvals2");
        source.sadd("sets3", String.class, "setvals1");
        source.sadd("sets4", String.class, "setvals2");
        source.sadd("sets4", String.class, "setvals1");
        col = source.smembersString("sets3");
        System.out.println("[两值] sets3 VALUES : " + col);
        List col2 = new ArrayList(col);
        Collections.sort(col2);
        Assertions.assertIterableEquals(col2, List.of("setvals1", "setvals2"));

        bool = source.exists("sets3");
        System.out.println("[有值] sets3 EXISTS : " + bool);
        Assertions.assertTrue(bool);

        bool = source.sismember("sets3", String.class, "setvals2");
        System.out.println("[有值] sets3-setvals2 EXISTSITEM : " + bool);
        Assertions.assertTrue(bool);

        bool = source.sismember("sets3", String.class, "setvals3");
        System.out.println("[无值] sets3-setvals3 EXISTSITEM : " + bool);
        Assertions.assertFalse(bool);

        source.srem("sets3", String.class, "setvals1");
        col = source.smembersString("sets3");
        System.out.println("[一值] sets3 VALUES : " + col);
        Assertions.assertIterableEquals(col, List.of("setvals2"));

        int size = (int) source.scard("sets3");
        System.out.println("sets3 大小 : " + size);
        Assertions.assertEquals(1, size);

        col = source.keys();
        System.out.println("all keys: " + col);

        col = source.keys("key*");
        Collections.sort((List<String>) col);
        System.out.println("key startkeys: " + col);
        Assertions.assertIterableEquals(col, List.of("key1", "keylong1", "keys3", "keys3-2", "keystr1"));

        num = source.incr("newnum");
        System.out.println("newnum 值 : " + num);
        Assertions.assertEquals(1, num);

        num = source.decr("newnum");
        System.out.println("newnum 值 : " + num);
        Assertions.assertEquals(0, num);

        Map<String, Collection> mapcol = new LinkedHashMap<>();
        mapcol.put("sets3", source.smembersString("sets3"));
        mapcol.put("sets4", source.smembersString("sets4"));
        System.out.println("sets3&sets4:  " + mapcol);
        Map<String, Collection> news = new HashMap<>();
        mapcol.forEach((x, y) -> {
            if (y instanceof Set) {
                List newy = new ArrayList(y);
                Collections.sort(newy);
                news.put(x, newy);
            } else {
                Collections.sort((List) y);
            }
        });
        mapcol.putAll(news);
        Assertions.assertEquals(Utility.ofMap("sets3", List.of("setvals2"), "sets4", List.of("setvals1", "setvals2")).toString(), mapcol.toString());

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

        source.del("myaddrs");
        source.del("myaddrs2");
        source.sadd("myaddrs", InetSocketAddress.class, addr88);
        source.sadd("myaddrs", InetSocketAddress.class, addr99);

        col = source.smembers("myaddrs", InetSocketAddress.class);
        System.out.println("myaddrs:  " + col);
        List cola2 = new ArrayList(col);
        Collections.sort(cola2, (o1, o2) -> o1.toString().compareTo(o2.toString()));
        Assertions.assertIterableEquals(cola2, List.of(addr88, addr99));

        source.srem("myaddrs", InetSocketAddress.class, addr88);
        col = source.smembers("myaddrs", InetSocketAddress.class);
        System.out.println("myaddrs:  " + col);
        Assertions.assertIterableEquals(col, List.of(addr99));

        source.sadd("myaddrs2", InetSocketAddress.class, addr88);
        source.sadd("myaddrs2", InetSocketAddress.class, addr99);
        mapcol.clear();
        mapcol.put("myaddrs", source.smembers("myaddrs", InetSocketAddress.class));
        mapcol.put("myaddrs2", source.smembers("myaddrs2", InetSocketAddress.class));
        System.out.println("myaddrs&myaddrs2:  " + mapcol);
        Map<String, Collection> news2 = new HashMap<>();
        mapcol.forEach((x, y) -> {
            if (y instanceof Set) {
                List newy = new ArrayList(y);
                Collections.sort(newy, (o1, o2) -> o1.toString().compareTo(o2.toString()));
                news2.put(x, newy);
            } else {
                Collections.sort((List) y, (o1, o2) -> o1.toString().compareTo(o2.toString()));
            }
        });
        mapcol.putAll(news2);
        Assertions.assertEquals(Utility.ofMap("myaddrs", List.of(addr99), "myaddrs2", List.of(addr88, addr99)).toString(), mapcol.toString());

        System.out.println("------------------------------------");
        source.del("myaddrs");
        Type mapType = new TypeToken<Map<String, Integer>>() {
        }.getType();
        Map<String, Integer> paramap = new HashMap<>();
        paramap.put("a", 1);
        paramap.put("b", 2);
        source.set("mapvals", mapType, paramap);

        map = source.get("mapvals", mapType);
        System.out.println("mapvals:  " + map);
        Assertions.assertEquals(Utility.ofMap("a", 1, "b", 2).toString(), map.toString());

        source.del("hmapall");
        source.hmset("hmapall", Utility.ofMap("k1", "111", "k2", "222"));
        Assertions.assertEquals(List.of("111", "222"), source.hvals("hmapall", String.class));
        Assertions.assertEquals(List.of("111", "222"), source.hvalsString("hmapall"));
        Assertions.assertEquals(List.of(111L, 222L), source.hvalsLong("hmapall"));
        Assertions.assertEquals(List.of("111", "222"), source.hvalsAsync("hmapall", String.class).join());
        Assertions.assertEquals(List.of("111", "222"), source.hvalsStringAsync("hmapall").join());
        Assertions.assertEquals(List.of(111L, 222L), source.hvalsLongAsync("hmapall").join());
        Assertions.assertEquals(Utility.ofMap("k1", "111", "k2", "222"), source.hgetall("hmapall", String.class));
        Assertions.assertEquals(Utility.ofMap("k1", "111", "k2", "222"), source.hgetallString("hmapall"));
        Assertions.assertEquals(JsonConvert.root().convertTo(Utility.ofMap("k1", 111L, "k2", 222L)), JsonConvert.root().convertTo(source.hgetallLong("hmapall")));
        Assertions.assertEquals(JsonConvert.root().convertTo(Utility.ofMap("k1", "111", "k2", "222")), JsonConvert.root().convertTo(source.hgetallAsync("hmapall", String.class).join()));
        Assertions.assertEquals(JsonConvert.root().convertTo(Utility.ofMap("k1", "111", "k2", "222")), JsonConvert.root().convertTo(source.hgetallStringAsync("hmapall").join()));
        Assertions.assertEquals(JsonConvert.root().convertTo(Utility.ofMap("k1", 111L, "k2", 222L)), JsonConvert.root().convertTo(source.hgetallLongAsync("hmapall").join()));

        //h
        source.del("hmap");
        source.hincr("hmap", "key1");
        num = source.hgetLong("hmap", "key1", -1);
        System.out.println("hmap.key1 值 : " + num);
        Assertions.assertEquals(1L, num);

        source.hmset("hmap", Utility.ofMap("key2", "haha", "key3", 333));
        source.hmset("hmap", "sm", (HashMap) Utility.ofMap("a", "aa", "b", "bb"));

        map = source.hget("hmap", "sm", JsonConvert.TYPE_MAP_STRING_STRING);
        System.out.println("hmap.sm 值 : " + map);
        Assertions.assertEquals(Utility.ofMap("a", "aa", "b", "bb").toString(), map.toString());

        col = source.hmget("hmap", String.class, "key1", "key2", "key3");
        System.out.println("hmap.[key1,key2,key3] 值 : " + col);
        Assertions.assertIterableEquals(col, List.of("1", "haha", "333"));

        col = source.hkeys("hmap");
        System.out.println("hmap.keys 四值 : " + col);
        Assertions.assertIterableEquals(col, List.of("key1", "key2", "key3", "sm"));

        source.hdel("hmap", "key1", "key3");

        col = source.hkeys("hmap");
        System.out.println("hmap.keys 两值 : " + col);
        Assertions.assertIterableEquals(col, List.of("key2", "sm"));

        obj = source.hgetString("hmap", "key2");
        System.out.println("hmap.key2 值 : " + obj);
        Assertions.assertEquals("haha", obj);

        size = (int) source.hlen("hmap");
        System.out.println("hmap列表(2)大小 : " + size);
        Assertions.assertEquals(2, size);

        source.del("hmaplong");
        source.hincrby("hmaplong", "key1", 10);
        source.hsetLong("hmaplong", "key2", 30);
        AtomicLong cursor = new AtomicLong();
        Map<String, Long> longmap = source.hscan("hmaplong", long.class, cursor, 10);
        System.out.println("hmaplong.所有两值 : " + longmap);
        Assertions.assertEquals(Utility.ofMap("key1", 10, "key2", 30).toString(), longmap.toString());

        source.del("hmapstr");
        source.hsetString("hmapstr", "key1", "str10");
        source.hsetString("hmapstr", "key2", null);
        cursor = new AtomicLong();
        map = source.hscan("hmapstr", String.class, cursor, 10);
        System.out.println("hmapstr.所有一值 : " + map);
        Assertions.assertEquals(Utility.ofMap("key1", "str10").toString(), map.toString());

        source.del("hmapstrmap");
        source.hset("hmapstrmap", "key1", JsonConvert.TYPE_MAP_STRING_STRING, (HashMap) Utility.ofMap("ks11", "vv11"));
        source.hset("hmapstrmap", "key2", JsonConvert.TYPE_MAP_STRING_STRING, null);
        cursor = new AtomicLong();
        map = source.hscan("hmapstrmap", JsonConvert.TYPE_MAP_STRING_STRING, cursor, 10, "key2*");
        System.out.println("hmapstrmap.无值 : " + map);
        Assertions.assertEquals(Utility.ofMap().toString(), map.toString());

        source.del("hmap");
        String vpref = "v";
        int ccc = 600;
        for (int i = 101; i <= ccc + 100; i++) {
            source.hmset("hmap", "k" + i, vpref + i);
        }
        cursor = new AtomicLong();
        Map<String, String> smap = source.hscan("hmap", String.class, cursor, 5);
        System.out.println("hmap.hscan 长度 : " + smap.size() + ", cursor: " + cursor + ", 内容: " + smap);
        //smap.size 是不确定的，可能是全量，也可能比5多，也可能比5少
        Assertions.assertFalse(smap.isEmpty());
        if (smap.size() == ccc) {
            Assertions.assertTrue(cursor.get() == 0);
        } else {
            Assertions.assertTrue(cursor.get() > 0);
        }
        cursor = new AtomicLong();
        smap = (Map) source.hscanAsync("hmap", String.class, cursor, 5).join();
        Assertions.assertFalse(smap.isEmpty());
        if (smap.size() == ccc) {
            Assertions.assertTrue(cursor.get() == 0);
        } else {
            Assertions.assertTrue(cursor.get() > 0);
        }

        source.del("popset");
        source.saddString("popset", "111");
        source.saddString("popset", "222");
        source.saddString("popset", "333");
        source.saddString("popset", "444");
        source.saddString("popset", "555");

        cursor = new AtomicLong();
        Set<String> sset = source.sscan("popset", String.class, cursor, 3);
        System.out.println("popset.sscan 长度 : " + sset.size() + ", cursor: " + cursor + ", 内容: " + sset);
        //smap.size 是不确定的，可能是全量，也可能比5多，也可能比5少
        Assertions.assertFalse(sset.isEmpty());
        if (sset.size() == 5) {
            Assertions.assertTrue(cursor.get() == 0);
        } else {
            Assertions.assertTrue(cursor.get() > 0);
        }
        cursor = new AtomicLong();
        sset = (Set) source.sscanAsync("popset", String.class, cursor, 3).join();
        Assertions.assertFalse(smap.isEmpty());
        if (sset.size() == 5) {
            Assertions.assertTrue(cursor.get() == 0);
        } else {
            Assertions.assertTrue(cursor.get() > 0);
        }

        obj = source.spopString("popset");
        System.out.println("SPOP一个元素：" + obj);
        Assertions.assertTrue(List.of("111", "222", "333", "444", "555").contains(obj));

        col = source.spopString("popset", 2);
        System.out.println("SPOP两个元素：" + col);
        System.out.println("SPOP五个元素：" + source.spopString("popset", 5));

        source.saddLong("popset", 111L);
        source.saddLong("popset", 222L);
        source.saddLong("popset", 333L);
        source.saddLong("popset", 444L, 555L);
        System.out.println("SPOP一个元素：" + source.spopLong("popset"));
        System.out.println("SPOP两个元素：" + source.spopLong("popset", 2));
        System.out.println("SPOP五个元素：" + source.spopLong("popset", 5));
        System.out.println("SPOP一个元素：" + source.spopLong("popset"));

        cursor = new AtomicLong();
        List<String> keys = source.scan(cursor, 5);
        System.out.println("scan 长度 : " + keys.size() + ", cursor: " + cursor + ", 内容: " + keys);
        Assertions.assertFalse(keys.isEmpty());
        Assertions.assertTrue(cursor.get() > 0);
        cursor = new AtomicLong();
        keys = (List) source.scanAsync(cursor, 5).join();
        Assertions.assertFalse(keys.isEmpty());
        if (press) {
            source.del("nxexkey1");
            Assertions.assertTrue(source.setnxexString("nxexkey1", 1, "hahaha"));
            Assertions.assertTrue(!source.setnxexString("nxexkey1", 1, "hehehe"));
            Thread.sleep(1100);
            Assertions.assertTrue(source.setnxexString("nxexkey1", 1, "haha"));
            Assertions.assertTrue(!source.setnxexString("nxexkey1", 1, "hehe"));
            Assertions.assertEquals("haha", source.getString("nxexkey1"));
            source.del("nxexkey1");
            Assertions.assertTrue(source.setnxexLong("nxexkey1", 1, 1111));
            Assertions.assertTrue(!source.setnxexLong("nxexkey1", 1, 2222));
            Thread.sleep(1100);
            Assertions.assertTrue(source.setnxexLong("nxexkey1", 1, 111));
            Assertions.assertTrue(!source.setnxexLong("nxexkey1", 1, 222));
            Assertions.assertEquals(111L, source.getLong("nxexkey1", 0L));
            source.del("nxexkey1");
            Assertions.assertTrue(source.setnxex("nxexkey1", 1, InetSocketAddress.class, addr88));
            Assertions.assertTrue(!source.setnxex("nxexkey1", 1, InetSocketAddress.class, addr99));
            Thread.sleep(1100);
            Assertions.assertTrue(source.setnxex("nxexkey1", 1, InetSocketAddress.class, addr88));
            Assertions.assertTrue(!source.setnxex("nxexkey1", 1, InetSocketAddress.class, addr99));
            Assertions.assertEquals(addr88.toString(), source.get("nxexkey1", InetSocketAddress.class).toString());
        }
        long dbsize = source.dbsize();
        System.out.println("keys总数量 : " + dbsize);
        //清除
        long rs = source.del("stritem1");
        System.out.println("删除stritem1个数: " + rs);
        source.del("popset");
        source.del("stritem2");
        source.del("intitem1");
        source.del("intitem2");
        source.del("keylong1");
        source.del("keystr1");
        source.del("mapvals");
        source.del("myaddr");
        source.del("myaddrs2");
        source.del("newnum");
        source.del("objitem1");
        source.del("objitem2");
        source.del("key1");
        source.del("key2");
        source.del("keys3");
        source.del("keys3-2");
        source.del("sets3");
        source.del("sets4");
        source.del("myaddrs");
        source.del("300");
        source.del("stringmap");
        source.del("hmap");
        source.del("hmapall");
        source.del("hmaplong");
        source.del("hmapstr");
        source.del("hmapstrmap");
        source.del("byteskey");
        source.del("nxexkey1");
        System.out.println("--------###------- 接口测试结束 --------###-------");
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
//        source.setex("bigmap", JsonConvert.TYPE_MAP_STRING_STRING, bigmap);
//        System.out.println("写入完成");
//        for (int i = 0; i < 1; i++) {
//            HashMap<String, String> fs = (HashMap) source.get("bigmap", JsonConvert.TYPE_MAP_STRING_STRING);
//            System.out.println("内容长度: " + fs.get("val").length());
//        }
        source.del("bigmap");
        if (press) {
            int count = Runtime.getRuntime().availableProcessors() * 10;
            System.out.println("------开始进行压力测试 " + count + "个并发------");
            source.del("hmap");
            source.del("testnumber");
            source.hincr("hmap", "key1");
            source.hdecr("hmap", "key1");
            source.hmset("hmap", "key2", "haha", "key3", 333);
            source.hmset("hmap", "sm", (HashMap) Utility.ofMap("a", "aa", "b", "bb"));
            source.hget("hmap", "sm", JsonConvert.TYPE_MAP_STRING_STRING);
            if (source.getLong("testnumber", -1) != -1) {
                System.err.println("testnumber的查询结果应该是 -1，但是得到的值却是:" + source.getLong("testnumber", -1));
            }
            source.setLong("testnumber", 0);
            Thread.sleep(10);
            AtomicInteger ai = new AtomicInteger(count);
            CountDownLatch start = new CountDownLatch(count * 3);
            CountDownLatch over = new CountDownLatch(count * 3);
            long s = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                new Thread() {
                    public void run() {
                        start.countDown();
                        if (ai.decrementAndGet() == 0) {
                            System.out.println("开始了 ");
                        }
                        source.incr("testnumber");
                        over.countDown();
                    }
                }.start();
                new Thread() {
                    public void run() {
                        start.countDown();
                        source.hincr("hmap", "key1");
                        over.countDown();
                    }
                }.start();
                new Thread() {
                    public void run() {
                        start.countDown();
                        source.hget("hmap", "sm", JsonConvert.TYPE_MAP_STRING_STRING);
                        over.countDown();
                    }
                }.start();
            }
            over.await();
            long e = System.currentTimeMillis() - s;
            System.out.println("hmap.key1 = " + source.hgetLong("hmap", "key1", -1));
            System.out.println("结束了, 循环" + count + "次耗时: " + e + "ms");
        }
        source.del("testnumber");
    }

    public static void main(String[] args) throws Exception {
        AnyValue.DefaultAnyValue conf = new AnyValue.DefaultAnyValue().addValue(CACHE_SOURCE_MAXCONNS, "1");
        conf.addValue(CACHE_SOURCE_NODE, new AnyValue.DefaultAnyValue().addValue(CACHE_SOURCE_URL, "redis://127.0.0.1:6363"));
        final ResourceFactory factory = ResourceFactory.create();
        {
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
        {
            System.out.println("############################  开始 Lettuce ############################");
            RedisLettuceCacheSource source = new RedisLettuceCacheSource();
            source.defaultConvert = JsonFactory.root().getConvert();
            source.init(conf);
            try {
                run(source, false);
            } finally {
                source.close();
            }
        }
        {
            System.out.println("############################  开始 Vertx ############################");
            RedisVertxCacheSource source = new RedisVertxCacheSource();
            factory.inject(source);
            source.defaultConvert = JsonFactory.root().getConvert();
            source.init(conf);
            try {
                run(source, false);
            } finally {
                source.close();
            }
        }
        {
            System.out.println("############################  开始 Redission ############################");
            RedissionCacheSource source = new RedissionCacheSource();
            source.defaultConvert = JsonFactory.root().getConvert();
            source.init(conf);
            try {
                run(source, false);
            } finally {
                source.close();
            }
        }
    }
}
