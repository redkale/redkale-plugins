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
import java.util.concurrent.atomic.AtomicInteger;
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
        source.del("stritem1", "stritem2");
        source.setString("stritem1", "value1");
        source.setString("stritem2", "value2");

        List<String> list = source.keysStartsWith("stritem");
        System.out.println("stritem开头的key有两个: " + list);
        Assertions.assertTrue(Utility.equalsElement(list, List.of("stritem2", "stritem1")));

        Map<String, String> map = source.mgetString("stritem1", "stritem2");
        System.out.println("[有值] MGET : " + map);
        Assertions.assertTrue(Utility.equalsElement(map, Utility.ofMap("stritem1", "value1", "stritem2", "value2")));

        String[] array = source.getStringArray("stritem1", "stritem2");
        System.out.println("[有值] MGET : " + Arrays.toString(array));
        Assertions.assertTrue(Utility.equalsElement(array, new String[]{"value1", "value2"}));

        source.del("intitem1", "intitem2");
        source.setLong("intitem1", 333);
        source.setLong("intitem2", 444);

        map = source.mgetString("intitem1", "intitem22", "intitem2");
        System.out.println("[有值] MGET : " + map);
        Assertions.assertTrue(Utility.equalsElement(map, Utility.ofMap("intitem1", "333", "intitem2", "444")));

        array = source.getStringArray("intitem1", "intitem22", "intitem2");
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

        int size = source.scard("sets3");
        System.out.println("sets3 大小 : " + size);
        Assertions.assertEquals(1, size);

        col = source.keys();
        System.out.println("all keys: " + col);

        col = source.keys("key*");
        Collections.sort((List<String>) col);
        System.out.println("key startkeys: " + col);
        Assertions.assertIterableEquals(col, List.of("key1", "keylong1", "keys3", "keystr1"));

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
        Assertions.assertEquals(mapcol.toString(), Utility.ofMap("sets3", List.of("setvals2"), "sets4", List.of("setvals1", "setvals2")).toString());

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
        Assertions.assertEquals(mapcol.toString(), Utility.ofMap("myaddrs", List.of(addr99), "myaddrs2", List.of(addr88, addr99)).toString());

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
        Assertions.assertEquals(map.toString(), Utility.ofMap("a", 1, "b", 2).toString());

        source.del("byteskey");
        source.setBytes("byteskey", new byte[]{1, 2, 3});
        byte[] bs = source.getBytes("byteskey");
        System.out.println("byteskey 值 : " + Arrays.toString(bs));
        Assertions.assertEquals(Arrays.toString(new byte[]{1, 2, 3}), Arrays.toString(bs));
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
        Assertions.assertEquals(map.toString(), Utility.ofMap("a", "aa", "b", "bb").toString());

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

        size = source.hlen("hmap");
        System.out.println("hmap列表(2)大小 : " + size);
        Assertions.assertEquals(2, size);

        source.del("hmaplong");
        source.hincrby("hmaplong", "key1", 10);
        source.hsetLong("hmaplong", "key2", 30);

        Map<String, Long> longmap = source.hmap("hmaplong", long.class, 0, 10);
        System.out.println("hmaplong.所有两值 : " + longmap);
        Assertions.assertEquals(longmap.toString(), Utility.ofMap("key1", 10, "key2", 30).toString());

        source.del("hmapstr");
        source.hsetString("hmapstr", "key1", "str10");
        source.hsetString("hmapstr", "key2", null);
        map = source.hmap("hmapstr", String.class, 0, 10);
        System.out.println("hmapstr.所有一值 : " + map);
        Assertions.assertEquals(map.toString(), Utility.ofMap("key1", "str10").toString());

        source.del("hmapstrmap");
        source.hset("hmapstrmap", "key1", JsonConvert.TYPE_MAP_STRING_STRING, (HashMap) Utility.ofMap("ks11", "vv11"));
        source.hset("hmapstrmap", "key2", JsonConvert.TYPE_MAP_STRING_STRING, null);
        map = source.hmap("hmapstrmap", JsonConvert.TYPE_MAP_STRING_STRING, 0, 10, "key2*");
        System.out.println("hmapstrmap.无值 : " + map);
        Assertions.assertEquals(map.toString(), Utility.ofMap().toString());

        source.del("popset");
        source.saddString("popset", "111");
        source.saddString("popset", "222");
        source.saddString("popset", "333");
        source.saddString("popset", "444");
        source.saddString("popset", "555");

        obj = source.spopString("popset");
        System.out.println("SPOP一个元素：" + obj);
        Assertions.assertTrue(List.of("111", "222", "333", "444", "555").contains(obj));

        col = source.spopString("popset", 2);
        System.out.println("SPOP两个元素：" + col);
        System.out.println("SPOP五个元素：" + source.spopString("popset", 5));

        source.saddLong("popset", 111);
        source.saddLong("popset", 222);
        source.saddLong("popset", 333);
        source.saddLong("popset", 444);
        source.saddLong("popset", 555);
        System.out.println("SPOP一个元素：" + source.spopLong("popset"));
        System.out.println("SPOP两个元素：" + source.spopLong("popset", 2));
        System.out.println("SPOP五个元素：" + source.spopLong("popset", 5));
        System.out.println("SPOP一个元素：" + source.spopLong("popset"));

        long dbsize = source.dbsize();
        System.out.println("keys总数量 : " + dbsize);
        //清除
        int rs = source.del("stritem1");
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
        source.del("sets3");
        source.del("sets4");
        source.del("myaddrs");
        source.del("300");
        source.del("stringmap");
        source.del("hmap");
        source.del("hmaplong");
        source.del("hmapstr");
        source.del("hmapstrmap");
        source.del("byteskey");
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
//        source.setex("bigmap", JsonConvert.TYPE_MAP_STRING_STRING, bigmap);
//        System.out.println("写入完成");
//        for (int i = 0; i < 1; i++) {
//            HashMap<String, String> fs = (HashMap) source.get("bigmap", JsonConvert.TYPE_MAP_STRING_STRING);
//            System.out.println("内容长度: " + fs.get("val").length());
//        }
        source.del("bigmap");
        {
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
    }
}
