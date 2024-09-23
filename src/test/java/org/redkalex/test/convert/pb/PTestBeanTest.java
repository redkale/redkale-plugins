/**
 */
package org.redkalex.test.convert.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.redkale.convert.json.JsonConvert;
import org.redkale.convert.pb.ProtobufConvert;
import org.redkale.convert.pb.ProtobufReader;
import org.redkale.service.RetResult;
import org.redkale.util.TypeToken;
import org.redkale.util.Utility;

/**
 *
 * @author zhangjx
 */
public class PTestBeanTest {

    public static void main(String[] args) throws Throwable {
        PTestBeanTest test = new PTestBeanTest();
        test.run1();
        test.run2();
        test.run3();
    }

    @Test
    public void run1() throws Exception {
        System.setProperty("convert.protobuf.enumtostring", "false"); // 禁用枚举按字符串类型出来
        // System.out.println(ProtobufConvert.root().getProtoDescriptor(PTestBeanSelf.class));
        // System.out.println(Integer.toHexString(14<<3|2));
        PTestBeanSelf bean = new PTestBeanSelf();

        bean.bools = new boolean[] {true, false, true};
        bean.bytes = new byte[] {1, 2, 3, 4};
        bean.chars = new char[] {'A', 'B', 'C'};
        bean.ints = new int[] {100, 200, 300};
        bean.floats = new float[] {10.12f, 20.34f};
        bean.longs = new long[] {111, 222, 333};
        bean.doubles = new double[] {65.65, 78.78};
        bean.name = "redkale";
        bean.email = "redkale@qq.org";
        bean.kind = PTestBeanSelf.Kind.TWO;
        bean.strings = new String[] {"str1", "str2", "str3"};
        bean.entrys =
                new PTestBeanSelf.PTestEntry[] {new PTestBeanSelf.PTestEntry(), null, new PTestBeanSelf.PTestEntry()};
        bean.map = Utility.ofMap("aa", 0x55, "bb", 0x66);
        bean.end = "over";

        // -------------------------------
        byte[] jsonbs = JsonConvert.root().convertToBytes(bean);
        byte[] bs = ProtobufConvert.root().convertTo(bean);
        Utility.println("pbconvert ", bs);
        PTestBeanOuterClass.PTestBean.Builder builder = PTestBeanOuterClass.PTestBean.newBuilder();

        PTestBeanOuterClass.PTestBean bean2 = createPTestBean(bean, builder);
        byte[] bs2 = bean2.toByteArray();
        Utility.println("proto-buf ", bs2);
        Utility.sleep(10);
        if (!Arrays.equals(bs, bs2)) throw new RuntimeException("两者序列化出来的byte[]不一致");

        System.out.println(bean);
        String frombean =
                ProtobufConvert.root().convertFrom(PTestBeanSelf.class, bs).toString();
        System.out.println(frombean);
        if (!bean.toString().equals(frombean)) throw new RuntimeException("ProtobufConvert反解析后的结果不正确");
        System.out.println(
                JsonConvert.root().convertFrom(PTestBeanSelf.class, jsonbs).toString());

        int count = 100000;
        long s, e;
        s = System.currentTimeMillis();
        for (int z = 0; z < count; z++) {
            ProtobufConvert.root().convertTo(bean);
        }
        e = System.currentTimeMillis() - s;
        System.out.println("redkale-protobuf耗时-------" + e);

        s = System.currentTimeMillis();
        for (int z = 0; z < count; z++) {
            JsonConvert.root().convertToBytes(bean);
        }
        e = System.currentTimeMillis() - s;
        System.out.println("redkale-json文本耗时-------" + e);

        s = System.currentTimeMillis();
        for (int z = 0; z < count; z++) {
            createPTestBean(bean, builder).toByteArray();
        }
        e = System.currentTimeMillis() - s;
        System.out.println("原生编译protobuf耗时-------" + e);
    }

    @Test
    public void run2() throws Exception {
        byte[] src = new byte[] {(byte) 0x82, (byte) 0x01, (byte) 0x84, (byte) 0x01, (byte) 0x86, (byte) 0x01};
        src = new byte[] {(byte) 0x01, (byte) 0x00, (byte) 0x01, (byte) 0x01};
        CodedInputStream input = CodedInputStream.newInstance(src);
        int v11 = input.readSInt32();
        int v12 = input.readSInt32();
        int v13 = input.readSInt32();
        System.out.println("结果1： " + v11);
        System.out.println("结果1： " + v12);
        System.out.println("结果1： " + v13);
        ProtobufReader reader = new ProtobufReader(src);
        int v21 = reader.readInt();
        int v22 = reader.readInt();
        int v23 = reader.readInt();
        System.out.println("结果2： " + v21);
        System.out.println("结果2： " + v22);
        System.out.println("结果2： " + v23);
        Assertions.assertEquals(v11, v21);
        Assertions.assertEquals(v12, v22);
        Assertions.assertEquals(v13, v23);
    }

    @Test
    public void run3() throws Exception {
        System.out.println(ProtobufConvert.root().getProtoDescriptor(retstring));
    }

    private static PTestBeanOuterClass.PTestBean createPTestBean(
            PTestBeanSelf bean, PTestBeanOuterClass.PTestBean.Builder builder) {
        if (builder == null) {
            builder = PTestBeanOuterClass.PTestBean.newBuilder();
        } else {
            builder.clear();
        }
        for (int i = 0; bean.bools != null && i < bean.bools.length; i++) {
            builder.addBools(bean.bools[i]);
        }
        if (bean.bytes != null) builder.addBytes(ByteString.copyFrom(bean.bytes));
        for (int i = 0; bean.chars != null && i < bean.chars.length; i++) {
            builder.addChars(bean.chars[i]);
        }
        for (int i = 0; bean.entrys != null && i < bean.entrys.length; i++) {
            PTestBeanOuterClass.PTestBean.PTestEntry.Builder entry =
                    PTestBeanOuterClass.PTestBean.PTestEntry.newBuilder();
            if (bean.entrys[i] == null) {
                builder.addEntrys(entry.build());
                continue;
            }
            for (int j = 0; bean.entrys[i].bools != null && j < bean.entrys[i].bools.length; j++) {
                entry.addBools(bean.entrys[i].bools[j]);
            }
            if (bean.entrys[i].bytes != null) entry.addBytes(ByteString.copyFrom(bean.entrys[i].bytes));
            for (int j = 0; bean.entrys[i].chars != null && j < bean.entrys[i].chars.length; j++) {
                entry.addChars(bean.entrys[i].chars[j]);
            }
            for (int j = 0; bean.entrys[i].shorts != null && j < bean.entrys[i].shorts.length; j++) {
                entry.addShorts(bean.entrys[i].shorts[j]);
            }
            builder.addEntrys(entry.build());
        }
        for (int i = 0; bean.ints != null && i < bean.ints.length; i++) {
            builder.addInts(bean.ints[i]);
        }
        for (int i = 0; bean.floats != null && i < bean.floats.length; i++) {
            builder.addFloats(bean.floats[i]);
        }
        for (int i = 0; bean.longs != null && i < bean.longs.length; i++) {
            builder.addLongs(bean.longs[i]);
        }
        for (int i = 0; bean.doubles != null && i < bean.doubles.length; i++) {
            builder.addDoubles(bean.doubles[i]);
        }
        for (int i = 0; bean.strings != null && i < bean.strings.length; i++) {
            builder.addStrings(bean.strings[i]);
        }
        builder.setId(bean.id);
        if (bean.name != null) builder.setName(bean.name);
        if (bean.email != null) builder.setEmail(bean.email);
        if (bean.kind != null) builder.setKind(PTestBeanOuterClass.PTestBean.Kind.TWO);
        if (bean.map != null) builder.putAllMap(bean.map);
        if (bean.end != null) builder.setEnd(bean.end);
        PTestBeanOuterClass.PTestBean bean2 = builder.build();
        return bean2;
    }

    private static java.lang.reflect.Type retstring = new TypeToken<RetResult<Map<String, String>>>() {}.getType();
}
