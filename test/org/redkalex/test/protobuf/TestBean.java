/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.test.protobuf;

import com.google.protobuf.ByteString;
import java.util.Map;
import org.redkale.convert.ConvertColumn;
import org.redkale.convert.json.JsonConvert;
import org.redkale.util.Utility;
import org.redkalex.convert.pson.ProtobufConvert;

/**
 *
 * @author zhangjx
 */
public class TestBean {

    public static class PTestEntry {

        @ConvertColumn(index = 1)
        public boolean[] bools = new boolean[]{true, false, true};

        @ConvertColumn(index = 2)
        public byte[] bytes = new byte[]{1, 2, 3, 4};

        @ConvertColumn(index = 3)
        public char[] chars = new char[]{'A', 'B', 'C'};

        @ConvertColumn(index = 4)
        public short[] shorts = new short[]{10, 20, 30};

        @Override
        public String toString() {
            return JsonConvert.root().convertTo(this);
        }
    }

    public static enum Kind {
        ONE,
        TWO,
        THREE
    }

    @ConvertColumn(index = 1)
    public boolean[] bools = new boolean[]{true, false, true};

    @ConvertColumn(index = 2)
    public byte[] bytes = new byte[]{1, 2, 3, 4};

    @ConvertColumn(index = 3)
    public char[] chars = new char[]{'A', 'B', 'C'};

    @ConvertColumn(index = 4)
    public PTestEntry[] entrys = new PTestEntry[]{new PTestEntry(), new PTestEntry()};

    @ConvertColumn(index = 5)
    public int[] ints = new int[]{100, 200, 300};

    @ConvertColumn(index = 6)
    public float[] floats = new float[]{10.12f, 20.34f};

    @ConvertColumn(index = 7)
    public long[] longs = new long[]{111, 222, 333};

    @ConvertColumn(index = 8)
    public double[] doubles = new double[]{65.65, 78.78}; //8

    @ConvertColumn(index = 9)
    public String[] strings = new String[]{"str1", "str2", "str3"}; //9

    @ConvertColumn(index = 10)
    public int id = 0x7788; //10

    @ConvertColumn(index = 11)
    public String name = "redkale"; //11

    @ConvertColumn(index = 12)
    public String email = "redkale@qq.org"; //12

    @ConvertColumn(index = 13)
    public Kind kind = Kind.TWO;  //13

    @ConvertColumn(index = 14)
    public Map<String, Integer> map = Utility.ofMap("aa", 0x55, "bb", 0x66); //14

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }

    public static void main(String[] args) throws Throwable {
        System.out.println(ProtobufConvert.root().getProtoDescriptor(TestBean.class));
        System.out.println(Integer.toHexString(5<<3|2));
        TestBean bean = new TestBean();
        bean.bools = null;
        bean.bytes = null;
        bean.chars = null;
        //bean.entrys = null;
        //bean.ints = null;
        bean.floats = null;
        bean.longs = null;
        bean.doubles = null;
        //bean.strings = null;
        //bean.name = null;
        bean.email = null;
        bean.kind = null;
        bean.map = null;
        System.out.println(bean);
        byte[] bs = ProtobufConvert.root().convertTo(bean);
        Utility.println("convert-", bs);
        PTestBeanOuterClass.PTestBean.Builder builder = PTestBeanOuterClass.PTestBean.newBuilder();

        for (int i = 0; bean.bools != null && i < bean.bools.length; i++) {
            builder.addBools(bean.bools[i]);
        }
        if(bean.bytes != null)   builder.addBytes(ByteString.copyFrom(bean.bytes));
        for (int i = 0; bean.chars != null && i < bean.chars.length; i++) {
            builder.addChars(bean.chars[i]);
        }
        for (int i = 0; bean.entrys != null && i < bean.entrys.length; i++) {
            PTestBeanOuterClass.PTestBean.PTestEntry.Builder entry = PTestBeanOuterClass.PTestBean.PTestEntry.newBuilder();
            for (int j = 0; bean.entrys[i].bools != null && j < bean.entrys[i].bools.length; j++) {
                entry.addBools(bean.entrys[i].bools[j]);
            }
            if(bean.entrys[i].bytes != null)   entry.addBytes(ByteString.copyFrom(bean.entrys[i].bytes));
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
        PTestBeanOuterClass.PTestBean bean2 = builder.build();
        Utility.println("protobuf", bean2.toByteArray());
        //集合判断hasNext有问题，还未解决
        System.out.println(ProtobufConvert.root().convertFrom(TestBean.class, bs).toString());
    }
}

//protoc --java_out=D:\Java-Projects\RedkalePluginsProject\test\ --proto_path=D:\Java-Projects\RedkalePluginsProject\test\org\redkalex\test\protobuf\ PTestBean.proto
