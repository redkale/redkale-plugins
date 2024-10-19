package org.redkalex.convert.pb;

import java.util.Map;
import org.redkale.convert.ConvertColumn;
import org.redkale.convert.json.JsonConvert;

/**
 *
 * @author zhangjx
 */
public class PTestBeanSelf {

    public static class PTestEntry {

        @ConvertColumn(index = 1)
        public boolean[] bools = new boolean[] {true, false, true};

        @ConvertColumn(index = 2)
        public byte[] bytes = new byte[] {1, 2, 3, 4};

        @ConvertColumn(index = 3)
        public char[] chars = new char[] {'A', 'B', 'C'};

        @ConvertColumn(index = 4)
        public short[] shorts = new short[] {10, 20, 30};

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
    public boolean[] bools;

    @ConvertColumn(index = 2)
    public byte[] bytes;

    @ConvertColumn(index = 3)
    public char[] chars;

    @ConvertColumn(index = 4)
    public PTestEntry[] entrys;

    @ConvertColumn(index = 5)
    public int[] ints;

    @ConvertColumn(index = 6)
    public float[] floats;

    @ConvertColumn(index = 7)
    public long[] longs;

    @ConvertColumn(index = 8)
    public double[] doubles; // 8

    @ConvertColumn(index = 9)
    public String[] strings; // 9

    @ConvertColumn(index = 10)
    public int id = 0x7788; // 10

    @ConvertColumn(index = 11)
    public String name; // 11

    @ConvertColumn(index = 12)
    public String email; // 12

    @ConvertColumn(index = 13)
    public Kind kind; // 13

    @ConvertColumn(index = 14)
    public Map<String, Integer> map; // 14

    @ConvertColumn(index = 15)
    public String end; // 15

    @Override
    public String toString() {
        return JsonConvert.root().convertTo(this);
    }


}

// protoc --java_out=D:\Java-Projects\RedkalePluginProject\src\test\java
// --proto_path=D:\Java-Projects\RedkalePluginProject\src\test\java\org\redkalex\convert\pb PTestBean.proto
