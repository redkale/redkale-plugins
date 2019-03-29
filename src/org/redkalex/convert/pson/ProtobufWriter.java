/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.pson;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Stream;
import org.redkale.convert.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class ProtobufWriter extends Writer {

    private static final int defaultSize = Integer.getInteger("convert.protobuf.writer.buffer.defsize", 1024);

    private byte[] content;

    protected int count;

    protected boolean tiny;

    public static ObjectPool<ProtobufWriter> createPool(int max) {
        return new ObjectPool<>(max, (Object... params) -> new ProtobufWriter(), null, (t) -> t.recycle());
    }

    protected ProtobufWriter(byte[] bs) {
        this.content = bs;
    }

    public ProtobufWriter() {
        this(defaultSize);
    }

    public ProtobufWriter(int size) {
        this.content = new byte[size > 128 ? size : 128];
    }

    public ByteBuffer[] toBuffers() {
        return new ByteBuffer[]{ByteBuffer.wrap(content, 0, count)};
    }

    public byte[] toArray() {
        if (count == content.length) return content;
        byte[] newdata = new byte[count];
        System.arraycopy(content, 0, newdata, 0, count);
        return newdata;
    }

    @Override
    public final boolean tiny() {
        return tiny;
    }

    public ProtobufWriter tiny(boolean tiny) {
        this.tiny = tiny;
        return this;
    }

    protected int expand(int len) {
        int newcount = count + len;
        if (newcount <= content.length) return 0;
        byte[] newdata = new byte[Math.max(content.length * 3 / 2, newcount)];
        System.arraycopy(content, 0, newdata, 0, count);
        this.content = newdata;
        return 0;
    }

    public void writeTo(final byte ch) {
        expand(1);
        content[count++] = ch;
    }

    public final void writeTo(final byte... chs) {
        writeTo(chs, 0, chs.length);
    }

    public void writeTo(final byte[] chs, final int start, final int len) {
        expand(len);
        System.arraycopy(chs, start, content, count, len);
        count += len;
    }

    protected boolean recycle() {
        this.count = 0;
        this.specify = null;
        if (this.content.length > defaultSize) {
            this.content = new byte[defaultSize];
        }
        return true;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[count=" + this.count + "]";
    }

    //------------------------------------------------------------------------
    public final int count() {
        return this.count;
    }

    @Override
    public final void writeBoolean(boolean value) {
        writeTo(value ? (byte) 1 : (byte) 0);
    }

    @Override
    public void writeNull() {
    }

    @Override
    public boolean needWriteClassName() {
        return false;
    }

    @Override
    public void writeClassName(String clazz) {
    }

    @Override
    public int writeObjectB(Object obj) {
        super.writeObjectB(obj);
        return -1;
    }

    @Override
    public void writeObjectE(Object obj) {
    }

    @Override
    public int writeArrayB(int size, Encodeable<Writer, Object> encoder, Object obj) {
        if (obj == null) {
            writeNull();
            return 0;
        } else if (size < 1) {
            writeUInt32(0);
            return 0;
        } else if (obj instanceof byte[]) {
            int length = ((byte[]) obj).length;
            writeUInt32(length);
            writeTo((byte[]) obj);
            return length;
        } else {
            final Class type = obj.getClass();
            ProtobufWriter tmp = new ProtobufWriter();
            if (type == boolean[].class) {
                for (boolean item : (boolean[]) obj) {
                    tmp.writeBoolean(item);
                }
            } else if (type == Boolean[].class) {
                for (Boolean item : (Boolean[]) obj) {
                    tmp.writeBoolean(item == null ? false : item);
                }
            } else if (type == short[].class) {
                for (short item : (short[]) obj) {
                    tmp.writeShort(item);
                }
            } else if (type == Short[].class) {
                for (Short item : (Short[]) obj) {
                    tmp.writeShort(item == null ? 0 : item);
                }
            } else if (type == char[].class) {
                for (char item : (char[]) obj) {
                    tmp.writeChar(item);
                }
            } else if (type == Character[].class) {
                for (Character item : (Character[]) obj) {
                    tmp.writeChar(item == null ? 0 : item);
                }
            } else if (type == int[].class) {
                for (int item : (int[]) obj) {
                    tmp.writeInt(item);
                }
            } else if (type == Integer[].class) {
                for (Integer item : (Integer[]) obj) {
                    tmp.writeInt(item == null ? 0 : item);
                }
            } else if (type == float[].class) {
                for (float item : (float[]) obj) {
                    tmp.writeFloat(item);
                }
            } else if (type == Float[].class) {
                for (Float item : (Float[]) obj) {
                    tmp.writeFloat(item == null ? 0F : item);
                }
            } else if (type == long[].class) {
                for (long item : (long[]) obj) {
                    tmp.writeLong(item);
                }
            } else if (type == Long[].class) {
                for (Long item : (Long[]) obj) {
                    tmp.writeLong(item == null ? 0L : item);
                }
            } else if (type == double[].class) {
                for (double item : (double[]) obj) {
                    tmp.writeDouble(item);
                }
            } else if (type == Double[].class) {
                for (Double item : (Double[]) obj) {
                    tmp.writeDouble(item == null ? 0D : item);
                }
            } else {
                return -1;
            }
            int length = tmp.count();
            writeUInt32(length);
            writeTo(tmp.toArray());
            return length;
        }
    }

    @Override
    public void writeArrayMark() {
    }

    @Override
    public void writeArrayE() {
    }

    @Override
    public int writeMapB(int size, Encodeable<Writer, Object> keyEncoder, Encodeable<Writer, Object> valueEncoder, Object obj) {
        return -1;
    }

    @Override
    public void writeMapMark() {
    }

    @Override
    public void writeMapE() {
    }

    @Override
    public void writeFieldName(EnMember member) {
        writeUInt32(ProtobufFactory.getTag(member));
    }

    @Override
    public void writeObjectField(final EnMember member, Object obj) {
        Object value = member.getAttribute().get(obj);
        if (value == null) return;
        Encodeable encoder = member.getEncoder();
        if (encoder instanceof MapEncoder) {
            ((MapEncoder) encoder).convertTo(this, member, (Map) value);
        } else if (encoder instanceof ArrayEncoder) {
            ((ArrayEncoder) encoder).convertTo(this, member, (Object[]) value);
        } else if (encoder instanceof CollectionEncoder) {
            ((CollectionEncoder) encoder).convertTo(this, member, (Collection) value);
        } else if (encoder instanceof StreamEncoder) {
            ((StreamEncoder) encoder).convertTo(this, member, (Stream) value);
        } else {
            this.writeFieldName(member);
            encoder.convertTo(this, value);
        }
        this.comma = true;
    }

    @Override
    public void writeByte(byte value) {
        writeInt(value);
    }

    @Override
    public final void writeByteArray(byte[] values) {
        if (values == null) {
            writeNull();
            return;
        }
        if (writeArrayB(values.length, null, values) < 0) {
            boolean flag = false;
            for (byte v : values) {
                if (flag) writeArrayMark();
                writeByte(v);
                flag = true;
            }
        }
        writeArrayE();
    }

    @Override
    public void writeChar(char value) {
        writeInt(value);
    }

    @Override
    public void writeShort(short value) {
        writeInt(value);
    }

    @Override
    public void writeInt(int value) { //writeSInt32  
        writeUInt32((value << 1) ^ (value >> 31));
    }

    @Override
    public void writeLong(long value) { //writeSInt64
        writeUInt64((value << 1) ^ (value >> 63));
    }

    @Override
    public void writeFloat(float value) {
        writeFixed32(Float.floatToRawIntBits(value));
    }

    @Override
    public void writeDouble(double value) {
        writeFixed64(Double.doubleToRawLongBits(value));
    }

    @Override
    public void writeSmallString(String value) {
        writeString(value);
    }

    @Override
    public void writeString(String value) {
        byte[] bs = Utility.encodeUTF8(value);
        writeUInt32(bs.length);
        writeTo(bs);
    }

    @Override
    public void writeWrapper(StringConvertWrapper value) {
        if (value != null) writeString(value.getValue());
    }

    protected void writeUInt32(int value) {
        while (true) {
            if ((value & ~0x7F) == 0) {
                writeTo((byte) value);
                return;
            } else {
                writeTo((byte) ((value & 0x7F) | 0x80));
                value >>>= 7;
            }
        }
    }

    protected void writeUInt64(long value) {
        while (true) {
            if ((value & ~0x7FL) == 0) {
                writeTo((byte) value);
                return;
            } else {
                writeTo((byte) (((int) value & 0x7F) | 0x80));
                value >>>= 7;
            }
        }
    }

    protected void writeFixed32(int value) {
        writeTo((byte) (value & 0xFF), (byte) ((value >> 8) & 0xFF), (byte) ((value >> 16) & 0xFF), (byte) ((value >> 24) & 0xFF));
    }

    protected void writeFixed64(long value) {
        writeTo((byte) ((int) (value) & 0xFF),
            (byte) ((int) (value >> 8) & 0xFF),
            (byte) ((int) (value >> 16) & 0xFF),
            (byte) ((int) (value >> 24) & 0xFF),
            (byte) ((int) (value >> 32) & 0xFF),
            (byte) ((int) (value >> 40) & 0xFF),
            (byte) ((int) (value >> 48) & 0xFF),
            (byte) ((int) (value >> 56) & 0xFF));
    }
}
