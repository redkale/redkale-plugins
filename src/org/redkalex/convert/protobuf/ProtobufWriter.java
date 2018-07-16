/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import java.nio.ByteBuffer;
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
    public void writeObjectB(Object obj) {
        super.writeObjectB(obj);
    }

    @Override
    public void writeObjectE(Object obj) {
    }

    @Override
    public void writeArrayB(int size) {
    }

    @Override
    public void writeArrayMark() {
    }

    @Override
    public void writeArrayE() {
    }

    @Override
    public void writeMapB(int size) {
    }

    @Override
    public void writeMapMark() {
    }

    @Override
    public void writeMapE() {
    }

    @Override
    public void writeFieldName(EnMember member) {
        Attribute attribute = member.getAttribute();
        int wiretype = ProtobufFactory.wireType(attribute.type());
        writeUInt32(member.getPosition() << 3 | wiretype);
    }

    public static void main(String[] args) throws Throwable {
        System.out.println(1 << 3);
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
        writeArrayB(values.length);
        boolean flag = false;
        for (byte v : values) {
            if (flag) writeArrayMark();
            writeByte(v);
            flag = true;
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
