/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import org.redkale.convert.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class ProtobufReader extends Reader {

    protected int position = -1;

    private byte[] content;

    public static ObjectPool<ProtobufReader> createPool(int max) {
        return new ObjectPool<>(max, (Object... params) -> new ProtobufReader(), null, (t) -> t.recycle());
    }

    public ProtobufReader() {
    }

    public ProtobufReader(byte[] bytes) {
        setBytes(bytes, 0, bytes.length);
    }

    public ProtobufReader(byte[] bytes, int start, int len) {
        setBytes(bytes, start, len);
    }

    public final void setBytes(byte[] bytes) {
        if (bytes == null) {
            this.position = 0;
        } else {
            setBytes(bytes, 0, bytes.length);
        }
    }

    public final void setBytes(byte[] bytes, int start, int len) {
        if (bytes == null) {
            this.position = 0;
        } else {
            this.content = bytes;
            this.position = start - 1;
        }
    }

    protected boolean recycle() {
        this.position = -1;
        this.content = null;
        return true;
    }

    public void close() {
        this.recycle();
    }

    /**
     * 跳过属性的值
     */
    @Override
    @SuppressWarnings("unchecked")
    public final void skipValue() {

    }

    @Override
    public final String readObjectB(final Class clazz) {
        return null;
    }

    @Override
    public final void readObjectE(final Class clazz) {

    }

    protected byte currentByte() {
        return this.content[this.position];
    }

    @Override
    public final int readMapB() {
        return readArrayB();
    }

    @Override
    public final void readMapE() {
    }

    /**
     * 判断下一个非空白字节是否为[
     *
     * @return 数组长度或SIGN_NULL
     */
    @Override
    public int readArrayB() {
        short bt = readShort();
        if (bt == Reader.SIGN_NULL) return bt;
        return (bt & 0xffff) << 16 | ((content[++this.position] & 0xff) << 8) | (content[++this.position] & 0xff);
    }

    @Override
    public final void readArrayE() {
    }

    /**
     * 判断下一个非空白字节是否:
     */
    @Override
    public final void readBlank() {
    }

    /**
     * 判断对象是否存在下一个属性或者数组是否存在下一个元素
     *
     * @return 是否存在
     */
    @Override
    public final boolean hasNext() {
        return this.position < this.content.length - 1;
    }

    @Override
    public final DeMember readFieldName(final DeMember[] members) {
        final String exceptedfield = readSmallString();
        int typeval = readByte();
        final int len = members.length;
        if (this.fieldIndex >= len) this.fieldIndex = 0;
        for (int k = this.fieldIndex; k < len; k++) {
            if (exceptedfield.equals(members[k].getAttribute().field())) {
                this.fieldIndex = k;
                return members[k];
            }
        }
        for (int k = 0; k < this.fieldIndex; k++) {
            if (exceptedfield.equals(members[k].getAttribute().field())) {
                this.fieldIndex = k;
                return members[k];
            }
        }
        return null;
    }

    //------------------------------------------------------------
    @Override
    public boolean readBoolean() {
        return content[++this.position] == 1;
    }

    @Override
    public byte readByte() {
        return content[++this.position];
    }

    @Override
    public final byte[] readByteArray() {
        int len = readArrayB();
        if (len == Reader.SIGN_NULL) return null;
        if (len == Reader.SIGN_NOLENGTH) {
            int size = 0;
            byte[] data = new byte[8];
            while (hasNext()) {
                if (size >= data.length) {
                    byte[] newdata = new byte[data.length + 4];
                    System.arraycopy(data, 0, newdata, 0, size);
                    data = newdata;
                }
                data[size++] = readByte();
            }
            readArrayE();
            byte[] newdata = new byte[size];
            System.arraycopy(data, 0, newdata, 0, size);
            return newdata;
        } else {
            byte[] values = new byte[len];
            for (int i = 0; i < values.length; i++) {
                values[i] = readByte();
            }
            readArrayE();
            return values;
        }
    }

    @Override
    public char readChar() {
        return (char) ((0xff00 & (content[++this.position] << 8)) | (0xff & content[++this.position]));
    }

    @Override
    public short readShort() {
        return (short) ((0xff00 & (content[++this.position] << 8)) | (0xff & content[++this.position]));
    }

    @Override
    public int readInt() {
        return ((content[++this.position] & 0xff) << 24) | ((content[++this.position] & 0xff) << 16)
            | ((content[++this.position] & 0xff) << 8) | (content[++this.position] & 0xff);
    }

    @Override
    public long readLong() {
        return ((((long) content[++this.position] & 0xff) << 56)
            | (((long) content[++this.position] & 0xff) << 48)
            | (((long) content[++this.position] & 0xff) << 40)
            | (((long) content[++this.position] & 0xff) << 32)
            | (((long) content[++this.position] & 0xff) << 24)
            | (((long) content[++this.position] & 0xff) << 16)
            | (((long) content[++this.position] & 0xff) << 8)
            | (((long) content[++this.position] & 0xff)));
    }

    @Override
    public final float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public final double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public final String readClassName() {
        return readSmallString();
    }

    @Override
    public String readSmallString() {
        int len = 0xff & readByte();
        if (len == 0) return "";
        String value = new String(content, ++this.position, len);
        this.position += len - 1; //上一行已经++this.position，所以此处要-1
        return value;
    }

    @Override
    public String readString() {
        int len = readInt();
        if (len == SIGN_NULL) return null;
        if (len == 0) return "";
        String value = new String(Utility.decodeUTF8(content, ++this.position, len));
        this.position += len - 1;//上一行已经++this.position，所以此处要-1
        return value;
    }

    protected int readRawVarint32() {
//    fastpath:
//    {
//        long tempPos = currentByteBufferPos;
//
//        if (currentByteBufferLimit == currentByteBufferPos) {
//            break fastpath;
//        }
//
//        int x;
//        if ((x = UnsafeUtil.getByte(tempPos++)) >= 0) {
//            currentByteBufferPos++;
//            return x;
//        } else if (currentByteBufferLimit - currentByteBufferPos < 10) {
//            break fastpath;
//        } else if ((x ^= (UnsafeUtil.getByte(tempPos++) << 7)) < 0) {
//            x ^= (~0 << 7);
//        } else if ((x ^= (UnsafeUtil.getByte(tempPos++) << 14)) >= 0) {
//            x ^= (~0 << 7) ^ (~0 << 14);
//        } else if ((x ^= (UnsafeUtil.getByte(tempPos++) << 21)) < 0) {
//            x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21);
//        } else {
//            int y = UnsafeUtil.getByte(tempPos++);
//            x ^= y << 28;
//            x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21) ^ (~0 << 28);
//            if (y < 0
//                && UnsafeUtil.getByte(tempPos++) < 0
//                && UnsafeUtil.getByte(tempPos++) < 0
//                && UnsafeUtil.getByte(tempPos++) < 0
//                && UnsafeUtil.getByte(tempPos++) < 0
//                && UnsafeUtil.getByte(tempPos++) < 0) {
//                break fastpath; // Will throw malformedVarint()
//            }
//        }
//        currentByteBufferPos = tempPos;
//        return x;
//    }
        return (int) readRawVarint64SlowPath();
    }

    protected long readRawVarint64SlowPath() {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            final byte b = readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new ConvertException("readRawVarint64SlowPath error");
    }
}
