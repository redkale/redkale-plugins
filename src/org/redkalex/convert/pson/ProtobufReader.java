/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.pson;

import java.nio.charset.StandardCharsets;
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
        return "";
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
    public final int readArrayB() {
        return Reader.SIGN_NOLENGTH;
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
        int tag = readRawVarint32() >>> 3;
        for (DeMember member : members) {
            if (member.getPosition() == tag) {
                return member;
            }
        }
        return null;
    }

    //------------------------------------------------------------
    @Override
    public final boolean readBoolean() {
        return readRawVarint64() != 0;
    }

    @Override
    public byte readByte() {
        return (byte) readInt();
    }

    @Override
    public final byte[] readByteArray() {
        final int size = readRawVarint32();
        byte[] bs = new byte[size];
        System.arraycopy(content, position + 1, bs, 0, size);
        position += size;
        return bs;
    }

    @Override
    public final char readChar() {
        return (char) readInt();
    }

    @Override
    public final short readShort() {
        return (short) readInt();
    }

    @Override
    public final int readInt() { //readSInt32
        int n = readRawVarint32();
        return (n >>> 1) ^ -(n & 1);
    }

    @Override
    public final long readLong() { //readSInt64
        long n = readRawVarint64();
        return (n >>> 1) ^ -(n & 1);
    }

    @Override
    public final float readFloat() {
        return Float.intBitsToFloat(readRawLittleEndian32());
    }

    @Override
    public final double readDouble() {
        return Double.longBitsToDouble(readRawLittleEndian64());
    }

    @Override
    public final String readClassName() {
        return "";
    }

    @Override
    public final String readSmallString() {
        return readString();
    }

    @Override
    public final String readString() {
        return new String(readByteArray(), StandardCharsets.UTF_8);
    }

    protected int readRawVarint32() {
        fastpath:
        {
            if (this.position == content.length - 1) break fastpath;
            int x;
            if ((x = content[++this.position]) >= 0) {
                return x;
            } else if (content.length - this.position < 10) {
                break fastpath;
            } else if ((x ^= (content[++this.position] << 7)) < 0) {
                x ^= (~0 << 7);
            } else if ((x ^= (content[++this.position] << 14)) >= 0) {
                x ^= (~0 << 7) ^ (~0 << 14);
            } else if ((x ^= (content[++this.position] << 21)) < 0) {
                x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21);
            } else {
                int y = content[++this.position];
                x ^= y << 28;
                x ^= (~0 << 7) ^ (~0 << 14) ^ (~0 << 21) ^ (~0 << 28);
                if (y < 0
                    && content[++this.position] < 0
                    && content[++this.position] < 0
                    && content[++this.position] < 0
                    && content[++this.position] < 0
                    && content[++this.position] < 0) {
                    break fastpath; // Will throw malformedVarint()
                }
            }
            return x;
        }
        return (int) readRawVarint64SlowPath();
    }

    protected long readRawVarint64() {
        fastpath:
        {

            if (this.position == content.length - 1) break fastpath;

            long x;
            int y;
            if ((y = content[++this.position]) >= 0) {
                return y;
            } else if (content.length - this.position < 9) {
                break fastpath;
            } else if ((y ^= (content[++this.position] << 7)) < 0) {
                x = y ^ (~0 << 7);
            } else if ((y ^= (content[++this.position] << 14)) >= 0) {
                x = y ^ ((~0 << 7) ^ (~0 << 14));
            } else if ((y ^= (content[++this.position] << 21)) < 0) {
                x = y ^ ((~0 << 7) ^ (~0 << 14) ^ (~0 << 21));
            } else if ((x = y ^ ((long) content[++this.position] << 28)) >= 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
            } else if ((x ^= ((long) content[++this.position] << 35)) < 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
            } else if ((x ^= ((long) content[++this.position] << 42)) >= 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42);
            } else if ((x ^= ((long) content[++this.position] << 49)) < 0L) {
                x ^= (~0L << 7)
                    ^ (~0L << 14)
                    ^ (~0L << 21)
                    ^ (~0L << 28)
                    ^ (~0L << 35)
                    ^ (~0L << 42)
                    ^ (~0L << 49);
            } else {
                x ^= ((long) content[++this.position] << 56);
                x ^= (~0L << 7)
                    ^ (~0L << 14)
                    ^ (~0L << 21)
                    ^ (~0L << 28)
                    ^ (~0L << 35)
                    ^ (~0L << 42)
                    ^ (~0L << 49)
                    ^ (~0L << 56);
                if (x < 0L) {
                    if (content[++this.position] < 0L) {
                        break fastpath; // Will throw malformedVarint()
                    }
                }
            }
            return x;
        }
        return readRawVarint64SlowPath();
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

    protected int readRawLittleEndian32() {
        return ((content[++this.position] & 0xff)
            | ((content[++this.position] & 0xff) << 8)
            | ((content[++this.position] & 0xff) << 16)
            | ((content[++this.position] & 0xff) << 24));
    }

    protected long readRawLittleEndian64() {
        return ((content[++this.position] & 0xffL)
            | ((content[++this.position] & 0xffL) << 8)
            | ((content[++this.position] & 0xffL) << 16)
            | ((content[++this.position] & 0xffL) << 24)
            | ((content[++this.position] & 0xffL) << 32)
            | ((content[++this.position] & 0xffL) << 40)
            | ((content[++this.position] & 0xffL) << 48)
            | ((content[++this.position] & 0xffL) << 56));
    }
}
