/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.pson;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.redkale.convert.*;
import org.redkale.util.*;

/**
 * protobuf的Convert实现  <br>
 * 注意:  <br>
 * 1、 只实现proto3版本 <br>
 * 2、 int统一使用sint32, long统一使用sint64 <br>
 * 3、集合统一 packed repeated <br>
 *
 * @author zhangjx
 */
public class ProtobufConvert extends BinaryConvert<ProtobufReader, ProtobufWriter> {

    private static final ObjectPool<ProtobufReader> readerPool = ProtobufReader.createPool(Integer.getInteger("convert.protobuf.pool.size", 16));

    private static final ObjectPool<ProtobufWriter> writerPool = ProtobufWriter.createPool(Integer.getInteger("convert.protobuf.pool.size", 16));

    private final boolean tiny;

    protected ProtobufConvert(ConvertFactory<ProtobufReader, ProtobufWriter> factory, boolean tiny) {
        super(factory);
        this.tiny = tiny;
    }

    @Override
    public ProtobufFactory getFactory() {
        return (ProtobufFactory) factory;
    }

    public static ProtobufConvert root() {
        return ProtobufFactory.root().getConvert();
    }

    //------------------------------ reader -----------------------------------------------------------
    public ProtobufReader pollProtobufReader(final ByteBuffer... buffers) {
        return new ProtobufByteBufferReader((ConvertMask) null, buffers);
    }

    public ProtobufReader pollProtobufReader(final InputStream in) {
        return new ProtobufStreamReader(in);
    }

    public ProtobufReader pollProtobufReader() {
        return readerPool.get();
    }

    public void offerProtobufReader(final ProtobufReader in) {
        if (in != null) readerPool.accept(in);
    }

    //------------------------------ writer -----------------------------------------------------------
    public ProtobufByteBufferWriter pollProtobufWriter(final Supplier<ByteBuffer> supplier) {
        return new ProtobufByteBufferWriter(tiny, supplier);
    }

    public ProtobufWriter pollProtobufWriter(final OutputStream out) {
        return new ProtobufStreamWriter(tiny, out);
    }

    public ProtobufWriter pollProtobufWriter() {
        return writerPool.get().tiny(tiny);
    }

    public void offerProtobufWriter(final ProtobufWriter out) {
        if (out != null) writerPool.accept(out);
    }

    public <T> String getProtoDescriptor(Class<T> clazz) {
        StringBuilder sb = new StringBuilder();
        sb.append("//java ").append(clazz.isArray() ? (clazz.getComponentType().getName() + "[]") : clazz.getName()).append("\r\n");
        sb.append("syntax = \"proto3\";\r\n");
        defineProtoDescriptor(clazz, sb, "");
        return sb.toString();
    }

    protected <T> void defineProtoDescriptor(Class<T> clazz, StringBuilder sb, String prefix) {
        sb.append(prefix).append("message ").append(clazz.getSimpleName().replace("[]", "_Array")).append(" {\r\n");
        Encodeable<ProtobufWriter, T> encoder = factory.loadEncoder(clazz);

        sb.append(prefix).append("}\r\n");
    }

    //------------------------------ convertFrom -----------------------------------------------------------
    public <T> T convertFrom(final Type type, final byte[] bytes) {
        if (bytes == null) return null;
        return convertFrom(type, bytes, 0, bytes.length);
    }

    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final byte[] bytes, final int start, final int len) {
        if (type == null) return null;
        final ProtobufReader in = readerPool.get();
        in.setBytes(bytes, start, len);
        @SuppressWarnings("unchecked")
        T rs = (T) factory.loadDecoder(type).convertFrom(in);
        readerPool.accept(in);
        return rs;
    }

    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final InputStream in) {
        if (type == null || in == null) return null;
        return (T) factory.loadDecoder(type).convertFrom(new ProtobufStreamReader(in));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final ByteBuffer... buffers) {
        if (type == null || buffers.length < 1) return null;
        return (T) factory.loadDecoder(type).convertFrom(new ProtobufByteBufferReader((ConvertMask) null, buffers));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final ConvertMask mask, final ByteBuffer... buffers) {
        if (type == null || buffers.length < 1) return null;
        return (T) factory.loadDecoder(type).convertFrom(new ProtobufByteBufferReader(mask, buffers));
    }

    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final ProtobufReader reader) {
        if (type == null) return null;
        @SuppressWarnings("unchecked")
        T rs = (T) factory.loadDecoder(type).convertFrom(reader);
        return rs;
    }

    //------------------------------ convertTo -----------------------------------------------------------
    @Override
    public byte[] convertTo(final Object value) {
        if (value == null) {
            final ProtobufWriter out = writerPool.get().tiny(tiny);
            out.writeNull();
            byte[] result = out.toArray();
            writerPool.accept(out);
            return result;
        }
        return convertTo(value.getClass(), value);
    }

    @Override
    public byte[] convertTo(final Type type, final Object value) {
        if (type == null) return null;
        final ProtobufWriter out = writerPool.get().tiny(tiny);
        factory.loadEncoder(type).convertTo(out, value);
        byte[] result = out.toArray();
        writerPool.accept(out);
        return result;
    }

    @Override
    public byte[] convertMapTo(final Object... values) {
        if (values == null) return null;
        final ProtobufWriter out = writerPool.get().tiny(tiny);
        ((AnyEncoder) factory.getAnyEncoder()).convertMapTo(out, values);
        byte[] result = out.toArray();
        writerPool.accept(out);
        return result;
    }

    public void convertTo(final OutputStream out, final Object value) {
        if (value == null) {
            new ProtobufStreamWriter(tiny, out).writeNull();
        } else {
            factory.loadEncoder(value.getClass()).convertTo(new ProtobufStreamWriter(tiny, out), value);
        }
    }

    public void convertTo(final OutputStream out, final Type type, final Object value) {
        if (type == null) return;
        if (value == null) {
            new ProtobufStreamWriter(tiny, out).writeNull();
        } else {
            factory.loadEncoder(type).convertTo(new ProtobufStreamWriter(tiny, out), value);
        }
    }

    public void convertMapTo(final OutputStream out, final Object... values) {
        if (values == null) {
            new ProtobufStreamWriter(tiny, out).writeNull();
        } else {
            ((AnyEncoder) factory.getAnyEncoder()).convertMapTo(new ProtobufStreamWriter(tiny, out), values);
        }
    }

    @Override
    public ByteBuffer[] convertTo(final Supplier<ByteBuffer> supplier, final Object value) {
        if (supplier == null) return null;
        ProtobufByteBufferWriter out = new ProtobufByteBufferWriter(tiny, supplier);
        if (value == null) {
            out.writeNull();
        } else {
            factory.loadEncoder(value.getClass()).convertTo(out, value);
        }
        return out.toBuffers();
    }

    @Override
    public ByteBuffer[] convertTo(final Supplier<ByteBuffer> supplier, final Type type, final Object value) {
        if (supplier == null || type == null) return null;
        ProtobufByteBufferWriter out = new ProtobufByteBufferWriter(tiny, supplier);
        if (value == null) {
            out.writeNull();
        } else {
            factory.loadEncoder(type).convertTo(out, value);
        }
        return out.toBuffers();
    }

    @Override
    public ByteBuffer[] convertMapTo(final Supplier<ByteBuffer> supplier, final Object... values) {
        if (supplier == null) return null;
        ProtobufByteBufferWriter out = new ProtobufByteBufferWriter(tiny, supplier);
        if (values == null) {
            out.writeNull();
        } else {
            ((AnyEncoder) factory.getAnyEncoder()).convertMapTo(out, values);
        }
        return out.toBuffers();
    }

    public void convertTo(final ProtobufWriter writer, final Object value) {
        if (value == null) {
            writer.writeNull();
        } else {
            factory.loadEncoder(value.getClass()).convertTo(writer, value);
        }
    }

    public void convertTo(final ProtobufWriter writer, final Type type, final Object value) {
        if (type == null) return;
        factory.loadEncoder(type).convertTo(writer, value);
    }

    public void convertMapTo(final ProtobufWriter writer, final Object... values) {
        if (values == null) {
            writer.writeNull();
        } else {
            ((AnyEncoder) factory.getAnyEncoder()).convertMapTo(writer, values);
        }
    }

    public ProtobufWriter convertToWriter(final Object value) {
        if (value == null) return null;
        return convertToWriter(value.getClass(), value);
    }

    public ProtobufWriter convertToWriter(final Type type, final Object value) {
        if (type == null) return null;
        final ProtobufWriter out = writerPool.get().tiny(tiny);
        factory.loadEncoder(type).convertTo(out, value);
        return out;
    }

    public ProtobufWriter convertMapToWriter(final Object... values) {
        final ProtobufWriter out = writerPool.get().tiny(tiny);
        ((AnyEncoder) factory.getAnyEncoder()).convertMapTo(out, values);
        return out;
    }
}
