/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import java.io.*;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.function.*;
import org.redkale.convert.*;
import org.redkale.util.*;

/**
 * protobuf的Convert实现  <br>
 * 注意:  <br>
 * 1、 只实现proto3版本 <br>
 * 2、 int统一使用sint32, long统一使用sint64 <br>
 * 3、 集合统一 packed repeated <br>
 * 4、 目前使用的基础数据类型为：bool、sint32、sint64、float、double、bytes、string、map、Any <br>
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

    @Override
    public ProtobufConvert newConvert(final BiFunction<Attribute, Object, Object> fieldFunc) {
        return newConvert(fieldFunc, null);
    }

    @Override
    public ProtobufConvert newConvert(final BiFunction<Attribute, Object, Object> fieldFunc, Function<Object, ConvertField[]> objExtFunc) {
        return new ProtobufConvert(getFactory(), tiny) {
            @Override
            protected <S extends ProtobufWriter> S configWrite(S writer) {
                return fieldFunc(writer, fieldFunc, objExtFunc);
            }
        };
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
        return configWrite(new ProtobufByteBufferWriter(tiny, supplier));
    }

    public ProtobufWriter pollProtobufWriter(final OutputStream out) {
        return configWrite(new ProtobufStreamWriter(tiny, out));
    }

    public ProtobufWriter pollProtobufWriter() {
        return configWrite(writerPool.get().tiny(tiny));
    }

    public void offerProtobufWriter(final ProtobufWriter out) {
        if (out != null) writerPool.accept(out);
    }

    public <T> String getJsonDescriptor(Type type) {
        StringBuilder sb = new StringBuilder();
        defineJsonDescriptor(type, sb, false, "");
        return sb.toString();
    }

    protected void defineJsonDescriptor(Type type, StringBuilder sb, boolean dot, String prefix) {
        Encodeable encoder = factory.loadEncoder(type);
        if (encoder instanceof ObjectEncoder) {
            sb.append(prefix).append("\"message ").append(defineTypeName(type)).append("\" : {\r\n");
            EnMember[] ems = ((ObjectEncoder) encoder).getMembers();
            boolean flag = false;
            for (EnMember member : ems) {
                Type mtype = member.getEncoder().getType();
                if (!(mtype instanceof Class)) continue;
                Class mclz = (Class) member.getEncoder().getType();
                if (!mclz.isArray() && !mclz.getName().startsWith("java")) {
                    defineJsonDescriptor(mclz, sb, flag, prefix + "    ");
                } else if (mclz.isArray() && !mclz.getComponentType().getName().startsWith("java")) {
                    defineJsonDescriptor(mclz.getComponentType(), sb, flag, prefix + "    ");
                }
                flag = true;
            }
            for (int i = 0; i < ems.length; i++) {
                EnMember member = ems[i];
                sb.append(prefix).append("    \"").append(ProtobufFactory.wireTypeString(member.getEncoder().getType()))
                    .append(" ").append(member.getAttribute().field()).append("\" : ").append(member.getPosition()).append(i == ems.length - 1 ? "\r\n" : ",\r\n");
            }
            sb.append(prefix).append(dot ? "}," : "}").append("\r\n");
        }
    }

    public <T> String getProtoDescriptor(Type type) {
        StringBuilder sb = new StringBuilder();
        Class clazz = TypeToken.typeToClass(type);
        sb.append("//java ").append(clazz.isArray() ? (clazz.getComponentType().getName() + "[]") : clazz.getName()).append("\r\n\r\n");
        if (type instanceof Class) sb.append("option java_package = \"").append(clazz.getPackage().getName()).append("\";\r\n\r\n");
        sb.append("syntax = \"proto3\";\r\n\r\n");
        defineProtoDescriptor(type, sb, "");
        return sb.toString();
    }

    protected void defineProtoDescriptor(Type type, StringBuilder sb, String prefix) {
        Encodeable encoder = factory.loadEncoder(type);
        if (encoder instanceof ObjectEncoder) {
            sb.append(prefix).append("message ").append(defineTypeName(type)).append(" {\r\n");
            for (EnMember member : ((ObjectEncoder) encoder).getMembers()) {
                sb.append(prefix).append("    ").append(ProtobufFactory.wireTypeString(member.getEncoder().getType()))
                    .append(" ").append(member.getAttribute().field()).append(" = ").append(member.getPosition()).append(";\r\n");
            }
            sb.append(prefix).append("}\r\n");
        }
    }

    protected StringBuilder defineTypeName(Type type) {
        StringBuilder sb = new StringBuilder();
        if (type instanceof Class) {
            sb.append(((Class) type).getSimpleName().replace("[]", "_Array"));
        } else if (type instanceof ParameterizedType) {
            Type raw = ((ParameterizedType) type).getRawType();
            sb.append(((Class) raw).getSimpleName().replace("[]", "_Array"));
            Type[] ts = ((ParameterizedType) type).getActualTypeArguments();
            if (ts != null) {
                for (Type t : ts) {
                    if (t != null) sb.append('_').append(defineTypeName(t));
                }
            }
        }
        return sb;
    }

    //------------------------------ convertFrom -----------------------------------------------------------
    @Override
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
        Decodeable decoder = factory.loadDecoder(type);
        if (!(decoder instanceof ObjectDecoder) && !(decoder instanceof SimpledCoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        T rs = (T) decoder.convertFrom(in);
        readerPool.accept(in);
        return rs;
    }

    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final InputStream in) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertFrom InputStream");
        if (type == null || in == null) return null;
        Decodeable decoder = factory.loadDecoder(type);
        if (!(decoder instanceof ObjectDecoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        return (T) decoder.convertFrom(new ProtobufStreamReader(in));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final ByteBuffer... buffers) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertFrom ByteBuffer");
        if (type == null || buffers.length < 1) return null;
        Decodeable decoder = factory.loadDecoder(type);
        if (!(decoder instanceof ObjectDecoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        return (T) decoder.convertFrom(new ProtobufByteBufferReader((ConvertMask) null, buffers));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final ConvertMask mask, final ByteBuffer... buffers) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertFrom ByteBuffer");
        if (type == null || buffers.length < 1) return null;
        Decodeable decoder = factory.loadDecoder(type);
        if (!(decoder instanceof ObjectDecoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        return (T) decoder.convertFrom(new ProtobufByteBufferReader(mask, buffers));
    }

    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final ProtobufReader reader) {
        if (type == null) return null;
        @SuppressWarnings("unchecked")
        Decodeable decoder = factory.loadDecoder(type);
        if (!(decoder instanceof ObjectDecoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        T rs = (T) decoder.convertFrom(reader);
        return rs;
    }

    //------------------------------ convertTo -----------------------------------------------------------
    @Override
    public byte[] convertTo(final Object value) {
        if (value == null) {
            final ProtobufWriter out = pollProtobufWriter();
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
        final ProtobufWriter out = pollProtobufWriter();
        Encodeable encoder = factory.loadEncoder(type);
        if (!(encoder instanceof ObjectEncoder) && !(encoder instanceof SimpledCoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        encoder.convertTo(out, value);
        byte[] result = out.toArray();
        writerPool.accept(out);
        return result;
    }

    @Override
    public byte[] convertToBytes(final Object value) {
        return convertTo(value);
    }

    @Override
    public byte[] convertToBytes(final Type type, final Object value) {
        return convertTo(type, value);
    }

    @Override
    public byte[] convertMapTo(final Object... values) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertMapTo");
        if (values == null) return null;
        final ProtobufWriter out = pollProtobufWriter();
        ((AnyEncoder) factory.getAnyEncoder()).convertMapTo(out, values);
        byte[] result = out.toArray();
        writerPool.accept(out);
        return result;
    }

    public void convertTo(final OutputStream out, final Object value) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertTo OutputStream");
        if (value == null) {
            pollProtobufWriter(out).writeNull();
        } else {
            Encodeable encoder = factory.loadEncoder(value.getClass());
            if (!(encoder instanceof ObjectEncoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + value.getClass() + ")");
            encoder.convertTo(pollProtobufWriter(out), value);
        }
    }

    public void convertTo(final OutputStream out, final Type type, final Object value) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertTo OutputStream");
        if (type == null) return;
        if (value == null) {
            pollProtobufWriter(out).writeNull();
        } else {
            Encodeable encoder = factory.loadEncoder(type);
            if (!(encoder instanceof ObjectEncoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
            encoder.convertTo(pollProtobufWriter(out), value);
        }
    }

    public void convertMapTo(final OutputStream out, final Object... values) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertMapTo");
        if (values == null) {
            pollProtobufWriter(out).writeNull();
        } else {
            ((AnyEncoder) factory.getAnyEncoder()).convertMapTo(pollProtobufWriter(out), values);
        }
    }

    @Override
    public ByteBuffer[] convertTo(final Supplier<ByteBuffer> supplier, final Object value) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertTo ByteBuffer");
        if (supplier == null) return null;
        ProtobufByteBufferWriter out = pollProtobufWriter(supplier);
        if (value == null) {
            out.writeNull();
        } else {
            Encodeable encoder = factory.loadEncoder(value.getClass());
            if (!(encoder instanceof ObjectEncoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + value.getClass() + ")");
            encoder.convertTo(out, value);
        }
        return out.toBuffers();
    }

    @Override
    public ByteBuffer[] convertTo(final Supplier<ByteBuffer> supplier, final Type type, final Object value) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertTo ByteBuffer");
        if (supplier == null || type == null) return null;
        ProtobufByteBufferWriter out = pollProtobufWriter(supplier);
        if (value == null) {
            out.writeNull();
        } else {
            Encodeable encoder = factory.loadEncoder(type);
            if (!(encoder instanceof ObjectEncoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
            encoder.convertTo(out, value);
        }
        return out.toBuffers();
    }

    @Override
    public ByteBuffer[] convertMapTo(final Supplier<ByteBuffer> supplier, final Object... values) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertMapTo");
        if (supplier == null) return null;
        ProtobufByteBufferWriter out = pollProtobufWriter(supplier);
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
            Encodeable encoder = factory.loadEncoder(value.getClass());
            if (!(encoder instanceof ObjectEncoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + value.getClass() + ")");
            encoder.convertTo(writer, value);
        }
    }

    public void convertTo(final ProtobufWriter writer, final Type type, final Object value) {
        if (type == null) return;
        factory.loadEncoder(type).convertTo(writer, value);
    }

    public void convertMapTo(final ProtobufWriter writer, final Object... values) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertMapTo");
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
        final ProtobufWriter out = pollProtobufWriter();
        Encodeable encoder = factory.loadEncoder(type);
        if (!(encoder instanceof ObjectEncoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        encoder.convertTo(out, value);
        return out;
    }

    public ProtobufWriter convertMapToWriter(final Object... values) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertMapTo");
        final ProtobufWriter out = pollProtobufWriter();
        ((AnyEncoder) factory.getAnyEncoder()).convertMapTo(out, values);
        return out;
    }
}
