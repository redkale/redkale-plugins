/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import java.io.*;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.*;
import org.redkale.convert.*;
import org.redkale.convert.ext.StringArraySimpledCoder;
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
    @Override
    protected <S extends ProtobufWriter> S configWrite(S writer) {
        writer.initoffset = writer.count;
        return writer;
    }

    public ProtobufByteBufferWriter pollProtobufWriter(final Supplier<ByteBuffer> supplier) {
        return configWrite(new ProtobufByteBufferWriter(tiny, ((ProtobufFactory) factory).enumtostring, supplier));
    }

    public ProtobufWriter pollProtobufWriter(final OutputStream out) {
        return configWrite(new ProtobufStreamWriter(tiny, ((ProtobufFactory) factory).enumtostring, out));
    }

    public ProtobufWriter pollProtobufWriter() {
        return configWrite(writerPool.get().tiny(tiny).enumtostring(((ProtobufFactory) factory).enumtostring));
    }

    public void offerProtobufWriter(final ProtobufWriter out) {
        if (out != null) writerPool.accept(out);
    }

    public <T> String getJsonDecodeDescriptor(Type type) {
        StringBuilder sb = new StringBuilder();
        defineJsonDecodeDescriptor(type, sb, "", null);
        return sb.toString();
    }

    public <T> String getJsonDecodeDescriptor(Type type, BiFunction<Type, DeMember, Boolean> func) {
        StringBuilder sb = new StringBuilder();
        defineJsonDecodeDescriptor(type, sb, "", func);
        return sb.toString();
    }

    protected void defineJsonDecodeDescriptor(Type type, StringBuilder sb, String prefix, BiFunction<Type, DeMember, Boolean> excludeFunc) {
        Decodeable decoder = factory.loadDecoder(type);
        boolean dot = sb.length() > 0;
        if (decoder instanceof ObjectDecoder) {
            if (sb.length() > 0) {
                sb.append(prefix).append("\"message ").append(defineTypeName(type)).append("\" : {\r\n");
            } else {
                sb.append("{\r\n");
            }
            DeMember[] ems = ((ObjectDecoder) decoder).getMembers();
            List<DeMember> members = new ArrayList<>();
            for (DeMember member : ems) {
                if (excludeFunc != null && excludeFunc.apply(type, member)) continue;
                members.add(member);
            }
            for (DeMember member : members) {
                Type mtype = member.getDecoder().getType();
                if (!(mtype instanceof Class)) {
                    if (mtype instanceof ParameterizedType) {
                        final ParameterizedType pt = (ParameterizedType) mtype;
                        if (pt.getActualTypeArguments().length == 1 && (pt.getActualTypeArguments()[0] instanceof Class)) {
                            defineJsonDecodeDescriptor(mtype, sb, prefix + "    ", excludeFunc);
                        }
                    } else if (mtype instanceof GenericArrayType) {
                        final GenericArrayType gt = (GenericArrayType) mtype;
                        if (!gt.getGenericComponentType().toString().startsWith("java")
                            && gt.getGenericComponentType().toString().indexOf('.') > 0) {
                            defineJsonDecodeDescriptor(gt.getGenericComponentType(), sb, prefix + "    ", excludeFunc);
                        }
                    }
                    continue;
                }
                Class mclz = (Class) member.getDecoder().getType();
                if (!mclz.isArray() && !mclz.isEnum() && !mclz.getName().startsWith("java")) {
                    defineJsonDecodeDescriptor(mclz, sb, prefix + "    ", excludeFunc);
                } else if (mclz.isArray() && !mclz.getComponentType().getName().startsWith("java")
                    && !mclz.getComponentType().getName().equals("boolean") && !mclz.getComponentType().getName().equals("byte")
                    && !mclz.getComponentType().getName().equals("char") && !mclz.getComponentType().getName().equals("short")
                    && !mclz.getComponentType().getName().equals("int") && !mclz.getComponentType().getName().equals("long")
                    && !mclz.getComponentType().getName().equals("float") && !mclz.getComponentType().getName().equals("double")) {
                    defineJsonDecodeDescriptor(mclz.getComponentType(), sb, prefix + "    ", excludeFunc);
                }
            }
            for (int i = 0; i < members.size(); i++) {
                DeMember member = members.get(i);
                try {
                    sb.append(prefix).append("    \"").append(ProtobufFactory.wireTypeString(member.getDecoder().getType(), ((ProtobufFactory) factory).enumtostring))
                        .append(" ").append(member.getAttribute().field()).append("\" : ").append(member.getPosition()).append(i == members.size() - 1 ? "\r\n" : ",\r\n");
                } catch (RuntimeException e) {
                    System.err.println("member = " + member);
                    throw e;
                }
            }
            sb.append(prefix).append(dot ? "}," : "}").append("\r\n");
        } else if ((!(type instanceof Class) || !((Class) type).isArray() || !((Class) type).getComponentType().getName().startsWith("java")) && (decoder instanceof ProtobufArrayDecoder || decoder instanceof ProtobufCollectionDecoder)) {
            Type mtype = decoder instanceof ProtobufArrayDecoder ? ((ProtobufArrayDecoder) decoder).getComponentType() : ((ProtobufCollectionDecoder) decoder).getComponentType();
            defineJsonDecodeDescriptor(mtype, sb, prefix, excludeFunc);
        } else if (sb.length() == 0) {
            if (decoder instanceof SimpledCoder
                || decoder instanceof StringArraySimpledCoder
                || (decoder instanceof ProtobufArrayDecoder && ((ProtobufArrayDecoder) decoder).getComponentDecoder() instanceof SimpledCoder)
                || (decoder instanceof ProtobufCollectionDecoder && ((ProtobufCollectionDecoder) decoder).getComponentDecoder() instanceof SimpledCoder)) {
                sb.append(prefix).append("{\r\n");
                sb.append(prefix).append("    \"").append(ProtobufFactory.wireTypeString(type, ((ProtobufFactory) factory).enumtostring)).append(" 0\" : 0\r\n");
                sb.append(prefix).append(dot ? "}," : "}").append("\r\n");
            } else if (decoder instanceof MapDecoder) {
                sb.append(prefix).append("{\r\n");
                sb.append(prefix).append("    \"").append(ProtobufFactory.wireTypeString(type, ((ProtobufFactory) factory).enumtostring)).append(" 0\" : 0\r\n");
                sb.append(prefix).append(dot ? "}," : "}").append("\r\n");
            } else {
                throw new ConvertException("Not support type (" + type + ")");
            }
        } else {
            throw new ConvertException("Not support the type (" + type + ")");
        }
    }

    public <T> String getJsonEncodeDescriptor(Type type) {
        StringBuilder sb = new StringBuilder();
        defineJsonEncodeDescriptor(type, sb, "", null);
        return sb.toString();
    }

    public <T> String getJsonEncodeDescriptor(Type type, BiFunction<Type, EnMember, Boolean> func) {
        StringBuilder sb = new StringBuilder();
        defineJsonEncodeDescriptor(type, sb, "", func);
        return sb.toString();
    }

    protected void defineJsonEncodeDescriptor(Type type, StringBuilder sb, String prefix, BiFunction<Type, EnMember, Boolean> excludeFunc) {
        Encodeable encoder = factory.loadEncoder(type);
        boolean dot = sb.length() > 0;
        if (encoder instanceof ObjectEncoder) {
            if (sb.length() > 0) {
                sb.append(prefix).append("\"message ").append(defineTypeName(type)).append("\" : {\r\n");
            } else {
                sb.append("{\r\n");
            }
            EnMember[] ems = ((ObjectEncoder) encoder).getMembers();
            List<EnMember> members = new ArrayList<>();
            for (EnMember member : ems) {
                if (excludeFunc != null && excludeFunc.apply(type, member)) continue;
                members.add(member);
            }
            for (EnMember member : members) {
                Type mtype = member.getEncoder().getType();
                if (!(mtype instanceof Class)) {
                    if (mtype instanceof ParameterizedType) {
                        final ParameterizedType pt = (ParameterizedType) mtype;
                        if (pt.getActualTypeArguments().length == 1 && (pt.getActualTypeArguments()[0] instanceof Class)) {
                            defineJsonEncodeDescriptor(mtype, sb, prefix + "    ", excludeFunc);
                        }
                    } else if (mtype instanceof GenericArrayType) {
                        final GenericArrayType gt = (GenericArrayType) mtype;
                        if (!gt.getGenericComponentType().toString().startsWith("java")
                            && gt.getGenericComponentType().toString().indexOf('.') > 0) {
                            defineJsonEncodeDescriptor(gt.getGenericComponentType(), sb, prefix + "    ", excludeFunc);
                        }
                    }
                    continue;
                }
                Class mclz = (Class) member.getEncoder().getType();
                if (!mclz.isArray() && !mclz.isEnum() && !mclz.getName().startsWith("java")) {
                    defineJsonEncodeDescriptor(mclz, sb, prefix + "    ", excludeFunc);
                } else if (mclz.isArray() && !mclz.getComponentType().getName().startsWith("java")
                    && !mclz.getComponentType().getName().equals("boolean") && !mclz.getComponentType().getName().equals("byte")
                    && !mclz.getComponentType().getName().equals("char") && !mclz.getComponentType().getName().equals("short")
                    && !mclz.getComponentType().getName().equals("int") && !mclz.getComponentType().getName().equals("long")
                    && !mclz.getComponentType().getName().equals("float") && !mclz.getComponentType().getName().equals("double")) {
                    defineJsonEncodeDescriptor(mclz.getComponentType(), sb, prefix + "    ", excludeFunc);
                }
            }
            for (int i = 0; i < members.size(); i++) {
                EnMember member = members.get(i);
                try {
                    sb.append(prefix).append("    \"").append(ProtobufFactory.wireTypeString(member.getEncoder().getType(), ((ProtobufFactory) factory).enumtostring))
                        .append(" ").append(member.getAttribute().field()).append("\" : ").append(member.getPosition()).append(i == members.size() - 1 ? "\r\n" : ",\r\n");
                } catch (RuntimeException e) {
                    System.err.println("member = " + member);
                    throw e;
                }
            }
            sb.append(prefix).append(dot ? "}," : "}").append("\r\n");
        } else if (encoder instanceof ProtobufArrayEncoder || encoder instanceof ProtobufCollectionEncoder) {
            Type mtype = encoder instanceof ProtobufArrayEncoder ? ((ProtobufArrayEncoder) encoder).getComponentType() : ((ProtobufCollectionEncoder) encoder).getComponentType();
            defineJsonEncodeDescriptor(mtype, sb, prefix, excludeFunc);
        } else if (sb.length() == 0) {
            if (encoder instanceof SimpledCoder
                || (encoder instanceof ProtobufArrayEncoder && ((ProtobufArrayEncoder) encoder).getComponentEncoder() instanceof SimpledCoder)
                || (encoder instanceof ProtobufCollectionEncoder && ((ProtobufCollectionEncoder) encoder).getComponentEncoder() instanceof SimpledCoder)) {
                sb.append(prefix).append("{\r\n");
                sb.append(prefix).append("    \"").append(ProtobufFactory.wireTypeString(type, ((ProtobufFactory) factory).enumtostring)).append(" 0\" : 0\r\n");
                sb.append(prefix).append(dot ? "}," : "}").append("\r\n");
            } else if (encoder instanceof MapEncoder) {
                sb.append(prefix).append("{\r\n");
                sb.append(prefix).append("    \"").append(ProtobufFactory.wireTypeString(type, ((ProtobufFactory) factory).enumtostring)).append(" 0\" : 0\r\n");
                sb.append(prefix).append(dot ? "}," : "}").append("\r\n");
            } else {
                throw new ConvertException("Not support the type (" + type + ")");
            }
        } else {
            throw new ConvertException("Not support the type (" + type + ")");
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
                sb.append(prefix).append("    ").append(ProtobufFactory.wireTypeString(member.getEncoder().getType(), ((ProtobufFactory) factory).enumtostring))
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

    @Override
    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final byte[] bytes, final int offset, final int len) {
        if (type == null) return null;
        final ProtobufReader in = readerPool.get();
        in.setBytes(bytes, offset, len);
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
        @SuppressWarnings("unchecked")
        Decodeable decoder = factory.loadDecoder(type);
        if (!(decoder instanceof ObjectDecoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        return (T) decoder.convertFrom(new ProtobufStreamReader(in));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final ByteBuffer... buffers) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertFrom ByteBuffer");
        if (type == null || buffers.length < 1) return null;
        @SuppressWarnings("unchecked")
        Decodeable decoder = factory.loadDecoder(type);
        if (!(decoder instanceof ObjectDecoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        return (T) decoder.convertFrom(new ProtobufByteBufferReader((ConvertMask) null, buffers));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T convertFrom(final Type type, final ConvertMask mask, final ByteBuffer... buffers) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertFrom ByteBuffer");
        if (type == null || buffers.length < 1) return null;
        @SuppressWarnings("unchecked")
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

    public byte[] convertTo(final Object value, int tag, byte... appends) {
        return convertTo(value.getClass(), value, tag, appends);
    }

    public byte[] convertTo(final Type type, final Object value, int tag, byte... appends) {
        if (type == null) return null;
        final ProtobufWriter out = pollProtobufWriter();
        Encodeable encoder = factory.loadEncoder(type);
        if (!(encoder instanceof ObjectEncoder) && !(encoder instanceof SimpledCoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        encoder.convertTo(out, value);
        out.writeUInt32(tag);
        out.writeTo(appends);
        byte[] result = out.toArray();
        writerPool.accept(out);
        return result;
    }

    public byte[] convertTo(final Type type, final Object value, int tag, final Type type2, final Object value2) {
        if (type == null) return null;
        final ProtobufWriter out = pollProtobufWriter();
        Encodeable encoder = factory.loadEncoder(type);
        if (!(encoder instanceof ObjectEncoder) && !(encoder instanceof SimpledCoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type + ")");
        encoder.convertTo(out, value);
        out.writeUInt32(tag);
        Encodeable encoder2 = factory.loadEncoder(type2);
        if (!(encoder2 instanceof ObjectEncoder) && !(encoder2 instanceof SimpledCoder)) throw new RuntimeException(this.getClass().getSimpleName() + " not supported type(" + type2 + ")");
        encoder2.convertTo(out, value2);
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
        //if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertTo ByteBuffer");
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
        //if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertTo ByteBuffer");
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
        //if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertMapTo");
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
            writer.initoffset = writer.count;
            encoder.convertTo(writer, value);
        }
    }

    public void convertTo(final ProtobufWriter writer, final Type type, final Object value) {
        if (type == null) return;
        writer.initoffset = writer.count;
        factory.loadEncoder(type).convertTo(writer, value);
    }

    public void convertMapTo(final ProtobufWriter writer, final Object... values) {
        if (true) throw new RuntimeException(this.getClass().getSimpleName() + " not supported convertMapTo");
        if (values == null) {
            writer.writeNull();
        } else {
            writer.initoffset = writer.count;
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
