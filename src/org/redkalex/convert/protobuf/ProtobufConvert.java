/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.redkale.convert.*;
import org.redkale.util.ObjectPool;

/**
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
    public byte[] convertTo(Object value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte[] convertTo(Type type, Object value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte[] convertMapTo(Object... values) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T> T convertFrom(Type type, ByteBuffer... buffers) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T> T convertFrom(Type type, ConvertMask mask, ByteBuffer... buffers) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ByteBuffer[] convertTo(Supplier<ByteBuffer> supplier, Object value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ByteBuffer[] convertTo(Supplier<ByteBuffer> supplier, Type type, Object value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ByteBuffer[] convertMapTo(Supplier<ByteBuffer> supplier, Object... values) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
