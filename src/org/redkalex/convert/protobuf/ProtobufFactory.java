/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import java.io.Serializable;
import org.redkale.convert.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class ProtobufFactory extends ConvertFactory<ProtobufReader, ProtobufWriter> {

    private static final ProtobufFactory instance = new ProtobufFactory(null, Boolean.getBoolean("convert.protobuf.tiny"));

    static final Decodeable objectDecoder = instance.loadDecoder(Object.class);

    static final Encodeable objectEncoder = instance.loadEncoder(Object.class);

    static {
        instance.register(Serializable.class, objectDecoder);
        instance.register(Serializable.class, objectEncoder);

        instance.register(AnyValue.class, instance.loadDecoder(AnyValue.DefaultAnyValue.class));
        instance.register(AnyValue.class, instance.loadEncoder(AnyValue.DefaultAnyValue.class));
    }

    private ProtobufFactory(ProtobufFactory parent, boolean tiny) {
        super(parent, tiny);
    }

    public static ProtobufFactory root() {
        return instance;
    }

    @Override
    public final ProtobufConvert getConvert() {
        if (convert == null) convert = new ProtobufConvert(this, tiny);
        return (ProtobufConvert) convert;
    }

    @Override
    public ConvertType getConvertType() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isReversible() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ConvertFactory createChild() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ConvertFactory createChild(boolean tiny) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
