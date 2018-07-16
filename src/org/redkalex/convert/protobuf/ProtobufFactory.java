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

    public static ProtobufFactory create() {
        return new ProtobufFactory(null, Boolean.getBoolean("convert.protobuf.tiny"));
    }

    @Override
    public final ProtobufConvert getConvert() {
        if (convert == null) convert = new ProtobufConvert(this, tiny);
        return (ProtobufConvert) convert;
    }

    @Override
    public ProtobufFactory createChild() {
        return new ProtobufFactory(this, this.tiny);
    }

    @Override
    public ProtobufFactory createChild(boolean tiny) {
        return new ProtobufFactory(this, tiny);
    }

    @Override
    public ConvertType getConvertType() {
        return ConvertType.DIY;
    }

    @Override
    public boolean isReversible() {
        return true;
    }

    @Override
    public boolean isFieldSort() {
        return false;
    }

    public static int wireType(Class type) {
        if (type == double.class || type == Double.class) return 1;
        if (type == float.class || type == Float.class) return 5;
        if (type == boolean.class || type == Boolean.class || type.isEnum()) return 0;
        if (type.isPrimitive() || Number.class.isAssignableFrom(type)) return 0;
        return 2;
    }

}
