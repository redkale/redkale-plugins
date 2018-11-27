/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.pson;

import java.lang.reflect.Type;
import org.redkale.convert.*;

/**
 *
 * @author zhangjx
 * @param <K> K
 * @param <V> V
 */
public class ProtobufMapEncoder<K, V> extends MapEncoder<K, V> {

    public ProtobufMapEncoder(ConvertFactory factory, Type type) {
        super(factory, type);
    }

    @Override
    protected void writeMemberValue(Writer out, EnMember member, K key, V value, boolean first) {
        ProtobufWriter tmp = new ProtobufWriter();
        if (member != null) out.writeFieldName(member);
        tmp.writeUInt32(1 << 3 | ProtobufFactory.wireType(keyEncoder.getType()));
        keyEncoder.convertTo(tmp, key);
        tmp.writeUInt32(2 << 3 | ProtobufFactory.wireType(valueEncoder.getType()));
        valueEncoder.convertTo(tmp, value);
        int length = tmp.count();
        ((ProtobufWriter) out).writeUInt32(length);
        ((ProtobufWriter) out).writeTo(tmp.toArray());
    }
}
