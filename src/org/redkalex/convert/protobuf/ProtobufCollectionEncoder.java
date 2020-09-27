/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import java.lang.reflect.Type;
import org.redkale.convert.*;

/**
 *
 * @author zhangjx
 * @param <T> T
 */
public class ProtobufCollectionEncoder<T> extends CollectionEncoder<T> {

    private final boolean enumtostring;

    public ProtobufCollectionEncoder(ConvertFactory factory, Type type) {
        super(factory, type);
        this.enumtostring = ((ProtobufFactory) factory).enumtostring;
    }

    @Override
    protected void writeValue(Writer out, EnMember member, Object item) {
        if (member != null) out.writeFieldName(member);
        if (item == null) {
            ((ProtobufWriter) out).writeUInt32(0);
        } else if (item instanceof CharSequence) {
            componentEncoder.convertTo(out, item); 
        } else {
            ProtobufWriter tmp = new ProtobufWriter().enumtostring(enumtostring);
            componentEncoder.convertTo(tmp, item);
            int length = tmp.count();
            ((ProtobufWriter) out).writeUInt32(length);
            ((ProtobufWriter) out).writeTo(tmp.toArray());
        }
    }
}
