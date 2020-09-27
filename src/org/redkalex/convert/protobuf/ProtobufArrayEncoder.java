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
public class ProtobufArrayEncoder<T> extends ArrayEncoder<T> {

    private final boolean enumtostring;

    public ProtobufArrayEncoder(ConvertFactory factory, Type type) {
        super(factory, type);
        this.enumtostring = ((ProtobufFactory) factory).enumtostring;
    }

    @Override
    protected void writeMemberValue(Writer out, EnMember member, Encodeable<Writer, Object> encoder, Object item, boolean first) {
        if (member != null) out.writeFieldName(member);
        if (item == null) {
            ((ProtobufWriter) out).writeUInt32(0);
        } else if (item instanceof CharSequence) {
            encoder.convertTo(out, item);
        } else {
            ProtobufWriter tmp = new ProtobufWriter().enumtostring(enumtostring);
            encoder.convertTo(tmp, item);
            int length = tmp.count();
            ((ProtobufWriter) out).writeUInt32(length);
            ((ProtobufWriter) out).writeTo(tmp.toArray());
        }
    }
}
