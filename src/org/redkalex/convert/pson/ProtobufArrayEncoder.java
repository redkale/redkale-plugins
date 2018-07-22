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
 * @param <T> T
 */
public class ProtobufArrayEncoder<T> extends ArrayEncoder<T> {

    public ProtobufArrayEncoder(ConvertFactory factory, Type type) {
        super(factory, type);
    }

    @Override
    protected void writeValue(Writer out, EnMember member, Encodeable<Writer, Object> encoder, Object value) {
        if (member != null) out.writeFieldName(member);
        encoder.convertTo(out, value);
    }
}
