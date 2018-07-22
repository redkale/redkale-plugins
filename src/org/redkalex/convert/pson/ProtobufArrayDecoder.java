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
public class ProtobufArrayDecoder<T> extends ArrayDecoder<T> {

    public ProtobufArrayDecoder(ConvertFactory factory, Type type) {
        super(factory, type);
    }

    @Override
    protected T readMemberValue(Reader in, DeMember member, boolean first) {
        if (member == null || first) {
            return this.decoder.convertFrom(in);
        }
        ProtobufReader reader = (ProtobufReader) in;
        int tag = reader.readRawVarint32() >>> 3;
        return (T) member.getDecoder().convertFrom(reader);
    }
}
