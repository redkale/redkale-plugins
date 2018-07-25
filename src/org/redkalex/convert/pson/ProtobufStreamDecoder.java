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
 */
public class ProtobufStreamDecoder<T> extends StreamDecoder<T> {

    public ProtobufStreamDecoder(ConvertFactory factory, Type type) {
        super(factory, type);
    }

    @Override
    protected T readMemberValue(Reader in, DeMember member, boolean first) {
        if (member == null || first) {
            T rs = this.decoder.convertFrom(in);
            return rs;
        }
        ProtobufReader reader = (ProtobufReader) in;
        int tag = reader.readRawVarint32() >>> 3;
        T rs = (T) this.decoder.convertFrom(reader);
        return rs;
    }
}
