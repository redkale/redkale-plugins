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
public class ProtobufObjectDecoder<T> extends ObjectDecoder<ProtobufReader, T> {

    protected ProtobufObjectDecoder(Type type) {
        super(type);
    }

    @Override
    protected boolean hasNext(ProtobufReader in, boolean first) {
        return in.hasNext();
    }

    @Override
    protected Object readMemberValue(ProtobufReader in, DeMember member, boolean first) {
        Decodeable decoder = member.getDecoder();
        if (decoder instanceof ProtobufArrayDecoder) {
            return ((ProtobufArrayDecoder) decoder).convertFrom(in, member);
        } else if (decoder instanceof ProtobufCollectionDecoder) {
            return ((ProtobufCollectionDecoder) decoder).convertFrom(in, member);
        } else if (decoder instanceof ProtobufStreamDecoder) {
            return ((ProtobufStreamDecoder) decoder).convertFrom(in, member);
        } else if (decoder instanceof ProtobufMapDecoder) {
            return ((ProtobufMapDecoder) decoder).convertFrom(in, member);
        } else {
            return member.read(in);
        }
    }

    @Override
    protected void readMemberValue(ProtobufReader in, DeMember member, T result, boolean first) {
        Decodeable decoder = member.getDecoder();
        if (decoder instanceof ProtobufArrayDecoder) {
            member.getAttribute().set(result, ((ProtobufArrayDecoder) decoder).convertFrom(in, member));
        } else if (decoder instanceof ProtobufCollectionDecoder) {
            member.getAttribute().set(result, ((ProtobufCollectionDecoder) decoder).convertFrom(in, member));
        } else if (decoder instanceof ProtobufStreamDecoder) {
            member.getAttribute().set(result, ((ProtobufStreamDecoder) decoder).convertFrom(in, member));
        } else if (decoder instanceof ProtobufMapDecoder) {
            member.getAttribute().set(result, ((ProtobufMapDecoder) decoder).convertFrom(in, member));
        } else {
            member.read(in, result);
        }
    }
}
