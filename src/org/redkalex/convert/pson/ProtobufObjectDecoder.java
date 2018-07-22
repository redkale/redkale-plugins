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
    protected boolean hasNext(ProtobufReader in) {
        return in.hasNext();
    }

    @Override
    protected Object readMemberValue(ProtobufReader in, DeMember member) {
        Decodeable decoder = member.getDecoder();
        if (decoder instanceof ProtobufArrayDecoder) {
            return ((ProtobufArrayDecoder) decoder).convertFrom(in, member);
        } else {
            return member.read(in);
        }
    }

    @Override
    protected void readMemberValue(ProtobufReader in, DeMember member, T result) {
        Decodeable decoder = member.getDecoder();
        if (decoder instanceof ProtobufArrayDecoder) {
            member.getAttribute().set(result, ((ProtobufArrayDecoder) decoder).convertFrom(in, member));
        } else {
            member.read(in, result);
        }
    }
}
