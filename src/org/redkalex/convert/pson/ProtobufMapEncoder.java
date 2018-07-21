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
    protected void writeKey(Writer out, EnMember member, K key) {
        if (member != null) out.writeFieldName(member);
        keyencoder.convertTo(out, key);
    }
}
