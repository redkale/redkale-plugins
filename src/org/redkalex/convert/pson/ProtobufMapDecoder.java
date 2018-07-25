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
public class ProtobufMapDecoder<K, V> extends MapDecoder<K, V> {

    public ProtobufMapDecoder(ConvertFactory factory, Type type) {
        super(factory, type);
    }

    @Override
    protected Reader getMapEntryReader(Reader in) {
        byte[] bs = ((ProtobufReader) in).readByteArray();
        //Utility.println("获得的字节", bs);
        return new ProtobufReader(bs);
    }

    @Override
    protected K readKeyMember(Reader in, DeMember member, boolean first) {
        ProtobufReader reader = (ProtobufReader) in;
        int tag = reader.readRawVarint32() >>> 3;
        return keyDecoder.convertFrom(in);
    }

    @Override
    protected V readValueMember(Reader in, DeMember member, boolean first) {
        ProtobufReader reader = (ProtobufReader) in;
        int tag = reader.readRawVarint32() >>> 3;
        return valueDecoder.convertFrom(in);
    }
}
