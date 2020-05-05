/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import java.lang.reflect.Type;
import org.redkale.convert.ObjectEncoder;

/**
 *
 * @author zhangjx
 */
public class ProtobufObjectEncoder<T> extends ObjectEncoder<ProtobufWriter, T> {

    protected ProtobufObjectEncoder(Type type) {
        super(type);
    }

}
