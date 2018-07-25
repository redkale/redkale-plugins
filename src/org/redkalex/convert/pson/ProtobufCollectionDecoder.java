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
public class ProtobufCollectionDecoder<T> extends CollectionDecoder<T> {
    
    public ProtobufCollectionDecoder(ConvertFactory factory, Type type) {
        super(factory, type);
    }
    
}
