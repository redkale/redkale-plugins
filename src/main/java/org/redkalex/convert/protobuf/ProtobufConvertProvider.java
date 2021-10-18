/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.convert.protobuf;

import org.redkale.convert.*;

/**
 *
 * @author zhangjx
 */
public class ProtobufConvertProvider implements ConvertProvider {

    @Override
    public ConvertType type() {
        return ConvertType.PROTOBUF;
    }

    @Override
    public Convert convert() {
        return ProtobufConvert.root();
    }

}
