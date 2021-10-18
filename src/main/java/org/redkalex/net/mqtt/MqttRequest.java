/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.net.mqtt;

import java.nio.ByteBuffer;
import org.redkale.net.*;

/**
 *
 * @author zhangjx
 */
public class MqttRequest extends Request<MqttContext> {

    protected MqttRequest(MqttContext context) {
        super(context);
    }

    @Override
    protected int readHeader(ByteBuffer buffer, Request last) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void prepare() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
