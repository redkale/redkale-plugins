/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.net.mqtt;

import org.redkale.net.Response;

/**
 *
 * @author zhangjx
 */
public class MqttResponse extends Response<MqttContext, MqttRequest> {

    public MqttResponse(MqttContext context, MqttRequest request) {
        super(context, request);
    }
    
    @Override
    protected void prepare() {
        super.prepare();
    }

    @Override
    protected boolean recycle() {
        return super.recycle();
    }

}
