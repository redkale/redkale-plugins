/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.net.mqtt;

import org.redkale.net.Context;

/**
 *
 * @author zhangjx
 */
public class MqttContext extends Context {

    public MqttContext(MqttContextConfig config) {
        super(config);
    }

    public static class MqttContextConfig extends ContextConfig {

    }
}
