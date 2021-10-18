/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.net.mqtt;

import java.io.IOException;
import org.redkale.net.PrepareServlet;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class MqttPrepareServlet extends PrepareServlet<String, MqttContext, MqttRequest, MqttResponse, MqttServlet> {

    @Override
    public void addServlet(MqttServlet servlet, Object attachment, AnyValue conf, String... mappings) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void execute(MqttRequest request, MqttResponse response) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
