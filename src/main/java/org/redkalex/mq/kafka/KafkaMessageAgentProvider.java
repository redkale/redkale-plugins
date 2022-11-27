/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import javax.annotation.Priority;
import org.redkale.mq.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
@Priority(-800)
public class KafkaMessageAgentProvider implements MessageAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {

        try {
            Object.class.isAssignableFrom(org.apache.kafka.clients.CommonClientConfigs.class); //试图加载相关类
            return new KafkaMessageAgent().acceptsConf(config);
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public MessageAgent createInstance() {
        return new KafkaMessageAgent();
    }

}
