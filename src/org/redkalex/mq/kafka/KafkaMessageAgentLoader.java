/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import org.redkale.mq.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageAgentLoader implements MessageAgentLoader {

    @Override
    public boolean match(AnyValue config) {
        try {
            KafkaMessageAgent source = KafkaMessageAgent.class.getConstructor().newInstance();
            return source.match(config);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public Class<? extends MessageAgent> agentClass() {
        return KafkaMessageAgent.class;
    }

}
