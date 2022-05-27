/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import javax.annotation.Priority;
import org.redkale.mq.*;
import org.redkale.util.AnyValue;

/**
 *
 * @author zhangjx
 */
@Priority(-900)
public class KafkaMessageAgentProvider implements MessageAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        try {
            KafkaMessageAgent source = KafkaMessageAgent.class.getConstructor().newInstance();
            return source.acceptsConf(config);
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public Class<? extends MessageAgent> agentClass() {
        return KafkaMessageAgent.class;
    }

}
