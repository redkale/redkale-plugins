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
@Priority(-900)
public class KafkaMessageAgentProvider implements MessageAgentProvider {

    @Override
    public boolean acceptsConf(AnyValue config) {
        return new KafkaMessageAgent().acceptsConf(config);
    }

    @Override
    public MessageAgent createInstance() {
        return new KafkaMessageAgent();
    }

}
