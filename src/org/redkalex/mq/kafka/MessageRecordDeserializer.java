/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class MessageRecordDeserializer implements org.apache.kafka.common.serialization.Deserializer<MessageRecord> {

    @Override
    public MessageRecord deserialize(String topic, byte[] data) {
        return MessageRecordCoder.getInstance().decode(data);
    }

}
