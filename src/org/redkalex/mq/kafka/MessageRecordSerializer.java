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
public class MessageRecordSerializer implements org.apache.kafka.common.serialization.Serializer<MessageRecord> {

    public static final byte[] EMPTY_BYTES = new byte[0];

    @Override
    public byte[] serialize(String topic, MessageRecord data) {
        return MessageRecordCoder.getInstance().encode(data);
    }

}
