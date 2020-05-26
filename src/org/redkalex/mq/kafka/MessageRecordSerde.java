/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.redkale.mq.MessageRecord;

/**
 *
 * @author zhangjx
 */
public class MessageRecordSerde extends WrapperSerde<MessageRecord> {

    public MessageRecordSerde() {
        super(new MessageRecordSerializer(), new MessageRecordDeserializer());
    }
}
