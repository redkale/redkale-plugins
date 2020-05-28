/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.redkale.mq.MessageRecord;

/**
 *
 * @author zhangjx
 */
public class MessageRecordSerializer implements org.apache.kafka.common.serialization.Serializer<MessageRecord> {

    public static final byte[] EMPTY_BYTES = new byte[0];

    @Override
    public byte[] serialize(String topic, MessageRecord data) {
        if (data == null) return null;
        byte[] stopics = data.getTopic() == null ? EMPTY_BYTES : data.getTopic().getBytes(StandardCharsets.UTF_8);
        byte[] dtopics = data.getResptopic() == null ? EMPTY_BYTES : data.getResptopic().getBytes(StandardCharsets.UTF_8);
        byte[] groupid = data.getGroupid() == null ? EMPTY_BYTES : data.getGroupid().getBytes(StandardCharsets.UTF_8);
        int count = 8 + 4 + 4 + 4 + 2 + stopics.length + 2 + dtopics.length + 2 + groupid.length + 4 + (data.getContent() == null ? 0 : data.getContent().length);
        final byte[] bs = new byte[count];
        ByteBuffer buffer = ByteBuffer.wrap(bs);
        buffer.putLong(data.getSeqid());
        buffer.putInt(data.getFormat() == null ? 0 : data.getFormat().getValue());
        buffer.putInt(data.getFlag());
        buffer.putInt(data.getUserid());
        buffer.putChar((char) groupid.length);
        if (groupid.length > 0) buffer.put(groupid);
        buffer.putChar((char) stopics.length);
        if (stopics.length > 0) buffer.put(stopics);
        buffer.putChar((char) dtopics.length);
        if (dtopics.length > 0) buffer.put(dtopics);
        if (data.getContent() == null) {
            buffer.putInt(0);
        } else {
            buffer.putInt(data.getContent().length);
            buffer.put(data.getContent());
        }
        return bs;
    }

}
