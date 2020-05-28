/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.redkale.convert.ConvertType;
import org.redkale.mq.MessageRecord;

/**
 *
 * @author zhangjx
 */
public class MessageRecordDeserializer implements org.apache.kafka.common.serialization.Deserializer<MessageRecord> {

    @Override
    public MessageRecord deserialize(String topic, byte[] data) {
        if (data == null) return null;
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long seqid = buffer.getLong();
        ConvertType format = ConvertType.find(buffer.getInt());
        int flag = buffer.getInt();
        int userid = buffer.getInt();

        byte[] groupid = null;
        int groupidlen = buffer.getChar();
        if (groupidlen > 0) {
            groupid = new byte[groupidlen];
            buffer.get(groupid);
        }

        byte[] stopics = null;
        int stopiclen = buffer.getChar();
        if (stopiclen > 0) {
            stopics = new byte[stopiclen];
            buffer.get(stopics);
        }

        byte[] dtopics = null;
        int dtopiclen = buffer.getChar();
        if (dtopiclen > 0) {
            dtopics = new byte[dtopiclen];
            buffer.get(dtopics);
        }

        byte[] content = null;
        int contentlen = buffer.getInt();
        if (contentlen > 0) {
            content = new byte[contentlen];
            buffer.get(content);
        }
        return new MessageRecord(seqid, format, flag, userid,
            groupid == null ? null : new String(groupid, StandardCharsets.UTF_8),
            stopics == null ? null : new String(stopics, StandardCharsets.UTF_8),
            dtopics == null ? null : new String(dtopics, StandardCharsets.UTF_8),
            content);
    }

}
