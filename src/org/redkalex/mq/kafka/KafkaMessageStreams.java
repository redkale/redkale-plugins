/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.util.*;
import java.util.function.Function;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageStreams extends MessageStreams {

    protected Properties config;

    protected KafkaStreams streams;

    public KafkaMessageStreams(String topic, Function<MessageRecord, MessageRecord> processor, Properties config) {
        super(topic, processor);
        this.config = config;
    }

    @Override
    public void run() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, MessageRecord> source = builder.stream(this.topic);
        KStream<String, MessageRecord> sink = source.mapValues(v -> processor.apply(v));
        sink.to((t, v, r) -> v.getResptopic());
        this.streams = new KafkaStreams(builder.build(), config);
        this.streams.start();
    }

    @Override
    public void close() {
        if (!this.closed) return;
        if (this.streams != null) this.streams.close();
    }

}
