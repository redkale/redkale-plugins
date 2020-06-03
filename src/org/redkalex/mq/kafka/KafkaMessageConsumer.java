/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import org.apache.kafka.clients.consumer.*;
import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageConsumer extends MessageConsumer {

    protected Properties config;

    protected CountDownLatch cdl = new CountDownLatch(1);

    protected KafkaConsumer<String, MessageRecord> consumer;

    public KafkaMessageConsumer(String topic, MessageProcessor processor, Properties config) {
        super(topic, processor);
        this.config = config;
    }

    @Override
    public void run() {
        this.consumer = new KafkaConsumer<>(this.config);
        consumer.subscribe(Arrays.asList(this.topic));
        cdl.countDown();
        while (!this.closed) {
            ConsumerRecords<String, MessageRecord> records = consumer.poll(Duration.ofMillis(10));
            if (records.count() == 0) continue;
            consumer.commitAsync((map, exp) -> {
                if (exp != null) logger.log(Level.SEVERE, topic + " consumer error: " + map, exp);
            });
            for (ConsumerRecord<String, MessageRecord> r : records) {
                processor.process(r.value());
            }
        }
    }

    @Override
    public void waitFor() {
        try {
            this.cdl.await(3, TimeUnit.SECONDS);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        if (!this.closed) return;
        if (this.consumer != null) this.consumer.close();
    }

}
