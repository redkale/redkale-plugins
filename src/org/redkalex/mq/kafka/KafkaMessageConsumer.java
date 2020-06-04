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
import org.apache.kafka.common.errors.InvalidTopicException;
import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageConsumer extends MessageConsumer implements Runnable {

    protected Properties config;

    protected Thread thread;

    protected CompletableFuture<Void> startFuture;

    protected KafkaConsumer<String, MessageRecord> consumer;

    public KafkaMessageConsumer(MessageAgent agent, String topic, MessageProcessor processor, Properties config) {
        super(agent, topic, processor);
        this.config = config;
    }

    @Override
    public void run() {
        this.consumer = new KafkaConsumer<>(this.config);
        consumer.subscribe(Arrays.asList(this.topic));
        ConsumerRecords<String, MessageRecord> records0 = null;
        try {
            records0 = consumer.poll(Duration.ofMillis(1));
        } catch (InvalidTopicException e) {
            agent.createTopic(this.topic);
        }
        this.startFuture.complete(null);
        try {
            if (records0 != null && records0.count() > 0) {
                consumer.commitAsync((map, exp) -> {
                    if (exp != null) logger.log(Level.SEVERE, topic + " consumer error: " + map, exp);
                });
                for (ConsumerRecord<String, MessageRecord> r : records0) {
                    processor.process(r.value());
                }
            }
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
        } catch (Throwable t) {
            logger.log(Level.SEVERE, MessageConsumer.class.getSimpleName() + "(" + topic + ") occur error", t);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> startup() {
        if (this.startFuture != null) return this.startFuture;
        this.thread = new Thread(this);
        this.thread.setName("MQ-" + topic + "-Consumer-Thread");
        this.startFuture = new CompletableFuture<>();
        this.thread.start();
        return this.startFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> shutdown() {
        if (!this.closed) return CompletableFuture.completedFuture(null);
        this.closed = true;
        if (this.consumer != null) this.consumer.close();
        return CompletableFuture.completedFuture(null);
    }

}
