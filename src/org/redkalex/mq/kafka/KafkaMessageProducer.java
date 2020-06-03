/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.producer.*;
import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageProducer extends MessageProducer {

    protected MessageAgent agent;

    protected Properties config;

    protected CountDownLatch cdl = new CountDownLatch(1);

    protected KafkaProducer<String, MessageRecord> producer;

    public KafkaMessageProducer(MessageAgent agent, Properties config) {
        Objects.requireNonNull(agent);
        this.agent = agent;
        this.config = config;
    }

    @Override
    public void run() {
        this.producer = new KafkaProducer<>(this.config);
        cdl.countDown();
    }

    @Override
    public CompletableFuture apply(MessageRecord message) {
        if (closed) throw new IllegalStateException(this.getClass().getSimpleName() + " is closed when send " + message);
        if (this.producer == null) throw new IllegalStateException(this.getClass().getSimpleName() + " not started when send " + message);
        final CompletableFuture future = new CompletableFuture();
        producer.send(new ProducerRecord<>(message.getTopic(), null, message), (metadata, exp) -> {
            if (exp != null) {
                future.completeExceptionally(exp);
            } else {
                future.complete(null);
            }
        });
        return future;
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
        if (this.producer != null) this.producer.close();
    }

}
