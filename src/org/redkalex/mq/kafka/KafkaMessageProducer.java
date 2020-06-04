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
public class KafkaMessageProducer extends MessageProducer implements Runnable {

    protected MessageAgent agent;

    protected Properties config;

    protected Thread thread;

    protected CompletableFuture<Void> startFuture;

    protected KafkaProducer<String, MessageRecord> producer;

    public KafkaMessageProducer(MessageAgent agent, Properties config) {
        Objects.requireNonNull(agent);
        this.agent = agent;
        this.config = config;
    }

    @Override
    public void run() {
        this.producer = new KafkaProducer<>(this.config);
        this.startFuture.complete(null);
    }

    @Override
    public CompletableFuture<Void> apply(MessageRecord message) {
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
    public synchronized CompletableFuture<Void> startup() {
        if (this.startFuture != null) return this.startFuture;
        this.thread = new Thread(this);
        this.thread.setName("MQ-Producer-Thread");
        this.startFuture = new CompletableFuture<>();
        this.thread.start();
        return this.startFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> shutdown() {
        if (!this.closed) return CompletableFuture.completedFuture(null);
        this.closed = true;
        if (this.producer != null) this.producer.close();
        return CompletableFuture.completedFuture(null);
    }

}
