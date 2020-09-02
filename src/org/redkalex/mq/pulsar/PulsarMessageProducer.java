/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.pulsar;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import org.apache.pulsar.client.api.*;
import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class PulsarMessageProducer extends MessageProducer implements Runnable {

    protected MessageAgent messageAgent;

    protected Thread thread;

    protected CompletableFuture<Void> startFuture;

    protected final ConcurrentHashMap<String, Integer[]> partionsMap = new ConcurrentHashMap<>();

    protected int partitions;

    protected boolean reconnecting;

    protected final Object resumeLock = new Object();

    public PulsarMessageProducer(String name, MessageAgent messageAgent, String servers, int partitions, Properties producerConfig) {
        super(name, messageAgent.getLogger());
        this.partitions = partitions;
        Objects.requireNonNull(messageAgent);
        this.messageAgent = messageAgent;
    }

    public void retryConnect() {
    }

    @Override
    public void run() {
        ProducerBuilder pb = ((PulsarMessageAgent) messageAgent).client.newProducer(PulsarMessageAgent.MessageRecordSchema.INSTANCE);
        this.startFuture.complete(null);
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageProducer.class.getSimpleName() + "(name=" + this.name + ") startuped");
    }

    @Override
    public CompletableFuture<Void> apply(MessageRecord message) {
        if (closed) throw new IllegalStateException(this.getClass().getSimpleName() + "(name=" + name + ") is closed when send " + message);
        final PulsarMessageAgent agent = (PulsarMessageAgent) messageAgent;
        Producer<MessageRecord> producer = agent.producers.get(message.getTopic());
        if (producer != null) return producer.sendAsync(message).thenApply(r -> null);
        ProducerBuilder<MessageRecord> pb = agent.client.newProducer(PulsarMessageAgent.MessageRecordSchema.INSTANCE);
        pb.topic(message.getTopic());
        return pb.createAsync().thenCompose(p -> {
            Producer<MessageRecord> old = agent.producers.put(message.getTopic(), p);
            if (old != null) old.closeAsync();
            return p.sendAsync(message).thenApply(r -> null);
        });
    }

    @Override
    public synchronized CompletableFuture<Void> startup() {
        if (this.startFuture != null) return this.startFuture;
        this.thread = new Thread(this);
        this.thread.setName("MQ-Producer-Thread");
        this.startFuture = new CompletableFuture<>();
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageProducer.class.getSimpleName() + " [" + this.name + "] startuping");
        this.thread.start();
        return this.startFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> shutdown() {
        if (!this.closed) return CompletableFuture.completedFuture(null);
        this.closed = true;
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageProducer.class.getSimpleName() + " [" + this.name + "] shutdown");
        return CompletableFuture.completedFuture(null);
    }

}
