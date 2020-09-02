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
public class PulsarMessageConsumer extends MessageConsumer implements Runnable {

    protected Properties config;

    protected Thread thread;

    protected CompletableFuture<Void> startFuture;

    protected CompletableFuture<Void> closeFuture;

    protected Consumer<MessageRecord> consumer;

    protected boolean reconnecting;

    protected final Object resumeLock = new Object();

    public PulsarMessageConsumer(MessageAgent agent, String[] topics, String group,
        MessageProcessor processor, String servers, Properties consumerConfig) {
        super(agent, topics, group, processor);
        final Properties props = new Properties();
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerid);
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageRecordDeserializer.class);
//        props.putAll(consumerConfig);
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.config = props;
    }

    public void retryConnect() {
        if (!this.reconnecting) return;
        try {
            ConsumerBuilder<MessageRecord> cb = ((PulsarMessageAgent) messageAgent).client.newConsumer(PulsarMessageAgent.MessageRecordSchema.INSTANCE);
            this.consumer = cb.topic(this.topics).subscribe();
        } catch (Exception e) {
            return;
        }
        synchronized (this.resumeLock) {
            this.resumeLock.notifyAll();
        }
    }

    @Override
    public void run() {
        final PulsarMessageAgent agent = (PulsarMessageAgent) messageAgent;
        ConsumerBuilder<MessageRecord> cb = agent.client.newConsumer(PulsarMessageAgent.MessageRecordSchema.INSTANCE);
        try {
            this.consumer = cb.topic(this.topics).subscribe();
            this.startFuture.complete(null);
            if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " [" + Arrays.toString(this.topics) + "] startuped");

            Messages<MessageRecord> pulsarmsgs;
            while (!this.closed) {
                try {
                    pulsarmsgs = this.consumer.batchReceive();
                } catch (Exception ex) {
                    logger.log(Level.WARNING, getClass().getSimpleName() + " poll error", ex);
                    this.consumer.close();
                    this.consumer = null;
                    if (!this.closed) {
                        this.reconnecting = true;
                        ((PulsarMessageAgent) messageAgent).startReconnect();
                        synchronized (resumeLock) {
                            resumeLock.wait();
                        }
                    }
                    continue;
                }
                if (this.reconnecting) this.reconnecting = false;
                if (pulsarmsgs == null || pulsarmsgs.size() < 1) continue;
                final Messages<MessageRecord> localmsg = pulsarmsgs;
                this.consumer.acknowledgeAsync(pulsarmsgs).whenComplete((r, exp) -> {
                    if (exp != null) logger.log(Level.SEVERE, Arrays.toString(this.topics) + " consumer error: " + localmsg, exp);
                });
                MessageRecord msg = null;
                try {
                    processor.begin(pulsarmsgs.size());
                    for (Message<MessageRecord> mmr : pulsarmsgs) {
                        msg = mmr.getValue();
                        processor.process(msg, null);
                    }
                    processor.commit();
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, MessageProcessor.class.getSimpleName() + " process " + msg + " error", e);
                }
            }
            if (this.consumer != null) this.consumer.close();
            if (this.closeFuture != null) {
                this.closeFuture.complete(null);
                if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " [" + Arrays.toString(this.topics) + "] shutdowned");
            }
        } catch (Throwable t) {
            if (this.closeFuture != null && !this.closeFuture.isDone()) {
                this.closeFuture.complete(null);
                if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " [" + Arrays.toString(this.topics) + "] shutdowned");
            }
            logger.log(Level.SEVERE, MessageConsumer.class.getSimpleName() + "(" + Arrays.toString(this.topics) + ") occur error", t);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> startup() {
        if (this.startFuture != null) return this.startFuture;
        this.thread = new Thread(this);
        this.thread.setName("MQ-" + consumerid + "-Thread");
        this.startFuture = new CompletableFuture<>();
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " [" + Arrays.toString(this.topics) + "] startuping");
        this.thread.start();
        return this.startFuture;
    }

    @Override
    public synchronized CompletableFuture<Void> shutdown() {
        if (this.consumer == null) return CompletableFuture.completedFuture(null);
        if (this.closeFuture != null) return this.closeFuture;
        this.closeFuture = new CompletableFuture<>();
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " [" + Arrays.toString(this.topics) + "] shutdownling");
        this.closed = true;
        synchronized (resumeLock) {
            resumeLock.notifyAll();
        }
        return this.closeFuture;
    }

}
