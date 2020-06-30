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
import org.apache.kafka.common.serialization.Deserializer;
import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageConsumer extends MessageConsumer implements Runnable {

    protected Properties config;

    protected Thread thread;

    protected CompletableFuture<Void> startFuture;

    protected CompletableFuture<Void> closeFuture;

    protected KafkaConsumer<String, MessageRecord> consumer;

    protected boolean reconnecting;

    protected final Object resumeLock = new Object();

    public KafkaMessageConsumer(MessageAgent agent, String[] topics, String group,
        MessageProcessor processor, String servers, Properties consumerConfig) {
        super(agent, topics, group, processor);
        final Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerid);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageRecordDeserializer.class);
        props.putAll(consumerConfig);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.config = props;
    }

    public void retryConnect() {
        if (!this.reconnecting) return;
        KafkaConsumer one = new KafkaConsumer<>(this.config);
        one.subscribe(Arrays.asList(this.topics));
        this.consumer = one;
        synchronized (this.resumeLock) {
            this.resumeLock.notifyAll();
        }
    }

    @Override
    public void run() {
        this.consumer = new KafkaConsumer<>(this.config);
        this.consumer.subscribe(Arrays.asList(this.topics));
        ConsumerRecords<String, MessageRecord> records0 = null;
        try {
            records0 = consumer.poll(Duration.ofMillis(1));
        } catch (InvalidTopicException e) {
            messageAgent.createTopic(this.topics);
        }
        this.startFuture.complete(null);
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " [" + Arrays.toString(this.topics) + "] startuped");

        try {
            if (records0 != null && records0.count() > 0) {
                consumer.commitAsync((map, exp) -> {
                    if (exp != null) logger.log(Level.SEVERE, Arrays.toString(this.topics) + " consumer error: " + map, exp);
                });
                for (ConsumerRecord<String, MessageRecord> r : records0) {
                    processor.process(r.value(), null);
                }
            }
            ConsumerRecords<String, MessageRecord> records;
            while (!this.closed) {
                try {
                    records = this.consumer.poll(Duration.ofMillis(10));
                } catch (Exception ex) {
                    logger.log(Level.WARNING, getClass().getSimpleName() + " poll error", ex);
                    this.consumer.close();
                    this.consumer = null;
                    if (!this.closed) {
                        this.reconnecting = true;
                        ((KafkaMessageAgent) messageAgent).startReconnect();
                        synchronized (resumeLock) {
                            resumeLock.wait();
                        }
                    }
                    continue;
                }
                if (this.reconnecting) this.reconnecting = false;
                int count = records.count();
                if (count == 0) continue;
                this.consumer.commitAsync((map, exp) -> {
                    if (exp != null) logger.log(Level.SEVERE, Arrays.toString(this.topics) + " consumer error: " + map, exp);
                });
                MessageRecord msg = null;
                try {
                    processor.begin(count);
                    for (ConsumerRecord<String, MessageRecord> r : records) {
                        msg = r.value();
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
        if (this.closeFuture != null) return this.closeFuture;
        this.closeFuture = new CompletableFuture<>();
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " [" + Arrays.toString(this.topics) + "] shutdownling");
        this.closed = true;
        synchronized (resumeLock) {
            resumeLock.notifyAll();
        }
        return this.closeFuture;
    }

    public static class MessageRecordDeserializer implements Deserializer<MessageRecord> {

        @Override
        public MessageRecord deserialize(String topic, byte[] data) {
            return MessageRecordCoder.getInstance().decode(data);
        }

    }
}
