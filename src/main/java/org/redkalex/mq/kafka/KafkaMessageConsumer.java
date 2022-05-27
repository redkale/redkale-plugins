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
import org.apache.kafka.common.serialization.*;
import org.redkale.mq.*;
import org.redkale.util.Traces;

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

    protected boolean autoCommit;

    protected final Object resumeLock = new Object();

    protected final boolean finest;

    protected final boolean finer;

    protected final boolean fine;

    public KafkaMessageConsumer(MessageAgent agent, String[] topics, String group,
        MessageProcessor processor, String servers, Properties consumerConfig) {
        super(agent, topics, group, processor);
        final Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerid);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");// 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.putAll(consumerConfig);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        this.autoCommit = "true".equalsIgnoreCase(props.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true").toString());
        this.config = props;
        this.finest = logger.isLoggable(Level.FINEST);
        this.finer = logger.isLoggable(Level.FINER);
        this.fine = logger.isLoggable(Level.FINE);
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
        this.consumer = new KafkaConsumer<>(this.config, new StringDeserializer(), new MessageRecordDeserializer(messageAgent.getMessageCoder()));
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
                if (!this.autoCommit) {
                    long cs = System.currentTimeMillis();
                    consumer.commitAsync((map, exp) -> {
                        if (exp != null) logger.log(Level.SEVERE, Arrays.toString(this.topics) + " consumer error: " + map, exp);
                    });
                    long ce = System.currentTimeMillis() - cs;
                    if (finest && ce > 100) logger.log(Level.FINEST, MessageProcessor.class.getSimpleName() + " processor async commit in " + ce + "ms");
                }
                long s = System.currentTimeMillis();
                MessageRecord msg = null;
                int count = records0.count();
                try {
                    processor.begin(count, s);
                    for (ConsumerRecord<String, MessageRecord> r : records0) {
                        msg = r.value();
                        Traces.currTraceid(msg.getTraceid());
                        processor.process(msg, null);
                    }
                    processor.commit();
                    long e = System.currentTimeMillis() - s;
                    if (finest && e > 10) logger.log(Level.FINEST, MessageProcessor.class.getSimpleName() + Arrays.toString(this.topics) + " processor run " + count + " records" + (count == 1 && msg != null ? ("(seqid=" + msg.getSeqid() + ")") : "") + " in " + e + "ms");
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, MessageProcessor.class.getSimpleName() + " process " + msg + " error", e);
                }
            }
            ConsumerRecords<String, MessageRecord> records;
            while (!this.closed) {
                try {
                    records = this.consumer.poll(Duration.ofMillis(10_000));
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
                if (!this.autoCommit) {
                    long cs = System.currentTimeMillis();
                    this.consumer.commitAsync((map, exp) -> {
                        if (exp != null) logger.log(Level.SEVERE, Arrays.toString(this.topics) + " consumer commitAsync error: " + map, exp);
                    });
                    long ce = System.currentTimeMillis() - cs;
                    if (finest && ce > 100) logger.log(Level.FINEST, MessageProcessor.class.getSimpleName() + " processor async commit in " + ce + "ms");
                }
                long s = System.currentTimeMillis();
                MessageRecord msg = null;
                try {
                    processor.begin(count, s);
                    for (ConsumerRecord<String, MessageRecord> r : records) {
                        msg = r.value();
                        Traces.currTraceid(msg.getTraceid());
                        processor.process(msg, null);
                    }
                    processor.commit();
                    long e = System.currentTimeMillis() - s;
                    if (finest && e > 10) logger.log(Level.FINEST, MessageProcessor.class.getSimpleName() + Arrays.toString(this.topics) + " processor run " + count + " records" + (count == 1 && msg != null ? ("(seqid=" + msg.getSeqid() + ")") : "") + " in " + e + "ms");
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, MessageProcessor.class.getSimpleName() + " process " + msg + " error", e);
                }
                long e = System.currentTimeMillis() - s;
                if (e > 1000 && fine) {
                    logger.log(Level.FINE, "Kafka." + processor.getClass().getSimpleName() + ".consumer (mqs.count = " + count + ", mqs.costs = " + e + " ms)， msg=" + msg);
                } else if (e > 100 && finer) {
                    logger.log(Level.FINER, "Kafka." + processor.getClass().getSimpleName() + ".consumer (mq.count = " + count + ", mq.costs = " + e + " ms)， msg=" + msg);
                } else if (finest) {
                    logger.log(Level.FINEST, "Kafka." + processor.getClass().getSimpleName() + ".consumer (mq.count = " + count + ", mq.cost = " + e + " ms)");
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

    public static class MessageRecordDeserializer implements Deserializer<MessageRecord> {

        private final MessageCoder<MessageRecord> coder;

        public MessageRecordDeserializer(MessageCoder<MessageRecord> coder) {
            this.coder = coder;
        }

        @Override
        public MessageRecord deserialize(String topic, byte[] data) {
            return coder.decode(data);
        }

    }
}
