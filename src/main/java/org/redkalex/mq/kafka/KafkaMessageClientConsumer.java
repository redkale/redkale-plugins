/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.*;
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
public class KafkaMessageClientConsumer extends MessageClientConsumer implements Runnable {

    protected Properties config;

    protected Thread thread;

    protected KafkaConsumer<String, MessageRecord> consumer;

    private CompletableFuture<Void> startFuture;

    private CompletableFuture<Void> closeFuture;
    
    protected boolean reconnecting;

    protected boolean autoCommit;

    private final ReentrantLock startCloseLock = new ReentrantLock();

    public KafkaMessageClientConsumer(KafkaMessageAgent messageAgent, String[] topics, String group, MessageClientProcessor processor) {
        super(messageAgent, topics, group, processor);
        final Properties props = messageAgent.createConsumerProperties(consumerid);
        this.autoCommit = "true".equalsIgnoreCase(props.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true").toString());
        this.config = props;
    }

    @Override
    public void run() {
        this.consumer = new KafkaConsumer<>(this.config, new StringDeserializer(), new MessageRecordDeserializer(messageAgent.getClientMessageCoder()));
        this.consumer.subscribe(Arrays.asList(this.topics));
        this.startFuture.complete(null);
        ConsumerRecords<String, MessageRecord> records0 = null;
        try {
            records0 = consumer.poll(Duration.ofMillis(1));
        } catch (InvalidTopicException e) {
            messageAgent.createTopic(this.topics);
        }
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, MessageClientConsumer.class.getSimpleName() + " " + Arrays.toString(this.topics) + " startuped");
        }

        try {
            if (records0 != null && records0.count() > 0) {
                if (!this.autoCommit) {
                    long cs = System.currentTimeMillis();
                    consumer.commitAsync((map, exp) -> {
                        if (exp != null) {
                            logger.log(Level.SEVERE, Arrays.toString(this.topics) + " consumer error: " + map, exp);
                        }
                    });
                    long ce = System.currentTimeMillis() - cs;
                    if (logger.isLoggable(Level.FINEST) && ce > 100) {
                        logger.log(Level.FINEST, MessageClientProcessor.class.getSimpleName() + " processor async commit in " + ce + "ms");
                    }
                }
                long s = System.currentTimeMillis();
                MessageRecord msg = null;
                int count = records0.count();
                try {
                    processor.begin(count, s);
                    for (ConsumerRecord<String, MessageRecord> r : records0) {
                        msg = r.value();
                        Traces.computeIfAbsent(msg.getTraceid());
                        processor.process(msg, null);
                    }
                    processor.commit();
                    long e = System.currentTimeMillis() - s;
                    if (logger.isLoggable(Level.FINEST) && e > 10) {
                        logger.log(Level.FINEST, MessageClientProcessor.class.getSimpleName() + Arrays.toString(this.topics) + " processor run " + count + " records" + (count == 1 && msg != null ? ("(seqid=" + msg.getSeqid() + ")") : "") + " in " + e + "ms");
                    }
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, MessageClientProcessor.class.getSimpleName() + " process " + msg + " error", e);
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
                    continue;
                }
                if (this.reconnecting) {
                    this.reconnecting = false;
                }
                int count = records.count();
                if (count == 0) {
                    continue;
                }
                if (!this.autoCommit) {
                    long cs = System.currentTimeMillis();
                    this.consumer.commitAsync((map, exp) -> {
                        if (exp != null) {
                            logger.log(Level.SEVERE, Arrays.toString(this.topics) + " consumer commitAsync error: " + map, exp);
                        }
                    });
                    long ce = System.currentTimeMillis() - cs;
                    if (logger.isLoggable(Level.FINEST) && ce > 100) {
                        logger.log(Level.FINEST, MessageClientProcessor.class.getSimpleName() + " processor async commit in " + ce + "ms");
                    }
                }
                long s = System.currentTimeMillis();
                MessageRecord msg = null;
                try {
                    processor.begin(count, s);
                    for (ConsumerRecord<String, MessageRecord> r : records) {
                        msg = r.value();
                        Traces.computeIfAbsent(msg.getTraceid());
                        processor.process(msg, null);
                    }
                    processor.commit();
                    long e = System.currentTimeMillis() - s;
                    if (logger.isLoggable(Level.FINEST) && e > 10) {
                        logger.log(Level.FINEST, MessageClientProcessor.class.getSimpleName() + Arrays.toString(this.topics) + " processor run " + count + " records" + (count == 1 && msg != null ? ("(seqid=" + msg.getSeqid() + ")") : "") + " in " + e + "ms");
                    }
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, MessageClientProcessor.class.getSimpleName() + " process " + msg + " error", e);
                }
                long e = System.currentTimeMillis() - s;
                if (e > 1000 && logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "Kafka." + processor.getClass().getSimpleName() + ".consumer (mqs.count = " + count + ", mqs.costs = " + e + " ms)， msg=" + msg);
                } else if (e > 100 && logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "Kafka." + processor.getClass().getSimpleName() + ".consumer (mq.count = " + count + ", mq.costs = " + e + " ms)， msg=" + msg);
                } else if (logger.isLoggable(Level.FINEST)) {
                    logger.log(Level.FINEST, "Kafka." + processor.getClass().getSimpleName() + ".consumer (mq.count = " + count + ", mq.cost = " + e + " ms)");
                }
            }
            if (this.consumer != null) {
                this.consumer.close();
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, MessageClientConsumer.class.getSimpleName() + " " + Arrays.toString(this.topics) + " shutdowned");
            }
        } catch (Throwable t) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, MessageClientConsumer.class.getSimpleName() + " " + Arrays.toString(this.topics) + " shutdowned");
            }
            logger.log(Level.SEVERE, MessageClientConsumer.class.getSimpleName() + " " + Arrays.toString(this.topics) + " occur error", t);
        }
    }

    @Override
    public void start() {
        startCloseLock.lock();
        try {
            this.thread = new Thread(this);
            this.thread.setName(MessageClientConsumer.class.getSimpleName() + "-" + consumerid + "-Thread");
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, MessageClientConsumer.class.getSimpleName() + " " + Arrays.toString(this.topics) + " startuping");
            }
            this.startFuture = new CompletableFuture<>();
            this.thread.start();
            this.startFuture.join();
        } finally {
            startCloseLock.unlock();
        }
    }

    @Override
    public void stop() {
        startCloseLock.lock();
        try {
            if (this.closeFuture != null) {
                this.closeFuture.join();
                return;
            }
            if (this.consumer == null || this.closed) {
                return;
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, MessageClientConsumer.class.getSimpleName() + " " + Arrays.toString(this.topics) + " shutdownling");
            }
            this.closeFuture = new CompletableFuture<>();
            this.closed = true;
            this.closeFuture.join();
        } finally {
            startCloseLock.unlock();
        }
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
