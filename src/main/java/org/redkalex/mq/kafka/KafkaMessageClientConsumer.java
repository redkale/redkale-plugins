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
import org.apache.kafka.common.serialization.*;
import org.redkale.mq.*;
import org.redkale.util.Traces;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageClientConsumer extends MessageClientConsumer implements Runnable {

    private Properties config;

    private Thread thread;

    private KafkaConsumer<String, MessageRecord> kafkaConsumer;

    private CompletableFuture<Void> startFuture;

    private CompletableFuture<Void> closeFuture;

    private boolean autoCommit;

    private boolean closed;

    private final ReentrantLock startCloseLock = new ReentrantLock();

    public KafkaMessageClientConsumer(KafkaMessageAgent messageAgent, MessageClient messageClient) {
        super(messageClient);
        final Properties props = messageAgent.createConsumerProperties("redkale-message");
        this.autoCommit = "true".equalsIgnoreCase(props.getOrDefault(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true").toString());
        this.config = props;
    }

    @Override
    public void run() {
        this.kafkaConsumer = new KafkaConsumer<>(this.config, new StringDeserializer(), new MessageRecordDeserializer(messageClient.getClientMessageCoder()));
        Collection<String> topics = getTopics();
        this.kafkaConsumer.subscribe(topics);
        this.startFuture.complete(null);
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") started");
        }
        try {
            MessageProcessor processor = this.messageClient;
            ConsumerRecords<String, MessageRecord> records;
            while (!this.closed) {
                try {
                    records = this.kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                } catch (Throwable ex) {
                    if (!this.closed) {
                        logger.log(Level.WARNING, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") poll error", ex);
                    }
                    break;
                }
                int count = records.count();
                if (count == 0) {
                    continue;
                }
                final boolean finest = logger.isLoggable(Level.FINEST);
                if (!this.autoCommit) {
                    try {
                        long cs = System.currentTimeMillis();
                        this.kafkaConsumer.commitSync(Duration.ofMillis(3000));
                        long ce = System.currentTimeMillis() - cs;
                        if (ce > 1000 && logger.isLoggable(Level.FINE)) {
                            logger.log(Level.FINE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") commitSync cost-slower = " + ce + " ms");
                        } else if (ce > 100 && finest) {
                            logger.log(Level.FINEST, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") commitSync cost-slowly = " + ce + "ms");
                        }
                    } catch (Throwable e) {
                        logger.log(Level.SEVERE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") commitSync error", e);
                    }
                }
                long s = System.currentTimeMillis();
                MessageRecord msg = null;
                for (ConsumerRecord<String, MessageRecord> r : records) {
                    try {
                        msg = r.value();
                        Traces.computeIfAbsent(msg.getTraceid());
                        processor.process(msg, s);
                    } catch (Throwable e) {
                        logger.log(Level.SEVERE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") consumer " + msg + " error", e);
                    }
                }
                long e2 = System.currentTimeMillis() - s;
                if (e2 > 100 && finest) {
                    logger.log(Level.FINEST, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") consumer run " + count + " records" + (count == 1 && msg != null ? ("(seqid=" + msg.getSeqid() + ")") : "") + " in " + e2 + "ms");
                }
                long e = System.currentTimeMillis() - s;
                if (e > 1000 && logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ").consumer (mq.count = " + count + ", mq.cost-slower = " + e + " ms)， msg=" + msg);
                } else if (e > 100 && logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ").consumer (mq.count = " + count + ", mq.cost-slowly = " + e + " ms)， msg=" + msg);
                } else if (finest) {
                    logger.log(Level.FINEST, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ").consumer (mq.count = " + count + ", mq.cost-normal = " + e + " ms)");
                }
            }
            if (this.kafkaConsumer != null) {
                this.kafkaConsumer.close();
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") stoped");
            }
        } catch (Throwable t) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") stoped");
            }
            if (!this.closed) {
                logger.log(Level.SEVERE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") occur error", t);
            }
        } finally {
            if (this.closeFuture != null) {
                this.closeFuture.complete(null);
            }
        }
    }

    @Override
    public void start() {
        startCloseLock.lock();
        try {
            if (messageClient.isEmpty()) {
                this.closed = true;
                return;
            }
            this.thread = new Thread(this);
            this.thread.setName(MessageClientConsumer.class.getSimpleName() + "-" + messageClient.getAppRespTopic() + "-Thread");
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") starting");
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
            if (this.kafkaConsumer == null || this.closed) {
                return;
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(" + messageClient.getProtocol() + "-" + Objects.hashCode(this) + ") stoping");
            }
            this.closeFuture = new CompletableFuture<>();
            this.closed = true;
            this.thread.interrupt();
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
