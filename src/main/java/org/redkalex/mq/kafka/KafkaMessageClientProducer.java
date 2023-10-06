/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.*;
import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageClientProducer extends MessageClientProducer implements Runnable {

    protected MessageAgent messageAgent;

    protected Properties config;

    protected Thread thread;

    protected KafkaProducer<String, MessageRecord> producer;

    protected final ConcurrentHashMap<String, Integer[]> partionsMap = new ConcurrentHashMap<>();

    protected int partitions;

    protected boolean reconnecting;

    protected final ReentrantLock resumeLock = new ReentrantLock();

    private final ReentrantLock startCloseLock = new ReentrantLock();

    public KafkaMessageClientProducer(KafkaMessageAgent messageAgent, String producerName, int partitions) {
        super(producerName);
        this.partitions = partitions;
        Objects.requireNonNull(messageAgent);
        this.messageAgent = messageAgent;
        this.config = messageAgent.createProducerProperties();
    }

    public void retryConnect() {

    }

    @Override
    public void run() {
        this.producer = new KafkaProducer<>(this.config, new StringSerializer(), new MessageRecordSerializer(messageAgent.getClientMessageCoder()));
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, MessageClientProducer.class.getSimpleName() + "(name=" + this.name + ") startuped");
        }
    }

    @Override
    public CompletableFuture<Void> apply(MessageRecord message) {
        if (closed.get()) {
            throw new IllegalStateException(this.getClass().getSimpleName() + "(name=" + name + ") is closed when send " + message);
        }
        if (this.producer == null) {
            throw new IllegalStateException(this.getClass().getSimpleName() + "(name=" + name + ") not started when send " + message);
        }
        final CompletableFuture future = new CompletableFuture();
        Integer partition = null;
        if (this.partitions > 0) {    //不指定 partition则设计上需要以对等为主
            if (message.getGroupid() != null && !message.getGroupid().isEmpty()) {
                partition = Math.abs(message.getGroupid().hashCode()) % this.partitions;
            } else if (message.getUserid() != null) {
                partition = Math.abs(message.getUserid().hashCode()) % this.partitions;
            }
        }
        final Integer partition0 = partition;
        //if (finest) logger.log(Level.FINEST, "Kafka.producer prepare send partition=" + partition0 + ", msg=" + message);
        producer.send(new ProducerRecord<>(message.getTopic(), partition, null, message), (metadata, exp) -> {
            if (exp != null) {
                future.completeExceptionally(exp);
            } else {
                future.complete(null);
            }

            long e = System.currentTimeMillis() - message.getCreateTime();
            if (e > 1000 && logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, "Kafka.producer (mqs.costs = " + e + " ms)，partition=" + partition0 + ", msg=" + message);
            } else if (e > 100 && logger.isLoggable(Level.FINER)) {
                logger.log(Level.FINER, "Kafka.producer (mq.costs = " + e + " ms)，partition=" + partition0 + ", msg=" + message);
            } else if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "Kafka.producer (mq.cost = " + e + " ms)，partition=" + partition0 + ", msg=" + message);
            }
        });
        return future;
    }

    protected Integer[] loadTopicPartition(String topic0) {
        return partionsMap.computeIfAbsent(topic0, topic -> {
            try {
                AdminClient adminClient = ((KafkaMessageAgent) messageAgent).adminClient;
                DescribeTopicsResult rs = adminClient.describeTopics(Arrays.asList(topic));
                List<TopicPartitionInfo> list = rs.topicNameValues().get(topic).get(6, TimeUnit.SECONDS).partitions();
                Integer[] parts = new Integer[list.size()];
                for (int i = 0; i < parts.length; i++) {
                    parts[i] = list.get(i).partition();
                }
                Arrays.sort(parts);
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "Topic(" + topic + ") load partitions = " + list);
                }
                return parts;
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Topic(" + topic + ")  load partitions error", ex);
                return new Integer[0];
            }
        });
    }

    @Override
    public void startup() {
        startCloseLock.lock();
        try {
            this.thread = new Thread(this);
            this.thread.setName("MQ-Producer-Thread");
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, MessageClientProducer.class.getSimpleName() + " [" + this.name + "] startuping");
            }
            this.thread.start();
        } finally {
            startCloseLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        startCloseLock.lock();
        try {
            if (this.closed.compareAndSet(false, true)) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, MessageClientProducer.class.getSimpleName() + " [" + this.name + "] shutdowning");
                }
                if (this.producer != null) {
                    this.producer.close();
                }
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, MessageClientProducer.class.getSimpleName() + " [" + this.name + "] shutdowned");
                }
            }
        } finally {
            startCloseLock.unlock();
        }
    }

    public static class MessageRecordSerializer implements Serializer<MessageRecord> {

        private final MessageCoder<MessageRecord> coder;

        public MessageRecordSerializer(MessageCoder<MessageRecord> coder) {
            this.coder = coder;
        }

        @Override
        public byte[] serialize(String topic, MessageRecord data) {
            return this.coder.encode(data);
        }

    }

}
