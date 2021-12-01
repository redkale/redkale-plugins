/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.Serializer;
import org.redkale.mq.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageProducer extends MessageProducer implements Runnable {

    protected MessageAgent messageAgent;

    protected Properties config;

    protected Thread thread;

    protected CompletableFuture<Void> startFuture;

    protected KafkaProducer<String, MessageRecord> producer;

    protected final ConcurrentHashMap<String, Integer[]> partionsMap = new ConcurrentHashMap<>();

    protected int partitions;

    protected boolean reconnecting;

    protected final Object resumeLock = new Object();

    protected final boolean finest;

    protected final boolean finer;

    protected final boolean fine;

    public KafkaMessageProducer(String name, MessageAgent messageAgent, String servers, int partitions, Properties producerConfig) {
        super(name, messageAgent.getLogger());
        this.partitions = partitions;
        Objects.requireNonNull(messageAgent);
        this.messageAgent = messageAgent;

        final Properties props = new Properties();
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "0");//all:所有follower都响应了才认为消息提交成功，即"committed"
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageRecordSerializer.class);
        props.putAll(producerConfig);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        this.config = props;
        this.finest = logger.isLoggable(Level.FINEST);
        this.finer = logger.isLoggable(Level.FINER);
        this.fine = logger.isLoggable(Level.FINE);
    }

    public void retryConnect() {

    }

    @Override
    public void run() {
        this.producer = new KafkaProducer<>(this.config);
        this.startFuture.complete(null);
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageProducer.class.getSimpleName() + "(name=" + this.name + ") startuped");
    }

    @Override
    public CompletableFuture<Void> apply(MessageRecord message) {
        if (closed) throw new IllegalStateException(this.getClass().getSimpleName() + "(name=" + name + ") is closed when send " + message);
        if (this.producer == null) throw new IllegalStateException(this.getClass().getSimpleName() + "(name=" + name + ") not started when send " + message);
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

            long e = System.currentTimeMillis() - message.getCreatetime();
            if (e > 1000 && fine) {
                logger.log(Level.FINE, "Kafka.producer (mqs.costs = " + e + " ms)，partition=" + partition0 + ", msg=" + message);
            } else if (e > 100 && finer) {
                logger.log(Level.FINER, "Kafka.producer (mq.costs = " + e + " ms)，partition=" + partition0 + ", msg=" + message);
            } else if (finest) {
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
                List<TopicPartitionInfo> list = rs.values().get(topic).get(6, TimeUnit.SECONDS).partitions();
                Integer[] parts = new Integer[list.size()];
                for (int i = 0; i < parts.length; i++) {
                    parts[i] = list.get(i).partition();
                }
                Arrays.sort(parts);
                if (logger.isLoggable(Level.FINER)) logger.log(Level.FINER, "Topic(" + topic + ") load partitions = " + list);
                return parts;
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "Topic(" + topic + ")  load partitions error", ex);
                return new Integer[0];
            }
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
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageProducer.class.getSimpleName() + " [" + this.name + "] shutdowning");
        if (this.producer != null) this.producer.close();
        if (logger.isLoggable(Level.FINE)) logger.log(Level.FINE, MessageProducer.class.getSimpleName() + " [" + this.name + "] shutdowned");
        return CompletableFuture.completedFuture(null);
    }

    public static class MessageRecordSerializer implements Serializer<MessageRecord> {

        @Override
        public byte[] serialize(String topic, MessageRecord data) {
            return MessageRecordCoder.getInstance().encode(data);
        }

    }

}
