/*
 *
 */
package org.redkalex.mq.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.redkale.mq.MessageAgent.MessageConsumerWrapper;
import org.redkale.mq.MessageClientProcessor;
import org.redkale.mq.MessageConext;
import org.redkale.mq.MessageConsumer;

/**
 *
 * @author zhangjx
 */
class KafkaMessageConsumer implements Runnable {

    private final ReentrantLock startCloseLock = new ReentrantLock();

    private final KafkaMessageAgent messageAgent;

    private final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private final Map<String, MessageConsumerWrapper> consumerMap;

    private final String group;

    private final List<String> topics;

    private KafkaConsumer<String, byte[]> consumer;

    private Thread thread;

    private boolean closed;

    protected KafkaMessageConsumer(KafkaMessageAgent messageAgent, String group, Map<String, MessageConsumerWrapper> consumerMap) {
        this.messageAgent = messageAgent;
        this.group = group;
        this.consumerMap = consumerMap;
        this.topics = new ArrayList<>(consumerMap.keySet());
    }

    @Override
    public void run() {
        this.consumer = new KafkaConsumer<>(this.messageAgent.createConsumerProperties(this.group), new StringDeserializer(), new ByteArrayDeserializer());
        this.consumer.subscribe(this.topics);
        try {
            Map<String, Map<Integer, List<byte[]>>> map = new LinkedHashMap<>();
            Map<String, Map<Integer, MessageConext>> contexts = new LinkedHashMap<>();
            ConsumerRecords<String, byte[]> records;
            while (!this.closed) {
                try {
                    records = this.consumer.poll(Duration.ofMillis(10_000));
                } catch (Exception ex) {
                    logger.log(Level.WARNING, getClass().getSimpleName() + " poll error", ex);
                    break;
                }
                int count = records.count();
                if (count == 0) {
                    continue;
                }
                map.clear();
                long s = System.currentTimeMillis();
                try {
                    for (ConsumerRecord<String, byte[]> r : records) {
                        map.computeIfAbsent(r.topic(), t -> new LinkedHashMap<>()).computeIfAbsent(r.partition(), p -> new ArrayList<>()).add(r.value());
                    }
                    map.forEach((topic, items) -> {
                        MessageConsumerWrapper wrapper = consumerMap.get(topic);
                        if (wrapper != null) {
                            items.forEach((partition, list) -> {
                                MessageConext context = contexts.computeIfAbsent(topic, t -> new HashMap<>()).computeIfAbsent(partition, p -> messageAgent.createMessageConext(topic, p));
                                wrapper.onMessage(context, list);
                            });
                        }
                    });
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, MessageClientProcessor.class.getSimpleName() + " process " + map + " error", e);
                }
                long e = System.currentTimeMillis() - s;
                if (e > 1000 && logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "Kafka." + MessageConsumer.class.getSimpleName() + ".consumer (mqs.count = " + count + ", mqs.costs = " + e + " ms)， msgs=" + map);
                } else if (e > 100 && logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "Kafka." + MessageConsumer.class.getSimpleName() + ".consumer (mq.count = " + count + ", mq.costs = " + e + " ms)， msgs=" + map);
                } else if (logger.isLoggable(Level.FINEST)) {
                    logger.log(Level.FINEST, "Kafka." + MessageConsumer.class.getSimpleName() + ".consumer (mq.count = " + count + ", mq.cost = " + e + " ms)");
                }
                map.clear();
            }
            if (this.consumer != null) {
                this.consumer.close();
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + " " + this.topics + " shutdowned");
            }
        } catch (Throwable t) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + " " + this.topics + " shutdowned");
            }
            logger.log(Level.SEVERE, getClass().getSimpleName() + "" + this.topics + " occur error", t);
        }
    }

    public void start() {
        startCloseLock.lock();
        try {
            this.thread = new Thread(this);
            this.thread.setName(MessageConsumer.class.getSimpleName() + "-[" + group + "]-Thread");
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " " + this.topics + " startuping");
            }
            this.thread.start();
        } finally {
            startCloseLock.unlock();
        }
    }

    public void stop() {
        startCloseLock.lock();
        try {
            if (this.consumer == null || this.closed) {
                return;
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, MessageConsumer.class.getSimpleName() + " " + this.topics + " shutdownling");
            }
            this.closed = true;
        } finally {
            startCloseLock.unlock();
        }
    }

}
