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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.redkale.mq.MessageAgent.MessageConsumerWrapper;
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

    private CompletableFuture<Void> startFuture;

    private CompletableFuture<Void> closeFuture;

    private boolean autoCommit;

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
        this.startFuture.complete(null);
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, getClass().getSimpleName() + "(topics=" + this.topics + ") started");
        }
        try {
            Map<String, Map<Integer, List<ConsumerRecord<String, byte[]>>>> map = new LinkedHashMap<>();
            Map<String, Map<Integer, MessageConext>> contexts = new LinkedHashMap<>();
            ConsumerRecords<String, byte[]> records;
            while (!this.closed) {
                try {
                    records = this.consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                } catch (Exception ex) {
                    if (!this.closed) {
                        logger.log(Level.WARNING, getClass().getSimpleName() + "(topics=" + this.topics + ") poll error", ex);
                    }
                    break;
                }
                int count = records.count();
                if (count == 0) {
                    continue;
                }
                if (!this.autoCommit) {
                    long cs = System.currentTimeMillis();
                    this.consumer.commitSync();
                    long ce = System.currentTimeMillis() - cs;
                    if (ce > 100 && logger.isLoggable(Level.FINE)) {
                        logger.log(Level.FINE, getClass().getSimpleName() + "(topics=" + this.topics + ") processor async commit in " + ce + "ms");
                    }
                }
                map.clear();
                long s = System.currentTimeMillis();
                try {
                    for (ConsumerRecord<String, byte[]> r : records) {
                        map.computeIfAbsent(r.topic(), t -> new LinkedHashMap<>()).computeIfAbsent(r.partition(), p -> new ArrayList<>()).add(r);
                    }
                    map.forEach((topic, items) -> {
                        MessageConsumerWrapper wrapper = consumerMap.get(topic);
                        if (wrapper != null) {
                            items.forEach((partition, list) -> {
                                MessageConext context = contexts.computeIfAbsent(topic, t -> new HashMap<>()).computeIfAbsent(partition, p -> messageAgent.createMessageConext(topic, p));
                                list.forEach(r -> wrapper.onMessage(context, r.key(), r.value()));
                            });
                        }
                    });
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, getClass().getSimpleName() + "(topics=" + this.topics + ") process " + map + " error", e);
                }
                long e = System.currentTimeMillis() - s;
                if (e > 1000 && logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, getClass().getSimpleName() + "(topics=" + this.topics + ").consumer (mq.count = " + count + ", mq.cost-slower = " + e + " ms)， msgs=" + map);
                } else if (e > 100 && logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, getClass().getSimpleName() + "(topics=" + this.topics + ").consumer (mq.count = " + count + ", mq.cost-slowly = " + e + " ms)， msgs=" + map);
                } else if (e > 10 && logger.isLoggable(Level.FINEST)) {
                    logger.log(Level.FINEST, getClass().getSimpleName() + "(topics=" + this.topics + ").consumer (mq.count = " + count + ", mq.cost-normal = " + e + " ms)");
                }
                map.clear();
            }
            if (this.consumer != null) {
                this.consumer.close();
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(topics=" + this.topics + ") stoped");
            }
        } catch (Throwable t) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(topics=" + this.topics + ") stoped");
            }
            logger.log(Level.SEVERE, getClass().getSimpleName() + "(topics=" + this.topics + ") occur error", t);
        } finally {
            if (this.closeFuture != null) {
                this.closeFuture.complete(null);
            }
        }
    }

    public void start() {
        startCloseLock.lock();
        try {
            if (this.startFuture != null) {
                this.startFuture.join();
                return;
            }
            this.thread = new Thread(this);
            this.startFuture = new CompletableFuture<>();
            this.thread.setName(MessageConsumer.class.getSimpleName() + "-[" + group + "]-Thread");
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(topics=" + this.topics + ") starting");
            }
            this.thread.start();
            this.startFuture.join();
        } finally {
            startCloseLock.unlock();
        }
    }

    public void stop() {
        startCloseLock.lock();
        try {
            if (this.closeFuture != null) {
                this.closeFuture.join();
                return;
            }
            if (this.consumer == null) {
                return;
            }
            if (this.closed) {
                return;
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(topics=" + this.topics + ") stoping");
            }
            this.closeFuture = new CompletableFuture<>();
            this.closed = true;
            this.thread.interrupt();
            this.closeFuture.join();
        } finally {
            startCloseLock.unlock();
        }
    }

}
