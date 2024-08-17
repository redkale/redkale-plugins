/*
 *
 */
package org.redkalex.mq.kafka;

import java.time.Duration;
import java.util.ArrayList;
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
import org.redkale.mq.MessageConsumer;
import org.redkale.mq.MessageEvent;
import org.redkale.mq.spi.MessageAgent.MessageConsumerWrapper;

/** @author zhangjx */
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

    protected KafkaMessageConsumer(
            KafkaMessageAgent messageAgent, String group, Map<String, MessageConsumerWrapper> consumerMap) {
        this.messageAgent = messageAgent;
        this.group = group;
        this.consumerMap = consumerMap;
        this.topics = new ArrayList<>(consumerMap.keySet());
    }

    @Override
    public void run() {
        this.consumer = new KafkaConsumer<>(
                this.messageAgent.createConsumerProperties(this.group),
                new StringDeserializer(),
                new ByteArrayDeserializer());
        this.consumer.subscribe(this.topics);
        this.startFuture.complete(null);
        try {
            // key: topic
            Map<String, List<MessageEvent<byte[]>>> map = new LinkedHashMap<>();
            ConsumerRecords<String, byte[]> records;
            while (!this.closed) {
                try {
                    records = this.consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                } catch (Exception ex) {
                    if (!this.closed) {
                        logger.log(
                                Level.WARNING,
                                getClass().getSimpleName() + "(topics=" + this.topics + ") poll error",
                                ex);
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
                        logger.log(
                                Level.FINE,
                                getClass().getSimpleName() + "(topics=" + this.topics + ") processor async commit in "
                                        + ce + "ms");
                    }
                }
                map.clear();
                long s = System.currentTimeMillis();
                try {
                    for (ConsumerRecord<String, byte[]> r : records) {
                        map.computeIfAbsent(r.topic(), t -> new ArrayList<>())
                                .add(new MessageEvent<>(r.topic(), r.partition(), r.key(), r.value()));
                    }
                    map.forEach((topic, events) -> {
                        MessageConsumerWrapper wrapper = consumerMap.get(topic);
                        if (wrapper != null) {
                            wrapper.onMessage(events);
                        }
                    });
                } catch (Throwable e) {
                    logger.log(
                            Level.SEVERE,
                            getClass().getSimpleName() + "(topics=" + this.topics + ") process " + map + " error",
                            e);
                }
                long e = System.currentTimeMillis() - s;
                if (e > 1000 && logger.isLoggable(Level.FINE)) {
                    logger.log(
                            Level.FINE,
                            getClass().getSimpleName() + "(topics=" + this.topics + ").consumer (mq.count = " + count
                                    + ", mq.cost-slower = " + e + " ms)， msgs=" + map);
                } else if (e > 100 && logger.isLoggable(Level.FINER)) {
                    logger.log(
                            Level.FINER,
                            getClass().getSimpleName() + "(topics=" + this.topics + ").consumer (mq.count = " + count
                                    + ", mq.cost-slowly = " + e + " ms)， msgs=" + map);
                } else if (e > 10 && logger.isLoggable(Level.FINEST)) {
                    logger.log(
                            Level.FINEST,
                            getClass().getSimpleName() + "(topics=" + this.topics + ").consumer (mq.count = " + count
                                    + ", mq.cost-normal = " + e + " ms)");
                }
                map.clear();
            }
            if (this.consumer != null) {
                this.consumer.close();
            }
        } catch (Throwable t) {
            logger.log(
                    Level.SEVERE,
                    getClass().getSimpleName() + "(topics=" + this.topics + ", group=" + group + ") occur error",
                    t);
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
            logger.log(
                    Level.INFO,
                    getClass().getSimpleName() + "(topics=" + this.topics + ", group=" + group + ") starting");
            this.thread.start();
            this.startFuture.join();
            logger.log(
                    Level.INFO,
                    getClass().getSimpleName() + "(topics=" + this.topics + ", group=" + group + ") started");
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
            logger.log(
                    Level.INFO,
                    getClass().getSimpleName() + "(topics=" + this.topics + ", group=" + group + ") stoping");
            this.closeFuture = new CompletableFuture<>();
            this.closed = true;
            this.thread.interrupt();
            this.closeFuture.join();
            logger.log(
                    Level.INFO,
                    getClass().getSimpleName() + "(topics=" + this.topics + ", group=" + group + ") stoped");
        } finally {
            startCloseLock.unlock();
        }
    }
}
