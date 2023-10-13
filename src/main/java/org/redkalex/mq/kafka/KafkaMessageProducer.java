/*
 *
 */
package org.redkalex.mq.kafka;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.redkale.convert.Convert;
import org.redkale.mq.MessageProducer;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageProducer implements MessageProducer {

    protected final Logger logger = Logger.getLogger(this.getClass().getSimpleName());

    private final AtomicBoolean closed = new AtomicBoolean();

    private KafkaMessageAgent messageAgent;

    protected Properties config;

    private KafkaProducer<String, byte[]> producer;

    public KafkaMessageProducer(KafkaMessageAgent messageAgent) {
        Objects.requireNonNull(messageAgent);
        this.messageAgent = messageAgent;
        this.producer = new KafkaProducer<>(messageAgent.createProducerProperties(), new StringSerializer(), new ByteArraySerializer());
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, getClass().getSimpleName() + "(name=" + messageAgent.getName() + ") started");
        }
    }

    @Override
    public CompletableFuture<Void> sendMessage(String topic, Integer partition, Convert convert, Type type, Object value) {
        if (closed.get()) {
            throw new IllegalStateException(getClass().getSimpleName() + "(name=" + messageAgent.getName() + ") is closed when send " + value);
        }
        if (this.producer == null) {
            throw new IllegalStateException(getClass().getSimpleName() + "(name=" + messageAgent.getName() + ") not started when send " + value);
        }
        final CompletableFuture future = new CompletableFuture();
        long s = System.currentTimeMillis();
        //if (finest) logger.log(Level.FINEST, "Kafka.producer prepare send partition=" + partition + ", msg=" + message);
        producer.send(new ProducerRecord<>(topic, partition, null, convertMessage(convert, type, value)), (metadata, exp) -> {
            if (exp != null) {
                future.completeExceptionally(exp);
            } else {
                future.complete(null);
            }

            long e = System.currentTimeMillis() - s;
            if (e > 1000 && logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(name=" + messageAgent.getName() + ") (mqs.costs = " + e + " ms)，partition=" + partition + ", msg=" + value);
            } else if (e > 100 && logger.isLoggable(Level.FINER)) {
                logger.log(Level.FINER, getClass().getSimpleName() + "(name=" + messageAgent.getName() + ") (mq.costs = " + e + " ms)，partition=" + partition + ", msg=" + value);
            } else if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, getClass().getSimpleName() + "(name=" + messageAgent.getName() + ") (mq.cost = " + e + " ms)，partition=" + partition + ", msg=" + value);
            }
        });
        return future;
    }

    protected byte[] convertMessage(Convert convert, Type type, Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        } else if (value instanceof CharSequence) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        } else if (type == null) {
            return convert.convertToBytes(value);
        } else {
            return convert.convertToBytes(type, value);
        }
    }

    public void stop() {
        if (this.closed.compareAndSet(false, true)) {
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(name=" + messageAgent.getName() + ") closing");
            }
            if (this.producer != null) {
                this.producer.close();
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.log(Level.FINE, getClass().getSimpleName() + "(name=" + messageAgent.getName() + ") closed");
            }
        }
    }

}
