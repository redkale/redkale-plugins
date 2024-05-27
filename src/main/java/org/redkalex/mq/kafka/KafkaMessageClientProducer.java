/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.*;
import org.redkale.mq.spi.MessageAgent;
import org.redkale.mq.spi.MessageClientProducer;
import org.redkale.mq.spi.MessageCoder;
import org.redkale.mq.spi.MessageRecord;
import org.redkale.util.Traces;

/** @author zhangjx */
public class KafkaMessageClientProducer extends MessageClientProducer {

	private final AtomicBoolean closed = new AtomicBoolean();

	private MessageAgent messageAgent;

	protected Properties config;

	private KafkaProducer<String, MessageRecord> producer;

	protected final ConcurrentHashMap<String, Integer[]> partionsMap = new ConcurrentHashMap<>();

	private int partitions;

	public KafkaMessageClientProducer(KafkaMessageAgent messageAgent, String producerName, int partitions) {
		super(producerName);
		this.partitions = partitions;
		Objects.requireNonNull(messageAgent);
		this.messageAgent = messageAgent;
		this.config = messageAgent.createProducerProperties();
		this.producer = new KafkaProducer<>(
				this.config, new StringSerializer(), new MessageRecordSerializer(messageAgent.getMessageRecordCoder()));
		if (logger.isLoggable(Level.INFO)) {
			logger.log(
					Level.INFO,
					getClass().getSimpleName() + "(mq=" + messageAgent.getName() + "， name=" + producerName
							+ ") started");
		}
	}

	@Override
	public CompletableFuture<Void> apply(MessageRecord message) {
		if (closed.get()) {
			throw new IllegalStateException(
					getClass().getSimpleName() + "(name=" + name + ") is closed when send " + message);
		}
		if (this.producer == null) {
			throw new IllegalStateException(
					getClass().getSimpleName() + "(name=" + name + ") not started when send " + message);
		}
		final CompletableFuture future = new CompletableFuture();
		Integer partition = null;
		if (this.partitions > 0) { // 不指定 partition则设计上需要以对等为主
			if (message.getGroupid() != null && !message.getGroupid().isEmpty()) {
				partition = Math.abs(message.getGroupid().hashCode()) % this.partitions;
			} else if (message.getUserid() != null) {
				partition = Math.abs(message.getUserid().hashCode()) % this.partitions;
			}
		}
		final Integer partition0 = partition;
		String traceid = message.getTraceid();
		// if (finest) logger.log(Level.FINEST, "Kafka.producer prepare send partition=" + partition0 + ", msg=" +
		// message);
		producer.send(new ProducerRecord<>(message.getTopic(), partition, null, message), (metadata, exp) -> {
			Traces.computeIfAbsent(traceid);
			if (exp != null) {
				messageAgent.execute(() -> {
					Traces.computeIfAbsent(traceid);
					future.completeExceptionally(exp);
					Traces.removeTraceid();
				});
			} else {
				messageAgent.execute(() -> {
					Traces.computeIfAbsent(traceid);
					future.complete(null);
					Traces.removeTraceid();
				});
			}

			long e = System.currentTimeMillis() - message.getCreateTime();
			if (e > 1000 && logger.isLoggable(Level.FINE)) {
				logger.log(
						Level.FINE,
						getClass().getSimpleName() + "(name=" + name + ") (mq.cost-slower = " + e + " ms)，partition="
								+ partition0 + ", msg=" + message);
			} else if (e > 100 && logger.isLoggable(Level.FINER)) {
				logger.log(
						Level.FINER,
						getClass().getSimpleName() + "(name=" + name + ") (mq.cost-slowly = " + e + " ms)，partition="
								+ partition0 + ", msg=" + message);
			} else if (e > 10 && logger.isLoggable(Level.FINEST)) {
				logger.log(
						Level.FINEST,
						getClass().getSimpleName() + "(name=" + name + ") (mq.cost-normal = " + e + " ms)，partition="
								+ partition0 + ", msg=" + message);
			}
			Traces.removeTraceid();
		});
		return future;
	}

	protected Integer[] loadTopicPartition(String topic0) {
		return partionsMap.computeIfAbsent(topic0, topic -> {
			try {
				AdminClient adminClient = ((KafkaMessageAgent) messageAgent).adminClient;
				DescribeTopicsResult rs = adminClient.describeTopics(Arrays.asList(topic));
				List<TopicPartitionInfo> list =
						rs.topicNameValues().get(topic).get(6, TimeUnit.SECONDS).partitions();
				Integer[] parts = new Integer[list.size()];
				for (int i = 0; i < parts.length; i++) {
					parts[i] = list.get(i).partition();
				}
				Arrays.sort(parts);
				if (logger.isLoggable(Level.FINER)) {
					logger.log(
							Level.FINER,
							getClass().getSimpleName() + "(name=" + name + ") Topic(" + topic + ") load partitions = "
									+ list);
				}
				return parts;
			} catch (Exception ex) {
				logger.log(
						Level.SEVERE,
						getClass().getSimpleName() + "(name=" + name + ") Topic(" + topic + ")  load partitions error",
						ex);
				return new Integer[0];
			}
		});
	}

	@Override
	public void stop() {
		if (this.closed.compareAndSet(false, true)) {
			if (logger.isLoggable(Level.FINE)) {
				logger.log(Level.FINE, getClass().getSimpleName() + "(name=" + name + ") closing");
			}
			if (this.producer != null) {
				this.producer.close();
			}
			if (logger.isLoggable(Level.FINE)) {
				logger.log(Level.FINE, getClass().getSimpleName() + "(name=" + name + ") closed");
			}
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
