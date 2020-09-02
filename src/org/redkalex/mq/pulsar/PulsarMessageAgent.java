/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.pulsar;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.schema.*;
import org.redkale.mq.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class PulsarMessageAgent extends MessageAgent {

    protected String servers;

    protected int checkIntervals = 10;

    protected Properties consumerConfig = new Properties();

    protected Properties producerConfig = new Properties();

    protected final ConcurrentHashMap<String, Producer<MessageRecord>> producers = new ConcurrentHashMap<>();

    protected PulsarClient client;

    protected PulsarAdmin adminClient;

    protected int partitions;

    protected ScheduledFuture reconnectFuture;

    protected boolean reconnecting;

    @Override
    public void init(AnyValue config) {
        super.init(config);
        this.servers = config.getAnyValue("servers").getValue("value");
        this.checkIntervals = config.getAnyValue("servers").getIntValue("checkintervals", 10);

        AnyValue consumerAnyValue = config.getAnyValue("consumer");
        if (consumerAnyValue != null) {
            for (AnyValue val : consumerAnyValue.getAnyValues("property")) {
                this.consumerConfig.put(val.getValue("name"), val.getValue("value"));
            }
        }

        AnyValue producerAnyValue = config.getAnyValue("producer");
        if (producerAnyValue != null) {
            this.partitions = producerAnyValue.getIntValue("partitions", 0);
            for (AnyValue val : producerAnyValue.getAnyValues("property")) {
                this.producerConfig.put(val.getValue("name"), val.getValue("value"));
            }
        }
        try {
            this.adminClient = org.apache.pulsar.client.admin.PulsarAdmin.builder().serviceHttpUrl(servers).build();
            this.client = org.apache.pulsar.client.api.PulsarClient.builder().serviceUrl(servers).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy(AnyValue config) {
        super.destroy(config);
        if (this.adminClient != null) {
            try {
                this.adminClient.close();
            } catch (Exception e) {
                logger.log(Level.WARNING, this.adminClient + " close error", e);
            }
        }
    }

    public synchronized void startReconnect() {
        if (this.reconnecting) return;
        this.reconnectFuture = this.timeoutExecutor.scheduleAtFixedRate(() -> retryConnect(), 0, this.checkIntervals, TimeUnit.SECONDS);
    }

    private void retryConnect() {
        if (this.adminClient != null) {
            try {
                this.adminClient.close();
            } catch (Exception e) {
                logger.log(Level.WARNING, this.adminClient + " close error", e);
            }
        }
        if (this.client != null) {
            try {
                this.client.close();
            } catch (Exception e) {
                logger.log(Level.WARNING, this.client + " close error", e);
            }
        }
        Collection< Producer<MessageRecord>> ps = producers.values();
        producers.clear();
        if (ps != null && !ps.isEmpty()) {
            try {
                for (Producer<MessageRecord> p : ps) {
                    p.close();
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "Producer close error", e);
            }
        }
        try {
            this.adminClient = org.apache.pulsar.client.admin.PulsarAdmin.builder().serviceHttpUrl(servers).build();
            this.client = org.apache.pulsar.client.api.PulsarClient.builder().serviceUrl(servers).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (queryTopic() != null) {
            logger.log(Level.INFO, getClass().getSimpleName() + " resume connect");
            this.reconnecting = false;
            if (this.reconnectFuture != null) {
                this.reconnectFuture.cancel(true);
                this.reconnectFuture = null;
            }
            this.getAllMessageConsumer().forEach(c -> ((PulsarMessageConsumer) c).retryConnect());
            this.getAllMessageProducer().forEach(c -> ((PulsarMessageProducer) c).retryConnect());
        }
    }

    public int getCheckIntervals() {
        return checkIntervals;
    }

    public ScheduledThreadPoolExecutor getTimeoutExecutor() {
        return timeoutExecutor;
    }

    @Override //ServiceLoader时判断配置是否符合当前实现类
    public boolean match(AnyValue config) {
        if (config == null) return false;
        AnyValue ser = config.getAnyValue("servers");
        if (ser == null) return false;
        if (ser.getValue("value") != null && ser.getValue("value").contains("pulsar")) return true;
        return false;
    }

    @Override
    public boolean createTopic(String... topics) {
        if (topics == null || topics.length < 1) return true;
        try {
            for (String topic : topics) {
                adminClient.topics().createNonPartitionedTopic(topic);
            }
            return true;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "createTopic error: " + Arrays.toString(topics), ex);
            return false;
        }
    }

    @Override
    public boolean deleteTopic(String... topics) {
        if (topics == null || topics.length < 1) return true;
        try {
            for (String topic : topics) {
                adminClient.topics().delete(topic);
            }
            return true;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "deleteTopic error: " + Arrays.toString(topics), ex);
            return false;
        }
    }

    @Override
    public List<String> queryTopic() {
        try {
            return adminClient.topics().getList(null);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "queryTopic error ", ex);
        }
        return null;
    }

    @Override //创建指定topic的消费处理器
    public MessageConsumer createConsumer(String[] topics, String consumerid, MessageProcessor processor) {
        return new PulsarMessageConsumer(this, topics, consumerid, processor, servers, this.consumerConfig);
    }

    @Override //创建指定topic的生产处理器
    protected MessageProducer createProducer(String name) {
        return new PulsarMessageProducer(name, this, servers, this.partitions, this.producerConfig);
    }

    public static class MessageRecordSchema implements Schema<MessageRecord> {

        public static final MessageRecordSchema INSTANCE = new MessageRecordSchema();

        private static final SchemaInfo SCHEMA_INFO = new SchemaInfo()
            .setName("MessageRecord")
            .setType(SchemaType.BYTES)
            .setSchema(new byte[0]);

        @Override
        public MessageRecord decode(byte[] data) {
            return MessageRecordCoder.getInstance().decode(data);
        }

        @Override
        public byte[] encode(MessageRecord data) {
            return MessageRecordCoder.getInstance().encode(data);
        }

        @Override
        public SchemaInfo getSchemaInfo() {
            return SCHEMA_INFO;
        }

        @Override
        public Schema<MessageRecord> clone() {
            return this;
        }

    }
}
