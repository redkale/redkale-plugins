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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.redkale.annotation.ResourceListener;
import org.redkale.mq.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageAgent extends MessageAgent {

    protected String servers;

    protected String controllers;

    protected int checkIntervals = 10;

    protected Properties consumerConfig = new Properties();

    protected Properties producerConfig = new Properties();

    protected AdminClient adminClient;

    protected int partitions;

    protected ScheduledFuture reconnectFuture;

    protected final AtomicBoolean reconnecting = new AtomicBoolean();

    private final List<KafkaMessageConsumer> kafkaConsumers = new ArrayList<>();

    @Override
    public void init(AnyValue config) {
        super.init(config);
        this.servers = config.getAnyValue("servers").getValue("value");
        AnyValue cs = config.getAnyValue("controllers");
        this.controllers = cs == null ? this.servers : cs.getValue("value", this.servers);
        this.checkIntervals = config.getAnyValue("servers").getIntValue("checkIntervals", 10);

        AnyValue[] propConfigValues = config.getAnyValues("config");
        if (propConfigValues != null) {
            for (AnyValue confValues : propConfigValues) {
                if ("consumer".equals(confValues.getValue("type"))) {
                    for (AnyValue val : confValues.getAnyValues("property")) {
                        this.consumerConfig.put(val.getValue("name"), val.getValue("value"));
                    }
                } else if ("producer".equals(confValues.getValue("type"))) {
                    this.partitions = confValues.getIntValue("partitions", 0);
                    for (AnyValue val : confValues.getAnyValues("property")) {
                        this.producerConfig.put(val.getValue("name"), val.getValue("value"));
                    }
                }
            }
        }
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, controllers);
        this.adminClient = KafkaAdminClient.create(props);
    }

    @Override
    @ResourceListener
    public void onResourceChange(ResourceEvent[] events) {
        StringBuilder sb = new StringBuilder();
        for (ResourceEvent event : events) {
            sb.append(KafkaMessageAgent.class.getSimpleName()).append(" skip change '").append(event.name()).append("' to '").append(event.coverNewValue()).append("'\r\n");
        }
        if (sb.length() > 0) {
            logger.log(Level.INFO, sb.toString());
        }
    }

    @Override
    public void destroy(AnyValue config) {
        super.destroy(config);
        if (this.adminClient != null) {
            this.adminClient.close();
        }
    }

    public void startReconnect() {
        if (this.reconnecting.compareAndSet(false, true)) {
            this.reconnectFuture = this.timeoutExecutor.scheduleAtFixedRate(() -> retryConnect(), 0, this.checkIntervals, TimeUnit.SECONDS);
        }
    }

    protected Properties createConsumerProperties(String consumerid) {
        final Properties props = new Properties();
        if (consumerid != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerid);
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");// 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.putAll(consumerConfig);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        return props;
    }

    protected Properties createProducerProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.ACKS_CONFIG, "0");//all:所有follower都响应了才认为消息提交成功，即"committed"
        props.putAll(producerConfig);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        return props;
    }

    private void retryConnect() {
        if (this.adminClient != null) {
            this.adminClient.close();
        }
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, controllers);
        this.adminClient = KafkaAdminClient.create(props);
        if (queryTopic() != null) {
            logger.log(Level.INFO, getClass().getSimpleName() + " resume connect");
            this.reconnecting.set(false);
            if (this.reconnectFuture != null) {
                this.reconnectFuture.cancel(true);
                this.reconnectFuture = null;
            }
            //this.getMessageClientConsumers().forEach(c -> ((KafkaMessageClientConsumer) c).retryConnect());
            //this.getMessageClientProducers().forEach(c -> ((KafkaMessageClientProducer) c).retryConnect());
        }
    }

    public int getCheckIntervals() {
        return checkIntervals;
    }

    public ScheduledThreadPoolExecutor getTimeoutExecutor() {
        return timeoutExecutor;
    }

    @Override //ServiceLoader时判断配置是否符合当前实现类
    public boolean acceptsConf(AnyValue config) {
        if (config == null) {
            return false;
        }
        if ("kafka".equalsIgnoreCase(config.getValue("type"))) {
            return true;
        }
        AnyValue ser = config.getAnyValue("servers");
        if (ser == null) {
            return false;
        }
        if (ser.getValue("value") != null && !ser.getValue("value").contains("pulsar")) {
            return true;
        }
        return false;
    }

    @Override
    public boolean createTopic(String... topics) {
        if (topics == null || topics.length < 1) {
            return true;
        }
        try {
            List<NewTopic> newTopics = new ArrayList<>(topics.length);
            for (String t : topics) {
                newTopics.add(new NewTopic(t, Optional.empty(), Optional.empty()));
            }
            adminClient.createTopics(newTopics, new CreateTopicsOptions().timeoutMs(3000)).all().get(3, TimeUnit.SECONDS);
            return true;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "createTopic error: " + Arrays.toString(topics), ex);
            return false;
        }
    }

    @Override
    public boolean deleteTopic(String... topics) {
        if (topics == null || topics.length < 1) {
            return true;
        }
        try {
            adminClient.deleteTopics(Utility.ofList(topics), new DeleteTopicsOptions().timeoutMs(3000)).all().get(3, TimeUnit.SECONDS);
            return true;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "deleteTopic error: " + Arrays.toString(topics), ex);
            return false;
        }
    }

    @Override
    public List<String> queryTopic() {
        try {
            Collection<TopicListing> list = adminClient.listTopics(new ListTopicsOptions().timeoutMs(3000)).listings().get(3, TimeUnit.SECONDS);
            List<String> result = new ArrayList<>(list.size());
            for (TopicListing t : list) {
                if (!t.isInternal()) {
                    result.add(t.name());
                }
            }
            return result;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "queryTopic error ", ex);
        }
        return null;
    }

    @Override //创建指定topic的消费处理器
    public MessageClientConsumer createMessageClientConsumer(String[] topics, String consumerid, MessageClientProcessor processor) {
        return new KafkaMessageClientConsumer(this, topics, consumerid, processor);
    }

    @Override //创建指定topic的生产处理器
    protected MessageClientProducer createMessageClientProducer(String producerName) {
        return new KafkaMessageClientProducer(this, producerName, this.partitions);
    }

    @Override
    protected void startMessageProducer() {
        this.messageBaseProducer = new KafkaMessageProducer(this, servers, this.producerConfig);
    }

    @Override
    protected void stopMessageProducer() {
        ((KafkaMessageProducer) this.messageBaseProducer).stop();
    }

    @Override
    protected void startMessageConsumer() {
        List<KafkaMessageConsumer> list = new ArrayList<>();
        this.messageConsumerMap.forEach((group, map) -> {
            list.add(new KafkaMessageConsumer(this, group, map));
        });
        for (KafkaMessageConsumer item : list) {
            item.start();
        }
        this.kafkaConsumers.addAll(list);
    }

    @Override
    protected void stopMessageConsumer() {
        for (KafkaMessageConsumer item : kafkaConsumers) {
            item.stop();
        }
    }
}
