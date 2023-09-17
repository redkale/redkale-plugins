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

    @Override
    public void init(AnyValue config) {
        super.init(config);
        this.servers = config.getAnyValue("servers").getValue("value");
        AnyValue cs = config.getAnyValue("controllers");
        this.controllers = cs == null ? this.servers : cs.getValue("value", this.servers);
        this.checkIntervals = config.getAnyValue("servers").getIntValue("checkIntervals", 10);

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
            this.getMessageClientConsumers().forEach(c -> ((KafkaMessageClientConsumer) c).retryConnect());
            this.getMessageClientProducers().forEach(c -> ((KafkaMessageClientProducer) c).retryConnect());
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
        return new KafkaMessageClientConsumer(this, topics, consumerid, processor, servers, this.consumerConfig);
    }

    @Override //创建指定topic的生产处理器
    protected MessageClientProducer createMessageClientProducer(String producerName) {
        return new KafkaMessageClientProducer(producerName, this, servers, this.partitions, this.producerConfig);
    }

    @Override
    protected MessageProducer createMessageProducer() {
        return new KafkaMessageProducer(this, servers, this.producerConfig);
    }

    @Override
    protected void closeMessageProducer(MessageProducer messageProducer) throws Exception {
        ((KafkaMessageProducer) messageProducer).close();
    }
}
