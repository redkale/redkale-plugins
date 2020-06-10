/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.redkalex.mq.kafka;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.kafka.clients.admin.*;
import org.redkale.mq.*;
import org.redkale.util.*;

/**
 *
 * @author zhangjx
 */
public class KafkaMessageAgent extends MessageAgent {

    protected String servers;

    protected Properties consumerConfig = new Properties();

    protected Properties producerConfig = new Properties();

    protected KafkaAdminClient adminClient;

    @Override
    public void init(AnyValue config) {
        super.init(config);
        this.servers = config.getAnyValue("servers").getValue("value");

        AnyValue consumerAnyValue = config.getAnyValue("consumer");
        if (consumerAnyValue != null) {
            for (AnyValue val : consumerAnyValue.getAnyValues("property")) {
                this.consumerConfig.put(val.getValue("name"), val.getValue("value"));
            }
        }

        AnyValue producerAnyValue = config.getAnyValue("producer");
        if (producerAnyValue != null) {
            for (AnyValue val : producerAnyValue.getAnyValues("property")) {
                this.producerConfig.put(val.getValue("name"), val.getValue("value"));
            }
        }
    }

    @Override
    public void destroy(AnyValue config) {
        super.destroy(config);
    }

    @Override //ServiceLoader时判断配置是否符合当前实现类
    public boolean match(AnyValue config) {
        if (config == null) return false;
        AnyValue ser = config.getAnyValue("servers");
        if (ser == null) return false;
        if (ser.getValue("value") != null) return true;
        return false;
    }

    @Override
    public boolean createTopic(String... topics) {
        if (topics == null || topics.length < 1) return true;
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
        if (topics == null || topics.length < 1) return true;
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
                if (!t.isInternal()) result.add(t.name());
            }
            return result;
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "queryTopic error ", ex);
        }
        return null;
    }

    @Override //创建指定topic的消费处理器
    public MessageConsumer createConsumer(String topic, String consumerid, MessageProcessor processor) {
        return new KafkaMessageConsumer(this, topic, consumerid, processor, servers, this.consumerConfig);
    }

    @Override //创建指定topic的生产处理器
    protected MessageProducer createProducer() {
        return new KafkaMessageProducer(this, servers, this.producerConfig);
    }

}
