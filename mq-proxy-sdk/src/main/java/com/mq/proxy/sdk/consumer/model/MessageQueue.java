package com.mq.proxy.sdk.consumer.model;

import java.util.Objects;

public class MessageQueue {

    private String topic;
    private String brokerName;
    private int queueId;

    public MessageQueue() {
    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageQueue that = (MessageQueue) o;
        return queueId == that.queueId
            && Objects.equals(topic, that.topic)
            && Objects.equals(brokerName, that.brokerName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, brokerName, queueId);
    }

    @Override
    public String toString() {
        return "MessageQueue{topic='" + topic + "', brokerName='" + brokerName + "', queueId=" + queueId + '}';
    }
}