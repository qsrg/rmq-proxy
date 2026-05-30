package com.mq.proxy.sdk.consumer.model;

public class ProxyMessage {

    private String topic;
    private int queueId;
    private long queueOffset;
    private byte[] body;

    public ProxyMessage() {
    }

    public ProxyMessage(String topic, int queueId, long queueOffset, byte[] body) {
        this.topic = topic;
        this.queueId = queueId;
        this.queueOffset = queueOffset;
        this.body = body;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @Override
    public String toString() {
        return "ProxyMessage{topic='" + topic + "', queueId=" + queueId + ", queueOffset=" + queueOffset + '}';
    }
}