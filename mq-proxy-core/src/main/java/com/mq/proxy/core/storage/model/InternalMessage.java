package com.mq.proxy.core.storage.model;

import com.mq.proxy.core.protocol.header.SendMessageRequestHeader;

public class InternalMessage {
    private String topic;
    private Integer queueId;
    private String msgId;
    private byte[] body;
    private String properties;
    private String producerGroup;
    private int flag;
    private int sysFlag;
    private long bornTimestamp;
    private int reconsumeTimes;
    private String transactionId;
    private String keys;
    private String tags;
    private String defaultTopic;
    private int defaultTopicQueueNums;
    private boolean unitMode;
    private int maxReconsumeTimes;
    private boolean batch;
    private int topicSysFlag;
    private String bname;

    public static InternalMessage createFromSendMessageRequest(SendMessageRequestHeader header, byte[] body) {
        InternalMessage message = new InternalMessage();
        message.setTopic(header.getTopic());
        message.setQueueId(header.getQueueId());
        message.setBody(body);
        message.setProperties(header.getProperties());
        message.setProducerGroup(header.getProducerGroup());
        message.setFlag(header.getFlag() != null ? header.getFlag() : 0);
        message.setSysFlag(header.getSysFlag() != null ? header.getSysFlag() : 0);
        message.setBornTimestamp(header.getBornTimestamp() != null ? header.getBornTimestamp() : 0L);
        message.setReconsumeTimes(header.getReconsumeTimes() != null ? header.getReconsumeTimes() : 0);
        message.setDefaultTopic(header.getDefaultTopic());
        message.setDefaultTopicQueueNums(header.getDefaultTopicQueueNums() != null ? header.getDefaultTopicQueueNums() : 4);
        message.setUnitMode(header.isUnitMode());
        message.setMaxReconsumeTimes(header.getMaxReconsumeTimes() != null ? header.getMaxReconsumeTimes() : 16);
        message.setBatch(header.isBatch());
        message.setTopicSysFlag(header.getTopicSysFlag() != null ? header.getTopicSysFlag() : 0);
        message.setBname(header.getBname());
        return message;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public String getProperties() {
        return properties;
    }

    public void setProperties(String properties) {
        this.properties = properties;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public int getMaxReconsumeTimes() {
        return maxReconsumeTimes;
    }

    public void setMaxReconsumeTimes(int maxReconsumeTimes) {
        this.maxReconsumeTimes = maxReconsumeTimes;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public int getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(int topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public String getBname() {
        return bname;
    }

    public void setBname(String bname) {
        this.bname = bname;
    }
}