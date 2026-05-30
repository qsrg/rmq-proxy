package com.mq.proxy.sdk.consumer.push;

import com.mq.proxy.sdk.config.ProxyCommonConfig;

public class ProxyPushConsumerConfig extends ProxyCommonConfig {

    private String consumerGroup = "SDKPushConsumerGroup";

    private long suspendTimeoutMillis = 15000L;

    private String messageModel = "CLUSTERING";

    private int pullBatchSize = 32;

    private long pullIntervalMillis = 0L;

    private int consumeThreadNums = Runtime.getRuntime().availableProcessors();

    private long rebalanceIntervalMillis = 20000L;

    private long autoCommitIntervalMillis = 5000L;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public ProxyPushConsumerConfig setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }

    public ProxyPushConsumerConfig setSuspendTimeoutMillis(long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
        return this;
    }

    public String getMessageModel() {
        return messageModel;
    }

    public ProxyPushConsumerConfig setMessageModel(String messageModel) {
        this.messageModel = messageModel;
        return this;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public ProxyPushConsumerConfig setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
        return this;
    }

    public long getPullIntervalMillis() {
        return pullIntervalMillis;
    }

    public ProxyPushConsumerConfig setPullIntervalMillis(long pullIntervalMillis) {
        this.pullIntervalMillis = pullIntervalMillis;
        return this;
    }

    public int getConsumeThreadNums() {
        return consumeThreadNums;
    }

    public ProxyPushConsumerConfig setConsumeThreadNums(int consumeThreadNums) {
        this.consumeThreadNums = consumeThreadNums;
        return this;
    }

    public long getRebalanceIntervalMillis() {
        return rebalanceIntervalMillis;
    }

    public ProxyPushConsumerConfig setRebalanceIntervalMillis(long rebalanceIntervalMillis) {
        this.rebalanceIntervalMillis = rebalanceIntervalMillis;
        return this;
    }

    public long getAutoCommitIntervalMillis() {
        return autoCommitIntervalMillis;
    }

    public ProxyPushConsumerConfig setAutoCommitIntervalMillis(long autoCommitIntervalMillis) {
        this.autoCommitIntervalMillis = autoCommitIntervalMillis;
        return this;
    }
}