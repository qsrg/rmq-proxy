package com.mq.proxy.sdk.consumer.lite;

import com.mq.proxy.sdk.config.ProxyCommonConfig;

public class ProxyLitePullConsumerConfig extends ProxyCommonConfig {

    private String consumerGroup = "SDKLitePullConsumerGroup";

    private long suspendTimeoutMillis = 15000L;

    private String messageModel = "CLUSTERING";

    private int pullBatchSize = 32;

    private long pullIntervalMillis = 0L;

    private long rebalanceIntervalMillis = 20000L;

    private boolean autoCommit = true;

    private long autoCommitIntervalMillis = 5000L;

    private int pullThresholdForQueue = 1000;

    private int pullThresholdSizeForQueue = 100;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public ProxyLitePullConsumerConfig setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }

    public ProxyLitePullConsumerConfig setSuspendTimeoutMillis(long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
        return this;
    }

    public String getMessageModel() {
        return messageModel;
    }

    public ProxyLitePullConsumerConfig setMessageModel(String messageModel) {
        this.messageModel = messageModel;
        return this;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public ProxyLitePullConsumerConfig setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
        return this;
    }

    public long getPullIntervalMillis() {
        return pullIntervalMillis;
    }

    public ProxyLitePullConsumerConfig setPullIntervalMillis(long pullIntervalMillis) {
        this.pullIntervalMillis = pullIntervalMillis;
        return this;
    }

    public long getRebalanceIntervalMillis() {
        return rebalanceIntervalMillis;
    }

    public ProxyLitePullConsumerConfig setRebalanceIntervalMillis(long rebalanceIntervalMillis) {
        this.rebalanceIntervalMillis = rebalanceIntervalMillis;
        return this;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public ProxyLitePullConsumerConfig setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    public long getAutoCommitIntervalMillis() {
        return autoCommitIntervalMillis;
    }

    public ProxyLitePullConsumerConfig setAutoCommitIntervalMillis(long autoCommitIntervalMillis) {
        this.autoCommitIntervalMillis = autoCommitIntervalMillis;
        return this;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public ProxyLitePullConsumerConfig setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
        return this;
    }

    public int getPullThresholdSizeForQueue() {
        return pullThresholdSizeForQueue;
    }

    public ProxyLitePullConsumerConfig setPullThresholdSizeForQueue(int pullThresholdSizeForQueue) {
        this.pullThresholdSizeForQueue = pullThresholdSizeForQueue;
        return this;
    }
}