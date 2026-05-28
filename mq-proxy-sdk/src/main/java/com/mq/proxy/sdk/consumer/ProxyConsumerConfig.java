package com.mq.proxy.sdk.consumer;

import com.mq.proxy.sdk.config.ProxyCommonConfig;

/**
 * ProxyConsumer配置 - 支持链式配置
 */
public class ProxyConsumerConfig extends ProxyCommonConfig {

    private String consumerGroup = "SDKConsumerGroup";

    private long suspendTimeoutMillis = 0L;

    private String messageModel = "CLUSTERING";

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public ProxyConsumerConfig setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public long getSuspendTimeoutMillis() {
        return suspendTimeoutMillis;
    }

    public ProxyConsumerConfig setSuspendTimeoutMillis(long suspendTimeoutMillis) {
        this.suspendTimeoutMillis = suspendTimeoutMillis;
        return this;
    }

    public String getMessageModel() {
        return messageModel;
    }

    public ProxyConsumerConfig setMessageModel(String messageModel) {
        this.messageModel = messageModel;
        return this;
    }
}