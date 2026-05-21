package com.mq.proxy.sdk.client;

public interface ConsumerChangeListener {
    void onConsumerIdsChanged(String consumerGroup);
}