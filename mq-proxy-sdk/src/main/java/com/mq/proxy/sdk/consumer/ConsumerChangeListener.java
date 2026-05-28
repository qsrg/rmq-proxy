package com.mq.proxy.sdk.consumer;

public interface ConsumerChangeListener {
    void onConsumerIdsChanged(String consumerGroup);
}