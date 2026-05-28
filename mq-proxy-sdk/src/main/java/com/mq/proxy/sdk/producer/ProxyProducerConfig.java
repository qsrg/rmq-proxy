package com.mq.proxy.sdk.producer;

import com.mq.proxy.sdk.config.ProxyCommonConfig;

/**
 * ProxyProducer配置 - 支持链式配置
 */
public class ProxyProducerConfig extends ProxyCommonConfig {

    private String producerGroup = "SDKProducerGroup";

    public String getProducerGroup() {
        return producerGroup;
    }

    public ProxyProducerConfig setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
        return this;
    }
}