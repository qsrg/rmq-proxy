package com.mq.proxy.sdk.consumer.push;

import com.mq.proxy.sdk.consumer.model.ProxyMessage;

import java.util.List;

public interface ProxyMessageListener {

    ConsumeStatus consume(List<ProxyMessage> messages);
}