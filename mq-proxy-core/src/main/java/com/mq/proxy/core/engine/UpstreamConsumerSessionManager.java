package com.mq.proxy.core.engine;

import java.util.List;

public interface UpstreamConsumerSessionManager {

    void syncHeartbeat(ClientConnectionManager.ClientInfo clientInfo);

    void unregisterClient(ClientConnectionManager.ClientInfo clientInfo);

    List<String> getConsumerListByGroup(String consumerGroup) throws Exception;
}
