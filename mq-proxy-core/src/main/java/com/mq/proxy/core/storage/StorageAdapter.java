package com.mq.proxy.core.storage;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;

public interface StorageAdapter {
    void initialize(StorageConfig config) throws Exception;

    void shutdown();

    PutResult putMessage(InternalMessage message) throws Exception;

    PullResult pullMessage(String consumerGroup, String topic, int queueId, long queueOffset, int maxMsgNums, long suspendTimeoutMillis, String subscription, String expressionType) throws Exception;

    OffsetResult queryConsumerOffset(String consumerGroup, String topic, int queueId) throws Exception;

    void updateConsumerOffset(String consumerGroup, String topic, int queueId, long commitOffset) throws Exception;

    boolean healthCheck();

    String getAdapterName();

    RemotingCommand forwardToBroker(RemotingCommand request) throws Exception;
}