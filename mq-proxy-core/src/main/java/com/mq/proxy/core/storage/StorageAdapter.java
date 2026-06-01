package com.mq.proxy.core.storage;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;

public interface StorageAdapter {
    void initialize(StorageConfig config) throws Exception;

    void shutdown();

    PutResult putMessage(InternalMessage message, String brokerAddr) throws Exception;

    PullResult pullMessage(String consumerGroup, String topic, int queueId, long queueOffset, int maxMsgNums, int sysFlag, long commitOffset, long suspendTimeoutMillis, String subscription, String expressionType, long subVersion, String brokerAddr) throws Exception;

    OffsetResult queryConsumerOffset(String consumerGroup, String topic, int queueId, String brokerAddr) throws Exception;

    void updateConsumerOffset(String consumerGroup, String topic, int queueId, long commitOffset, String brokerAddr) throws Exception;

    boolean healthCheck();

    RemotingCommand forwardToBroker(RemotingCommand request, String brokerAddr) throws Exception;
}
