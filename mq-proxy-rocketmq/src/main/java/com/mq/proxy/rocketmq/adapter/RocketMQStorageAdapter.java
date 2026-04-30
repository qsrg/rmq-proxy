package com.mq.proxy.rocketmq.adapter;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.PullMessageRequestHeader;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetRequestHeader;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeader;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeaderV2;
import com.mq.proxy.core.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;

import java.util.List;

public class RocketMQStorageAdapter implements StorageAdapter {

    private NettyRemotingClient remotingClient;
    private NettyClientConfig clientConfig;
    private StorageConfig storageConfig;
    private volatile boolean initialized = false;

    @Override
    public void initialize(StorageConfig config) throws Exception {
        this.storageConfig = config;
        this.clientConfig = new NettyClientConfig();
        this.clientConfig.setNamesrvAddr(config.getNamesrvAddr());
        this.clientConfig.setConnectTimeoutMillis(config.getConnectTimeoutMillis());
        this.remotingClient = new NettyRemotingClient(this.clientConfig);
        this.remotingClient.start();
        this.initialized = true;
    }

    @Override
    public void shutdown() {
        if (this.remotingClient != null) {
            this.remotingClient.shutdown();
        }
        this.initialized = false;
    }

    @Override
    public String getAdapterName() {
        return "rocketmq";
    }

    @Override
    public PutResult putMessage(InternalMessage message) throws Exception {
        SendMessageRequestHeader header = new SendMessageRequestHeader();
        header.setProducerGroup(message.getProducerGroup());
        header.setTopic(message.getTopic());
        header.setDefaultTopic(message.getDefaultTopic() != null ? message.getDefaultTopic() : "TBW102");
        header.setDefaultTopicQueueNums(message.getDefaultTopicQueueNums() > 0 ? message.getDefaultTopicQueueNums() : 4);
        header.setQueueId(message.getQueueId() != null ? message.getQueueId() : null);
        header.setSysFlag(message.getSysFlag());
        header.setBornTimestamp(message.getBornTimestamp());
        header.setFlag(message.getFlag());
        header.setProperties(message.getProperties());
        header.setReconsumeTimes(message.getReconsumeTimes());
        header.setUnitMode(message.isUnitMode());
        header.setMaxReconsumeTimes(message.getMaxReconsumeTimes());
        header.setBatch(message.isBatch());
        header.setTopicSysFlag(message.getTopicSysFlag());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, header);
        request.setBody(message.getBody());
        request.makeCustomHeaderToNet();

        String brokerAddr = this.storageConfig.getBrokerAddr();
        System.out.println("[DEBUG] putMessage extFields: " + request.getExtFields());
        byte[] headerData = request.headerEncode();
        System.out.println("[DEBUG] putMessage headerJson: " + new String(headerData, "UTF-8"));
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            String msgId = response.getExtFields() != null ? response.getExtFields().get("msgId") : null;
            int queueId = response.getExtFields() != null && response.getExtFields().get("queueId") != null
                    ? Integer.parseInt(response.getExtFields().get("queueId")) : -1;
            long queueOffset = response.getExtFields() != null && response.getExtFields().get("queueOffset") != null
                    ? Long.parseLong(response.getExtFields().get("queueOffset")) : -1L;
            return PutResult.success(msgId, queueId, queueOffset);
        } else {
            return PutResult.fail(response.getCode(), response.getRemark());
        }
    }

    @Override
    public PullResult pullMessage(String consumerGroup, String topic, int queueId, long queueOffset, int maxMsgNums, long suspendTimeoutMillis, String subscription, String expressionType) throws Exception {
        PullMessageRequestHeader header = new PullMessageRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setQueueOffset(queueOffset);
        header.setMaxMsgNums(maxMsgNums);
        header.setSysFlag(0);
        header.setCommitOffset(0L);
        header.setSuspendTimeoutMillis(suspendTimeoutMillis);
        header.setSubscription(subscription);
        header.setSubVersion(0L);
        header.setExpressionType(expressionType != null ? expressionType : "TAG");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, header);
        request.makeCustomHeaderToNet();

        String brokerAddr = this.storageConfig.getBrokerAddr();
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, suspendTimeoutMillis + 1000);

        long nextBeginOffset = response.getExtFields() != null && response.getExtFields().get("nextBeginOffset") != null
                ? Long.parseLong(response.getExtFields().get("nextBeginOffset")) : 0L;
        long minOffset = response.getExtFields() != null && response.getExtFields().get("minOffset") != null
                ? Long.parseLong(response.getExtFields().get("minOffset")) : 0L;
        long maxOffset = response.getExtFields() != null && response.getExtFields().get("maxOffset") != null
                ? Long.parseLong(response.getExtFields().get("maxOffset")) : 0L;

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            return PullResult.found(response.getBody(), nextBeginOffset, minOffset, maxOffset);
        } else if (response.getCode() == ResponseCode.PULL_NOT_FOUND) {
            return PullResult.notFound(nextBeginOffset, minOffset, maxOffset);
        } else if (response.getCode() == ResponseCode.PULL_RETRY_IMMEDIATELY) {
            return PullResult.notFound(nextBeginOffset, minOffset, maxOffset);
        } else {
            PullResult result = new PullResult();
            result.setResponseCode(response.getCode());
            result.setNextBeginOffset(nextBeginOffset);
            result.setMinOffset(minOffset);
            result.setMaxOffset(maxOffset);
            return result;
        }
    }

    @Override
    public OffsetResult queryConsumerOffset(String consumerGroup, String topic, int queueId) throws Exception {
        QueryConsumerOffsetRequestHeader header = new QueryConsumerOffsetRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, header);
        request.makeCustomHeaderToNet();

        String brokerAddr = this.storageConfig.getBrokerAddr();
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            long offset = response.getExtFields() != null && response.getExtFields().get("offset") != null
                    ? Long.parseLong(response.getExtFields().get("offset")) : -1L;
            return OffsetResult.success(offset);
        } else {
            return OffsetResult.fail(response.getCode(), response.getRemark());
        }
    }

    @Override
    public void updateConsumerOffset(String consumerGroup, String topic, int queueId, long commitOffset) throws Exception {
        UpdateConsumerOffsetRequestHeader header = new UpdateConsumerOffsetRequestHeader();
        header.setConsumerGroup(consumerGroup);
        header.setTopic(topic);
        header.setQueueId(queueId);
        header.setCommitOffset(commitOffset);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, header);
        request.makeCustomHeaderToNet();

        String brokerAddr = this.storageConfig.getBrokerAddr();
        RemotingCommand response = this.remotingClient.invokeSync(brokerAddr, request, 3000);

        if (response.getCode() != RemotingSysResponseCode.SUCCESS) {
            throw new RuntimeException("updateConsumerOffset failed, code: " + response.getCode() + ", remark: " + response.getRemark());
        }
    }

    @Override
    public boolean healthCheck() {
        return this.initialized;
    }

    @Override
    public RemotingCommand forwardToBroker(RemotingCommand request) throws Exception {
        String brokerAddr = this.storageConfig.getBrokerAddr();
        
        RemotingCommand forwardRequest = RemotingCommand.createRequestCommand(request.getCode(), request.getCustomHeader());
        forwardRequest.setExtFields(request.getExtFields());
        forwardRequest.setBody(request.getBody());
        forwardRequest.setFlag(request.getFlag());
        forwardRequest.setRemark(request.getRemark());
        
        return this.remotingClient.invokeSync(brokerAddr, forwardRequest, 3000);
    }
}
