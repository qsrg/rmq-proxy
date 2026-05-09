package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetRequestHeader;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetResponseHeader;
import com.mq.proxy.core.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.mq.proxy.core.server.RemotingProcessor;
import com.mq.proxy.core.storage.model.OffsetResult;
import io.netty.channel.Channel;

import java.util.HashMap;

public class ConsumerManageProcessor implements RemotingProcessor {

    private final MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;

    public ConsumerManageProcessor(MessageEngine messageEngine) {
        this.messageEngine = messageEngine;
    }

    public void setVirtualRouteManager(VirtualRouteManager virtualRouteManager) {
        this.virtualRouteManager = virtualRouteManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        int requestCode = request.getCode();

        if (requestCode == RequestCode.QUERY_CONSUMER_OFFSET) {
            return queryConsumerOffset(request);
        } else if (requestCode == RequestCode.UPDATE_CONSUMER_OFFSET) {
            return updateConsumerOffset(request);
        } else {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, "unsupported request code");
        }
    }

    private RemotingCommand queryConsumerOffset(RemotingCommand request) {
        QueryConsumerOffsetRequestHeader requestHeader = parseQueryConsumerOffsetRequestHeader(request);
        String brokerName = resolveBrokerName(requestHeader.getTopic(), requestHeader.getQueueId() != null ? requestHeader.getQueueId() : 0, request);
        OffsetResult offsetResult = messageEngine.queryConsumerOffset(
                requestHeader.getConsumerGroup(),
                requestHeader.getTopic(),
                requestHeader.getQueueId() != null ? requestHeader.getQueueId() : 0,
                brokerName
        );

        if (offsetResult.isSuccess()) {
            QueryConsumerOffsetResponseHeader responseHeader = new QueryConsumerOffsetResponseHeader();
            responseHeader.setOffset(offsetResult.getOffset());
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
            response.setCustomHeader(responseHeader);
            response.makeCustomHeaderToNet();
            return response;
        } else {
            return RemotingCommand.createResponseCommand(offsetResult.getResponseCode(), offsetResult.getRemark());
        }
    }

    private RemotingCommand updateConsumerOffset(RemotingCommand request) {
        UpdateConsumerOffsetRequestHeader requestHeader = parseUpdateConsumerOffsetRequestHeader(request);
        String brokerName = resolveBrokerName(requestHeader.getTopic(), requestHeader.getQueueId() != null ? requestHeader.getQueueId() : 0, request);
        try {
            messageEngine.updateConsumerOffset(
                    requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(),
                    requestHeader.getQueueId() != null ? requestHeader.getQueueId() : 0,
                    requestHeader.getCommitOffset() != null ? requestHeader.getCommitOffset() : 0,
                    brokerName
            );
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        } catch (Exception e) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, e.getMessage());
        }
    }

    private QueryConsumerOffsetRequestHeader parseQueryConsumerOffsetRequestHeader(RemotingCommand request) {
        QueryConsumerOffsetRequestHeader header = (QueryConsumerOffsetRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new QueryConsumerOffsetRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setConsumerGroup(extFields.get("consumerGroup"));
            header.setTopic(extFields.get("topic"));
            if (extFields.get("queueId") != null) {
                header.setQueueId(Integer.parseInt(extFields.get("queueId")));
            }
        }
        return header;
    }

    private UpdateConsumerOffsetRequestHeader parseUpdateConsumerOffsetRequestHeader(RemotingCommand request) {
        UpdateConsumerOffsetRequestHeader header = (UpdateConsumerOffsetRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new UpdateConsumerOffsetRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setConsumerGroup(extFields.get("consumerGroup"));
            header.setTopic(extFields.get("topic"));
            if (extFields.get("queueId") != null) {
                header.setQueueId(Integer.parseInt(extFields.get("queueId")));
            }
            if (extFields.get("commitOffset") != null) {
                header.setCommitOffset(Long.parseLong(extFields.get("commitOffset")));
            }
        }
        return header;
    }

    private String resolveBrokerName(String topic, int queueId, RemotingCommand request) {
        if (request.getExtFields() != null) {
            String bname = request.getExtFields().get("bname");
            if (bname != null && !bname.isEmpty()) {
                return bname;
            }
        }
        if (this.virtualRouteManager != null) {
            return this.virtualRouteManager.findBrokerNameByTopicAndQueueId(topic, queueId);
        }
        return null;
    }
}
