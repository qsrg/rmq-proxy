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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class ConsumerManageProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(ConsumerManageProcessor.class);

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
        log.info("QUERY_CONSUMER_OFFSET: group={}, topic={}, queueId={}, brokerName={}", requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), brokerName);
        OffsetResult offsetResult = messageEngine.queryConsumerOffset(
                requestHeader.getConsumerGroup(),
                requestHeader.getTopic(),
                requestHeader.getQueueId() != null ? requestHeader.getQueueId() : 0,
                brokerName
        );

        if (offsetResult.isSuccess()) {
            log.info("QUERY_CONSUMER_OFFSET_RESULT: group={}, topic={}, queueId={}, offset={}", requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), offsetResult.getOffset());
            QueryConsumerOffsetResponseHeader responseHeader = new QueryConsumerOffsetResponseHeader();
            responseHeader.setOffset(offsetResult.getOffset());
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
            response.setCustomHeader(responseHeader);
            response.makeCustomHeaderToNet();
            return response;
        } else {
            log.warn("QUERY_CONSUMER_OFFSET_FAILED: group={}, topic={}, queueId={}, code={}, remark={}", requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), offsetResult.getResponseCode(), offsetResult.getRemark());
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
            if (header.getBrokerName() == null && request.getExtFields() != null) {
                header.setBrokerName(request.getExtFields().get("bname"));
            }
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
            header.setBrokerName(extFields.get("bname"));
        }
        return header;
    }

    private UpdateConsumerOffsetRequestHeader parseUpdateConsumerOffsetRequestHeader(RemotingCommand request) {
        UpdateConsumerOffsetRequestHeader header = (UpdateConsumerOffsetRequestHeader) request.getCustomHeader();
        if (header != null) {
            if (header.getBrokerName() == null && request.getExtFields() != null) {
                header.setBrokerName(request.getExtFields().get("bname"));
            }
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
            header.setBrokerName(extFields.get("bname"));
        }
        return header;
    }

    private String resolveBrokerName(String topic, int queueId, RemotingCommand request) {
        if (request.getCustomHeader() instanceof QueryConsumerOffsetRequestHeader) {
            String brokerName = ((QueryConsumerOffsetRequestHeader) request.getCustomHeader()).getBrokerName();
            if (brokerName != null && !brokerName.isEmpty()) {
                return brokerName;
            }
        }
        if (request.getCustomHeader() instanceof UpdateConsumerOffsetRequestHeader) {
            String brokerName = ((UpdateConsumerOffsetRequestHeader) request.getCustomHeader()).getBrokerName();
            if (brokerName != null && !brokerName.isEmpty()) {
                return brokerName;
            }
        }
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
