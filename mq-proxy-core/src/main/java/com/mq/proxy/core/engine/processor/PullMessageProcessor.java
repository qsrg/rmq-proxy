package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.PullMessageRequestHeader;
import com.mq.proxy.core.protocol.header.PullMessageResponseHeader;
import com.mq.proxy.core.server.RemotingProcessor;
import com.mq.proxy.core.storage.PullMessageCallback;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PullResult;
import io.netty.channel.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

public class PullMessageProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(PullMessageProcessor.class);

    private final MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;

    public PullMessageProcessor(MessageEngine messageEngine) {
        this.messageEngine = messageEngine;
    }

    public void setVirtualRouteManager(VirtualRouteManager virtualRouteManager) {
        this.virtualRouteManager = virtualRouteManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        final PullMessageRequestHeader requestHeader = parsePullMessageRequestHeader(request);
        final String topic = requestHeader.getTopic();
        final int queueId = requestHeader.getQueueId() != null ? requestHeader.getQueueId() : 0;
        final String brokerName = resolveBrokerName(topic, queueId, request);
        final long queueOffset = requestHeader.getQueueOffset() != null ? requestHeader.getQueueOffset() : 0;
        final int maxMsgNums = requestHeader.getMaxMsgNums() != null ? requestHeader.getMaxMsgNums() : 32;
        final int sysFlag = requestHeader.getSysFlag() != null ? requestHeader.getSysFlag() : 0;
        final long commitOffset = requestHeader.getCommitOffset() != null ? requestHeader.getCommitOffset() : -1;
        final long suspendTimeoutMillis = requestHeader.getSuspendTimeoutMillis() != null ? requestHeader.getSuspendTimeoutMillis() : 0;
        final long subVersion = requestHeader.getSubVersion() != null ? requestHeader.getSubVersion() : 0L;

        log.debug("PULL_REQUEST: opaque={}, group={}, topic={}, queueId={}, offset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeout={}, subscription={}, exprType={}, brokerName={}",
                request.getOpaque(), requestHeader.getConsumerGroup(), topic, queueId, queueOffset, maxMsgNums,
                sysFlag, commitOffset, suspendTimeoutMillis, requestHeader.getSubscription(),
                requestHeader.getExpressionType(), brokerName);

        messageEngine.pullMessageAsync(requestHeader.getConsumerGroup(), topic, queueId, queueOffset, maxMsgNums,
                sysFlag, commitOffset, suspendTimeoutMillis, requestHeader.getSubscription(),
                requestHeader.getExpressionType(), subVersion, brokerName, new PullMessageCallback() {
                    @Override
                    public void onSuccess(PullResult pullResult) {
                        try {
                            log.debug("PULL_RESULT: opaque={}, group={}, topic={}, queueId={}, responseCode={}, nextBeginOffset={}, minOffset={}, maxOffset={}, suggestBrokerId={}, hasBody={}",
                                    request.getOpaque(), requestHeader.getConsumerGroup(), topic, queueId,
                                    pullResult.getResponseCode(), pullResult.getNextBeginOffset(),
                                    pullResult.getMinOffset(), pullResult.getMaxOffset(),
                                    pullResult.getSuggestWhichBrokerId(),
                                    pullResult.getMessageBinary() != null && pullResult.getMessageBinary().length > 0);

                            RemotingCommand response = buildPullResponse(pullResult, topic);
                            if (channel == null || !channel.isActive()) {
                                log.debug("skip pull response because channel is inactive, opaque={}, group={}, topic={}, queueId={}",
                                        request.getOpaque(), requestHeader.getConsumerGroup(), topic, queueId);
                                return;
                            }
                            response.setOpaque(request.getOpaque());
                            response.setSerializeTypeCurrentRPC(request.getSerializeTypeCurrentRPC());
                            channel.writeAndFlush(response);
                        } catch (Exception e) {
                            log.error("async pull response handling failed, opaque={}, remoteChannel={}",
                                    request.getOpaque(), channel, e);
                        }
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        log.error("unexpected async pull exception after MessageEngine normalization, opaque={}",
                                request.getOpaque(), throwable);
                    }
                });

        return null;
    }

    private RemotingCommand buildPullResponse(PullResult pullResult, String topic) throws IOException {
        PullMessageResponseHeader responseHeader = new PullMessageResponseHeader();
        responseHeader.setNextBeginOffset(pullResult.getNextBeginOffset());
        responseHeader.setMinOffset(pullResult.getMinOffset());
        responseHeader.setMaxOffset(pullResult.getMaxOffset());
        if (pullResult.getSuggestWhichBrokerId() != null) {
            responseHeader.setSuggestWhichBrokerId(Long.parseLong(pullResult.getSuggestWhichBrokerId()));
        } else {
            responseHeader.setSuggestWhichBrokerId(0L);
        }

        RemotingCommand response = RemotingCommand.createResponseCommand(pullResult.getResponseCode());
        response.setCustomHeader(responseHeader);
        response.makeCustomHeaderToNet();

        if (pullResult.getMessageBinary() != null && pullResult.getMessageBinary().length > 0) {
            response.setBody(pullResult.getMessageBinary());
        } else if (pullResult.getMessageList() != null && !pullResult.getMessageList().isEmpty()) {
            response.setBody(encodeMessageList(pullResult.getMessageList()));
        }

        int originalResponseCode = pullResult.getResponseCode();
        if (pullResult.getResponseCode() == RemotingSysResponseCode.SUCCESS
                || pullResult.getResponseCode() == ResponseCode.PULL_NOT_FOUND
                || pullResult.getResponseCode() == ResponseCode.PULL_RETRY_IMMEDIATELY
                || pullResult.getResponseCode() == ResponseCode.PULL_OFFSET_MOVED) {
            response.setCode(pullResult.getResponseCode());
        } else if (pullResult.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
            response.setCode(ResponseCode.PULL_NOT_FOUND);
            log.info("PULL_TOPIC_NOT_EXIST: originalCode={}, convertedTo=PULL_NOT_FOUND for topic={}",
                    originalResponseCode, topic);
        } else if (pullResult.getResponseCode() == ResponseCode.SUBSCRIPTION_NOT_EXIST
                || pullResult.getResponseCode() == ResponseCode.SUBSCRIPTION_NOT_LATEST) {
            response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
            messageEngine.triggerHeartbeatForward();
            log.info("PULL_SUBSCRIPTION_ISSUE: originalCode={}, convertedTo=PULL_RETRY_IMMEDIATELY, triggered heartbeat", originalResponseCode);
        }

        log.debug("PULL_RESPONSE: topic={}, responseCode={}, nextBeginOffset={}, minOffset={}, maxOffset={}",
                topic, response.getCode(), responseHeader.getNextBeginOffset(),
                responseHeader.getMinOffset(), responseHeader.getMaxOffset());
        return response;
    }

    private PullMessageRequestHeader parsePullMessageRequestHeader(RemotingCommand request) {
        PullMessageRequestHeader header = (PullMessageRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new PullMessageRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setConsumerGroup(extFields.get("consumerGroup"));
            header.setTopic(extFields.get("topic"));
            if (extFields.get("queueId") != null) {
                header.setQueueId(Integer.parseInt(extFields.get("queueId")));
            }
            if (extFields.get("queueOffset") != null) {
                header.setQueueOffset(Long.parseLong(extFields.get("queueOffset")));
            }
            if (extFields.get("maxMsgNums") != null) {
                header.setMaxMsgNums(Integer.parseInt(extFields.get("maxMsgNums")));
            }
            if (extFields.get("sysFlag") != null) {
                header.setSysFlag(Integer.parseInt(extFields.get("sysFlag")));
            }
            if (extFields.get("commitOffset") != null) {
                header.setCommitOffset(Long.parseLong(extFields.get("commitOffset")));
            }
            if (extFields.get("suspendTimeoutMillis") != null) {
                header.setSuspendTimeoutMillis(Long.parseLong(extFields.get("suspendTimeoutMillis")));
            }
            header.setSubscription(extFields.get("subscription"));
            if (extFields.get("subVersion") != null) {
                header.setSubVersion(Long.parseLong(extFields.get("subVersion")));
            }
            header.setExpressionType(extFields.get("expressionType"));
        }
        return header;
    }

    private byte[] encodeMessageList(List<InternalMessage> messageList) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        for (InternalMessage msg : messageList) {
            byte[] body = msg.getBody() != null ? msg.getBody() : new byte[0];
            String topic = msg.getTopic() != null ? msg.getTopic() : "";
            byte[] topicBytes = topic.getBytes("UTF-8");
            byte[] propertiesBytes = msg.getProperties() != null ? msg.getProperties().getBytes("UTF-8") : new byte[0];

            int sysFlag = msg.getSysFlag();
            int bornHostLength = (sysFlag & 0x01) == 0x01 ? 16 : 4;
            int storeHostLength = (sysFlag & 0x01) == 0x01 ? 16 : 4;

            int msgLen = 4 + 4 + 4 + 4 + 8 + 4 + 8 + bornHostLength + 8 + storeHostLength
                    + 4 + 8 + 4 + body.length + 1 + topicBytes.length + 2 + propertiesBytes.length;

            ByteBuffer buffer = ByteBuffer.allocate(4 + msgLen);
            buffer.putInt(msgLen);
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.putInt(msg.getQueueId());
            buffer.putLong(0);
            buffer.putInt(sysFlag);
            buffer.putLong(msg.getBornTimestamp());
            buffer.put(new byte[bornHostLength]);
            buffer.putLong(0);
            buffer.put(new byte[storeHostLength]);
            buffer.putInt(msg.getReconsumeTimes());
            buffer.putLong(0);
            buffer.putInt(body.length);
            buffer.put(body);

            buffer.put((byte) topicBytes.length);
            buffer.put(topicBytes);

            buffer.putShort((short) propertiesBytes.length);
            buffer.put(propertiesBytes);

            bos.write(buffer.array());
        }
        return bos.toByteArray();
    }

    private String resolveBrokerName(String topic, int queueId, RemotingCommand request) {
        String requestBrokerName = getRequestBrokerName(request);
        if (requestBrokerName != null) {
            return requestBrokerName;
        }

        if (isRetryOrDlqTopic(topic) && this.virtualRouteManager != null) {
            this.virtualRouteManager.getRouteInfoByTopic(topic);
            String brokerName = this.virtualRouteManager.findBrokerNameByTopicAndQueueId(topic, queueId);
            if (brokerName != null && !brokerName.isEmpty()) {
                return brokerName;
            }
        }
        if (this.virtualRouteManager != null) {
            return this.virtualRouteManager.findBrokerNameByTopicAndQueueId(topic, queueId);
        }
        return null;
    }

    private String getRequestBrokerName(RemotingCommand request) {
        if (request.getExtFields() != null) {
            String bname = request.getExtFields().get("bname");
            if (bname != null && !bname.isEmpty()) {
                return bname;
            }
        }
        return null;
    }

    private boolean isRetryOrDlqTopic(String topic) {
        return topic != null && (topic.startsWith("%RETRY%") || topic.startsWith("%DLQ%"));
    }
}
