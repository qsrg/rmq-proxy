package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.mq.proxy.core.server.RemotingProcessor;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class ConsumerSendMsgBackProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(ConsumerSendMsgBackProcessor.class);

    private final MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;

    public ConsumerSendMsgBackProcessor(MessageEngine messageEngine) {
        this.messageEngine = messageEngine;
    }

    public void setVirtualRouteManager(VirtualRouteManager virtualRouteManager) {
        this.virtualRouteManager = virtualRouteManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        ConsumerSendMsgBackRequestHeader requestHeader = parseRequestHeader(request);

        String brokerName = resolveBrokerName(requestHeader, request);
        if (brokerName == null) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                    "cannot resolve brokerName for ConsumerSendMsgBack");
        }
        String brokerAddr = resolveBrokerAddr(brokerName);
        if (brokerAddr == null) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                    "cannot resolve brokerAddr for brokerName=" + brokerName);
        }

        log.info("ConsumerSendMsgBack: group={}, offset={}, delayLevel={}, originTopic={}, brokerName={}",
                requestHeader.getGroup(), requestHeader.getOffset(), requestHeader.getDelayLevel(),
                requestHeader.getOriginTopic(), brokerName);

        RemotingCommand forwardRequest = buildForwardRequest(requestHeader, request, brokerName);
        forwardRequest.setBody(request.getBody());

        try {
            RemotingCommand brokerResponse = messageEngine.getStorageAdapter().forwardToBroker(forwardRequest, brokerAddr);
            return brokerResponse;
        } catch (Exception e) {
            log.error("ConsumerSendMsgBack forward failed: group={}, offset={}, error={}",
                    requestHeader.getGroup(), requestHeader.getOffset(), e.getMessage());
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, e.getMessage());
        }
    }

    private ConsumerSendMsgBackRequestHeader parseRequestHeader(RemotingCommand request) {
        ConsumerSendMsgBackRequestHeader header = (ConsumerSendMsgBackRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }

        header = new ConsumerSendMsgBackRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            if (extFields.get("offset") != null) {
                header.setOffset(Long.parseLong(extFields.get("offset")));
            }
            header.setGroup(extFields.get("group"));
            if (extFields.get("delayLevel") != null) {
                header.setDelayLevel(Integer.parseInt(extFields.get("delayLevel")));
            }
            header.setOriginMsgId(extFields.get("originMsgId"));
            header.setOriginTopic(extFields.get("originTopic"));
            if (extFields.get("unitMode") != null) {
                header.setUnitMode(Boolean.parseBoolean(extFields.get("unitMode")));
            }
            if (extFields.get("maxReconsumeTimes") != null) {
                header.setMaxReconsumeTimes(Integer.parseInt(extFields.get("maxReconsumeTimes")));
            }
        }
        return header;
    }

    private String resolveBrokerName(ConsumerSendMsgBackRequestHeader header, RemotingCommand request) {
        if (request.getExtFields() != null) {
            String bname = request.getExtFields().get("bname");
            if (bname != null && !bname.isEmpty()) {
                return bname;
            }
        }
        String originTopic = header.getOriginTopic();
        if (originTopic != null && this.virtualRouteManager != null) {
            return this.virtualRouteManager.findBrokerNameByTopicAndQueueId(originTopic, 0);
        }
        return null;
    }

    private String resolveBrokerAddr(String brokerName) {
        if (brokerName != null && virtualRouteManager != null) {
            String realAddr = virtualRouteManager.getRealBrokerAddr(brokerName);
            if (realAddr != null) {
                return realAddr;
            }
        }
        return null;
    }

    private RemotingCommand buildForwardRequest(ConsumerSendMsgBackRequestHeader header, RemotingCommand originalRequest, String brokerName) {
        RemotingCommand forwardRequest = RemotingCommand.createRequestCommand(
                RequestCode.CONSUMER_SEND_MSG_BACK, header);
        forwardRequest.makeCustomHeaderToNet();
        HashMap<String, String> extFields = new HashMap<>();
        if (originalRequest.getExtFields() != null) {
            extFields.putAll(originalRequest.getExtFields());
        }
        if (brokerName != null && !brokerName.isEmpty()) {
            extFields.put("bname", brokerName);
        }
        forwardRequest.setExtFields(extFields);
        return forwardRequest;
    }
}
