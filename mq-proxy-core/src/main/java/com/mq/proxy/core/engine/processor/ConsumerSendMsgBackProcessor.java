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
import java.util.Map;

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
        // 优先从请求 extFields 中获取 bname（消费者通过 context.getMessageQueue().getBrokerName() 设置）
        if (request.getExtFields() != null) {
            String bname = request.getExtFields().get("bname");
            if (bname != null && !bname.isEmpty()) {
                return bname;
            }
        }
        // fallback: 从 customHeader 中获取 brokerName
        // 注意：proxy 的 ConsumerSendMsgBackRequestHeader 没有 brokerName 字段，
        // 但如果消费者使用 RocketMQ 原生 header，decoder 可能会设置 customHeader
        // 此处不依赖这个路径，仅作为最终兜底

        // fallback: 使用 originTopic 解析 brokerName
        // 注意：这里无法知道原始消息的 queueId，所以无法精确路由
        // 如果 originTopic 的路由只有一个 broker，则可以正确路由
        // 如果有多个 broker，则可能路由到错误的 broker
        String originTopic = header.getOriginTopic();
        if (originTopic != null && this.virtualRouteManager != null) {
            // 确保路由信息已缓存
            this.virtualRouteManager.getRouteInfoByTopic(originTopic);
            String brokerName = this.virtualRouteManager.findBrokerNameByTopicAndQueueId(originTopic, 0);
            if (brokerName != null && !brokerName.isEmpty()) {
                log.warn("ConsumerSendMsgBack: bname not found in request, fallback to originTopic route. originTopic={}, resolvedBrokerName={}",
                        originTopic, brokerName);
                return brokerName;
            }
        }
        log.error("ConsumerSendMsgBack: cannot resolve brokerName, bname not in request and originTopic route not found. originTopic={}",
                originTopic);
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
        // 先将原始请求的 extFields 作为基础，确保不丢失任何字段（包括 bname 等）
        HashMap<String, String> extFields = new HashMap<>();
        if (originalRequest.getExtFields() != null) {
            extFields.putAll(originalRequest.getExtFields());
        }
        // 将 proxy 解析出的 header 字段合并到 extFields（覆盖同名字段，确保使用 proxy 解析后的值）
        Map<String, String> headerMap = header.toMap();
        if (headerMap != null && !headerMap.isEmpty()) {
            extFields.putAll(headerMap);
        }
        // 确保 bname 使用解析出的 brokerName（可能来自原始请求或 fallback 解析）
        if (brokerName != null && !brokerName.isEmpty()) {
            extFields.put("bname", brokerName);
        }

        RemotingCommand forwardRequest = RemotingCommand.createRequestCommand(
                RequestCode.CONSUMER_SEND_MSG_BACK, header);
        forwardRequest.setExtFields(extFields);
        forwardRequest.makeCustomHeaderToNet();
        return forwardRequest;
    }
}
