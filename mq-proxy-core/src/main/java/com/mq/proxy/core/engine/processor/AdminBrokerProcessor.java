package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import com.mq.proxy.core.protocol.header.GetMaxOffsetRequestHeader;
import com.mq.proxy.core.protocol.header.GetMinOffsetRequestHeader;
import com.mq.proxy.core.protocol.header.SearchOffsetRequestHeader;
import com.mq.proxy.core.server.RemotingProcessor;
import com.mq.proxy.core.storage.StorageAdapter;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class AdminBrokerProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(AdminBrokerProcessor.class);

    private final MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;

    public AdminBrokerProcessor(MessageEngine messageEngine) {
        this.messageEngine = messageEngine;
    }

    public void setVirtualRouteManager(VirtualRouteManager virtualRouteManager) {
        this.virtualRouteManager = virtualRouteManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        int requestCode = request.getCode();

        switch (requestCode) {
            case RequestCode.GET_MAX_OFFSET:
                return forwardOffsetQuery(request);
            case RequestCode.GET_MIN_OFFSET:
                return forwardOffsetQuery(request);
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
                return forwardOffsetQuery(request);
            case RequestCode.GET_EARLIEST_MSG_STORETIME:
                return forwardOffsetQuery(request);
            case RequestCode.GET_ALL_CONSUMER_OFFSET:
                return forwardToBroker(request);
            case RequestCode.GET_ALL_DELAY_OFFSET:
                return forwardToBroker(request);
            case RequestCode.QUERY_BROKER_OFFSET:
                return forwardToBroker(request);
            case RequestCode.QUERY_MESSAGE:
                return forwardToBroker(request);
            case RequestCode.VIEW_MESSAGE_BY_ID:
                return forwardToBroker(request);
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return forwardToBroker(request);
            case RequestCode.GET_ALL_TOPIC_CONFIG:
                return forwardToBroker(request);
            case RequestCode.GET_TOPIC_CONFIG_LIST:
                return forwardToBroker(request);
            case RequestCode.GET_TOPIC_NAME_LIST:
                return forwardToBroker(request);
            case RequestCode.DELETE_TOPIC_IN_BROKER:
                return forwardToBroker(request);
            case RequestCode.UPDATE_BROKER_CONFIG:
                return forwardToBroker(request);
            case RequestCode.GET_BROKER_CONFIG:
                return forwardToBroker(request);
            case RequestCode.GET_BROKER_RUNTIME_INFO:
                return forwardToBroker(request);
            case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
                return forwardToBroker(request);
            case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
                return forwardToBroker(request);
            case RequestCode.GET_TOPIC_STATS_INFO:
                return forwardToBroker(request);
            case RequestCode.GET_CONSUMER_CONNECTION_LIST:
                return forwardToBroker(request);
            case RequestCode.GET_PRODUCER_CONNECTION_LIST:
                return forwardToBroker(request);
            case RequestCode.DELETE_SUBSCRIPTIONGROUP:
                return forwardToBroker(request);
            case RequestCode.GET_CONSUME_STATS:
                return forwardToBroker(request);
            case RequestCode.RESET_CONSUMER_OFFSET_IN_BROKER:
                return forwardToBroker(request);
            case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
                return forwardToBroker(request);
            case RequestCode.QUERY_CONSUME_TIME_SPAN:
                return forwardToBroker(request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
                return forwardToBroker(request);
            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return forwardToBroker(request);
            case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
                return forwardToBroker(request);
            case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
                return forwardToBroker(request);
            case RequestCode.CLONE_GROUP_OFFSET:
                return forwardToBroker(request);
            case RequestCode.GET_BROKER_CONSUME_STATS:
                return forwardToBroker(request);
            default:
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED,
                        "unsupported request code: " + requestCode);
        }
    }

    private RemotingCommand forwardOffsetQuery(RemotingCommand request) throws Exception {
        StorageAdapter adapter = messageEngine.getDefaultStorageAdapter();
        if (adapter == null) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                    "no storage adapter available");
        }

        String brokerName = resolveBrokerName(request);
        if (brokerName == null) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                    "cannot resolve brokerName for request code=" + request.getCode());
        }
        String brokerAddr = resolveBrokerAddr(brokerName);
        if (brokerAddr == null) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                    "cannot resolve brokerAddr for brokerName=" + brokerName);
        }

        log.debug("Forwarding offset query: code={}, brokerName={}, brokerAddr={}",
                request.getCode(), brokerName, brokerAddr);

        try {
            return adapter.forwardToBroker(request, brokerAddr);
        } catch (Exception e) {
            log.error("Offset query forward failed: code={}, error={}", request.getCode(), e.getMessage());
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, e.getMessage());
        }
    }

    private RemotingCommand forwardToBroker(RemotingCommand request) throws Exception {
        String brokerAddr = null;
        if (virtualRouteManager != null) {
            String brokerName = resolveBrokerName(request);
            if (brokerName != null) {
                brokerAddr = virtualRouteManager.getRealBrokerAddr(brokerName);
            }
        }

        StorageAdapter adapter = messageEngine.getDefaultStorageAdapter();
        if (adapter == null) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                    "no storage adapter available");
        }

        try {
            return adapter.forwardToBroker(request, brokerAddr);
        } catch (Exception e) {
            log.error("Admin forward failed: code={}, error={}", request.getCode(), e.getMessage());
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, e.getMessage());
        }
    }

    private String resolveBrokerName(RemotingCommand request) {
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            String bname = extFields.get("bname");
            if (bname != null && !bname.isEmpty()) {
                return bname;
            }
            String topic = extFields.get("topic");
            if (topic != null && virtualRouteManager != null) {
                return virtualRouteManager.findBrokerNameByTopicAndQueueId(topic, 0);
            }
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
}