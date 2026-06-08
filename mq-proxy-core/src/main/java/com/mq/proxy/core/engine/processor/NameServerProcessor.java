package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.GetRouteInfoRequestHeader;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.RemotingProcessor;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class NameServerProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(NameServerProcessor.class);

    private final VirtualRouteManager virtualRouteManager;

    public NameServerProcessor(VirtualRouteManager virtualRouteManager) {
        this.virtualRouteManager = virtualRouteManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        log.debug("NameServerProcessor received request: code={}, opaque={}, channel={}",
                request.getCode(), request.getOpaque(), channel.remoteAddress());
        int requestCode = request.getCode();

        if (requestCode == RequestCode.GET_ROUTEINFO_BY_TOPIC) {
            return getRouteInfoByTopic(request);
        } else if (requestCode == RequestCode.REGISTER_BROKER) {
            return forwardToNameServer(request);
        } else if (requestCode == RequestCode.UNREGISTER_BROKER) {
            return forwardToNameServer(request);
        } else if (requestCode == RequestCode.GET_BROKER_CLUSTER_INFO) {
            return forwardToNameServer(request);
        } else if (requestCode == RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER) {
            return forwardToNameServer(request);
        } else if (requestCode == RequestCode.DELETE_TOPIC_IN_NAMESRV) {
            return forwardToNameServer(request);
        } else {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, "unsupported request code");
        }
    }

    private RemotingCommand getRouteInfoByTopic(RemotingCommand request) {
        GetRouteInfoRequestHeader requestHeader = parseGetRouteInfoRequestHeader(request);
        log.debug("getRouteInfoByTopic: topic={}", requestHeader.getTopic());
        TopicRouteInfo routeInfo = virtualRouteManager.getRouteInfoByTopic(requestHeader.getTopic());

        if (routeInfo != null) {
            log.debug("RouteInfo found for topic, brokerDatas count={}", routeInfo.getBrokerDatas() != null ? routeInfo.getBrokerDatas().size() : 0);
            byte[] body = RouteInfoSerializer.encodeTopicRouteInfo(routeInfo);
            log.debug("RouteInfo body encoded, size={}", body.length);
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
            response.setBody(body);
            return response;
        }
        return RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, "no route info for this topic");
    }

    private RemotingCommand forwardToNameServer(RemotingCommand request) {
        NettyRemotingClient namesrvClient = virtualRouteManager.getNamesrvClient();
        String namesrvAddr = virtualRouteManager.getNamesrvAddr();
        if (namesrvClient != null && namesrvAddr != null) {
            // 支持多namesrv地址，用分号分隔
            String[] addrs = namesrvAddr.split(";");
            Exception lastException = null;
            for (String addr : addrs) {
                String trimmedAddr = addr.trim();
                if (trimmedAddr.isEmpty()) {
                    continue;
                }
                try {
                    return namesrvClient.invokeSync(trimmedAddr, request, 3000);
                } catch (Exception e) {
                    lastException = e;
                    log.warn("forwardToNameServer failed for addr={}, error={}", trimmedAddr, e.getMessage());
                }
            }
            if (lastException != null) {
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, lastException.getMessage());
            }
        }
        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "namesrv not available");
    }

    private GetRouteInfoRequestHeader parseGetRouteInfoRequestHeader(RemotingCommand request) {
        GetRouteInfoRequestHeader header = (GetRouteInfoRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new GetRouteInfoRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setTopic(extFields.get("topic"));
        }
        return header;
    }

    private boolean isRetryOrDlqTopic(String topic) {
        return topic != null && (topic.startsWith("%RETRY%") || topic.startsWith("%DLQ%"));
    }
}
