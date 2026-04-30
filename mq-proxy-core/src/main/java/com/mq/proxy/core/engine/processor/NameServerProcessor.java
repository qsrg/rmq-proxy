package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.GetRouteInfoRequestHeader;
import com.mq.proxy.core.protocol.header.RegisterBrokerRequestHeader;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.RemotingProcessor;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import io.netty.channel.Channel;

import java.util.HashMap;

public class NameServerProcessor implements RemotingProcessor {

    private final VirtualRouteManager virtualRouteManager;

    public NameServerProcessor(VirtualRouteManager virtualRouteManager) {
        this.virtualRouteManager = virtualRouteManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        System.out.println("[DEBUG] NameServerProcessor received request: code=" + request.getCode() + ", opaque=" + request.getOpaque() + ", channel=" + channel.remoteAddress());
        int requestCode = request.getCode();

        if (requestCode == RequestCode.GET_ROUTEINFO_BY_TOPIC) {
            return getRouteInfoByTopic(request);
        } else if (requestCode == RequestCode.REGISTER_BROKER) {
            return registerBroker(request);
        } else if (requestCode == RequestCode.GET_BROKER_CLUSTER_INFO) {
            return forwardToNameServer(request);
        } else if (requestCode == RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER) {
            return forwardToNameServer(request);
        } else {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, "unsupported request code");
        }
    }

    private RemotingCommand getRouteInfoByTopic(RemotingCommand request) {
        GetRouteInfoRequestHeader requestHeader = parseGetRouteInfoRequestHeader(request);
        System.out.println("[DEBUG] getRouteInfoByTopic: topic=" + requestHeader.getTopic());
        TopicRouteInfo routeInfo = virtualRouteManager.getRouteInfoByTopic(requestHeader.getTopic());

        if (routeInfo != null) {
            System.out.println("[DEBUG] RouteInfo brokerDatas: " + routeInfo.getBrokerDatas());
            byte[] body = RouteInfoSerializer.encodeTopicRouteInfo(routeInfo);
            System.out.println("[DEBUG] RouteInfo body: " + new String(body, java.nio.charset.StandardCharsets.UTF_8));
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
            response.setBody(body);
            return response;
        } else {
            return RemotingCommand.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, "no route info for this topic");
        }
    }

    private RemotingCommand registerBroker(RemotingCommand request) {
        RegisterBrokerRequestHeader requestHeader = parseRegisterBrokerRequestHeader(request);

        if (isProxySelf(requestHeader)) {
            virtualRouteManager.registerProxyToNameServer();
        }

        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
    }

    private RemotingCommand forwardToNameServer(RemotingCommand request) {
        NettyRemotingClient namesrvClient = virtualRouteManager.getNamesrvClient();
        String namesrvAddr = virtualRouteManager.getNamesrvAddr();
        if (namesrvClient != null && namesrvAddr != null) {
            try {
                return namesrvClient.invokeSync(namesrvAddr, request, 3000);
            } catch (Exception e) {
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, e.getMessage());
            }
        }
        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "namesrv not available");
    }

    private boolean isProxySelf(RegisterBrokerRequestHeader header) {
        String proxyBrokerAddr = virtualRouteManager.getProxyBrokerAddr();
        return proxyBrokerAddr != null && proxyBrokerAddr.equals(header.getBrokerAddr());
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

    private RegisterBrokerRequestHeader parseRegisterBrokerRequestHeader(RemotingCommand request) {
        RegisterBrokerRequestHeader header = (RegisterBrokerRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new RegisterBrokerRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setBrokerName(extFields.get("brokerName"));
            header.setBrokerAddr(extFields.get("brokerAddr"));
            header.setClusterName(extFields.get("clusterName"));
            header.setHaServerAddr(extFields.get("haServerAddr"));
            if (extFields.get("brokerId") != null) {
                header.setBrokerId(Long.parseLong(extFields.get("brokerId")));
            }
        }
        return header;
    }
}
