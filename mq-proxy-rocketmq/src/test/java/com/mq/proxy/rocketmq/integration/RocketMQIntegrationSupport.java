package com.mq.proxy.rocketmq.integration;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import com.mq.proxy.core.server.NettyRemotingClient;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

final class RocketMQIntegrationSupport {

    static final String DEFAULT_NAMESRV_ADDR = "127.0.0.1:9876;127.0.0.1:9877";

    private RocketMQIntegrationSupport() {
    }

    static String discoverBrokerAddr(NettyRemotingClient client, String namesrvAddr) throws Exception {
        for (String addr : namesrvAddr.split(";")) {
            String trimmedAddr = addr.trim();
            if (trimmedAddr.isEmpty()) {
                continue;
            }
            String brokerAddr = discoverBrokerAddrFromOneNamesrv(client, trimmedAddr);
            if (brokerAddr != null) {
                return brokerAddr;
            }
        }

        return null;
    }

    private static String discoverBrokerAddrFromOneNamesrv(NettyRemotingClient client, String namesrvAddr) throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TBW102");
        request.setExtFields(extFields);
        request.makeCustomHeaderToNet();

        try {
            RemotingCommand response = client.invokeSync(namesrvAddr, request, 5000);
            if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
                TopicRouteInfo routeInfo = RouteInfoSerializer.decodeTopicRouteInfo(response.getBody());
                if (routeInfo.getBrokerDatas() != null && !routeInfo.getBrokerDatas().isEmpty()) {
                    Map<Long, String> addrs = routeInfo.getBrokerDatas().get(0).getBrokerAddrs();
                    if (addrs != null && !addrs.isEmpty()) {
                        String masterAddr = addrs.get(0L);
                        if (masterAddr != null) {
                            return masterAddr;
                        }
                        return addrs.values().iterator().next();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to discover broker from TBW102: " + e.getMessage());
        }

        request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        try {
            RemotingCommand response = client.invokeSync(namesrvAddr, request, 5000);
            if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
                String json = new String(response.getBody(), "UTF-8");
                json = RouteInfoSerializer.fixNumericKeys(json);
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                Map<String, Object> clusterInfo = mapper.readValue(json, Map.class);
                Map<String, Object> brokerAddrTable = (Map<String, Object>) clusterInfo.get("brokerAddrTable");
                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Map<String, Object> firstBroker = (Map<String, Object>) brokerAddrTable.values().iterator().next();
                    Map<String, Object> brokerAddrs = (Map<String, Object>) firstBroker.get("brokerAddrs");
                    if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                        Object masterAddr = brokerAddrs.get("0");
                        if (masterAddr == null) {
                            masterAddr = brokerAddrs.values().iterator().next();
                        }
                        return masterAddr.toString();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to discover broker from cluster info: " + e.getMessage());
        }

        return null;
    }

    static int findAvailablePort() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        try {
            return ss.getLocalPort();
        } finally {
            ss.close();
        }
    }
}
