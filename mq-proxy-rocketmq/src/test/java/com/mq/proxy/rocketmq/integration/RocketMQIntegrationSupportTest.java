package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RocketMQIntegrationSupportTest {

    @Test
    public void testDiscoverBrokerAddrTriesNextNamesrvWhenFirstFails() throws Exception {
        RecordingClient client = new RecordingClient();
        client.willThrow("127.0.0.1:9876", new RuntimeException("connect failed"));
        client.willReturn("127.0.0.1:9877", routeResponse("127.0.0.1:10911"));

        String brokerAddr = RocketMQIntegrationSupport.discoverBrokerAddr(client, "127.0.0.1:9876;127.0.0.1:9877");

        assertEquals("127.0.0.1:10911", brokerAddr);
        assertEquals(Arrays.asList("127.0.0.1:9876", "127.0.0.1:9876", "127.0.0.1:9877"), client.invokedAddrs);
    }

    private static RemotingCommand routeResponse(String brokerAddr) {
        TopicRouteInfo routeInfo = new TopicRouteInfo();
        TopicRouteInfo.BrokerData brokerData = new TopicRouteInfo.BrokerData();
        brokerData.setBrokerName("broker-a");
        Map<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, brokerAddr);
        brokerData.setBrokerAddrs(brokerAddrs);
        routeInfo.setBrokerDatas(Arrays.asList(brokerData));

        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, null);
        response.setBody(RouteInfoSerializer.encodeTopicRouteInfo(routeInfo));
        return response;
    }

    private static class RecordingClient extends NettyRemotingClient {
        private final java.util.List<String> invokedAddrs = new java.util.ArrayList<>();
        private final Map<String, RemotingCommand> responses = new HashMap<>();
        private final Map<String, RuntimeException> errors = new HashMap<>();

        RecordingClient() {
            super(new NettyClientConfig());
        }

        void willReturn(String addr, RemotingCommand response) {
            responses.put(addr, response);
        }

        void willThrow(String addr, RuntimeException error) {
            errors.put(addr, error);
        }

        @Override
        public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
            invokedAddrs.add(addr);
            RuntimeException error = errors.get(addr);
            if (error != null) {
                throw error;
            }
            return responses.get(addr);
        }
    }
}
