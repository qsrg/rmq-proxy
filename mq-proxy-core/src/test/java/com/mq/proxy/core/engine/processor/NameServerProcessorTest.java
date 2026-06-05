package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import io.netty.channel.Channel;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NameServerProcessorTest {

    @Test
    public void testForwardToNameServerFallsBackToNextAddress() throws Exception {
        RecordingNettyRemotingClient namesrvClient = new RecordingNettyRemotingClient();
        RemotingCommand expected = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "OK");
        expected.setBody(new byte[]{1, 2, 3});

        namesrvClient.willThrow("171.31.208.1:9876", new RuntimeException("connect failed"));
        namesrvClient.willReturn("171.31.208.2:9876", expected);

        VirtualRouteManager routeManager = new VirtualRouteManager(namesrvClient);
        routeManager.start("171.31.208.1:9876;171.31.208.2:9876", "127.0.0.1", 10911);

        NameServerProcessor processor = new NameServerProcessor(routeManager);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        request.setExtFields(new HashMap<String, String>());

        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 12345));

        RemotingCommand response = processor.processRequest(channel, request);

        assertSame(expected, response);
        assertEquals(
                Arrays.asList("171.31.208.1:9876", "171.31.208.2:9876"),
                namesrvClient.getInvokedAddrs()
        );
    }

    @Test
    public void testMissingTopicFallbackRouteUsesAllBrokerQueues() throws Exception {
        VirtualRouteManager routeManager = mock(VirtualRouteManager.class);
        when(routeManager.getRouteInfoByTopic("MissingTopic")).thenReturn(null);
        when(routeManager.getRouteInfoByTopic("TBW102")).thenReturn(createDefaultTopicRoute());

        NameServerProcessor processor = new NameServerProcessor(routeManager);
        RemotingCommand request = RemotingCommand.createRequestCommand(
                RequestCode.GET_ROUTEINFO_BY_TOPIC,
                null
        );
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "MissingTopic");
        request.setExtFields(extFields);

        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 12345));

        RemotingCommand response = processor.processRequest(channel, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        TopicRouteInfo routeInfo = RouteInfoSerializer.decodeTopicRouteInfo(response.getBody());
        assertEquals("MissingTopic", routeInfo.getTopic());
        assertEquals(2, routeInfo.getQueueDatas().size());
        assertEquals("broker-a", routeInfo.getQueueDatas().get(0).getBrokerName());
        assertEquals(4, routeInfo.getQueueDatas().get(0).getReadQueueNums());
        assertEquals(4, routeInfo.getQueueDatas().get(0).getWriteQueueNums());
        assertEquals("broker-b", routeInfo.getQueueDatas().get(1).getBrokerName());
        assertEquals(4, routeInfo.getQueueDatas().get(1).getReadQueueNums());
        assertEquals(4, routeInfo.getQueueDatas().get(1).getWriteQueueNums());
        assertEquals(2, routeInfo.getBrokerDatas().size());
        assertEquals("broker-a", routeInfo.getBrokerDatas().get(0).getBrokerName());
        assertEquals("broker-b", routeInfo.getBrokerDatas().get(1).getBrokerName());
    }

    private static TopicRouteInfo createDefaultTopicRoute() {
        TopicRouteInfo routeInfo = new TopicRouteInfo();

        List<TopicRouteInfo.QueueData> queueDatas = new ArrayList<>();
        TopicRouteInfo.QueueData brokerAQueue = new TopicRouteInfo.QueueData();
        brokerAQueue.setBrokerName("broker-a");
        brokerAQueue.setReadQueueNums(8);
        brokerAQueue.setWriteQueueNums(8);
        brokerAQueue.setPerm(7);
        queueDatas.add(brokerAQueue);

        TopicRouteInfo.QueueData brokerBQueue = new TopicRouteInfo.QueueData();
        brokerBQueue.setBrokerName("broker-b");
        brokerBQueue.setReadQueueNums(8);
        brokerBQueue.setWriteQueueNums(8);
        brokerBQueue.setPerm(7);
        queueDatas.add(brokerBQueue);
        routeInfo.setQueueDatas(queueDatas);

        List<TopicRouteInfo.BrokerData> brokerDatas = new ArrayList<>();
        TopicRouteInfo.BrokerData brokerA = new TopicRouteInfo.BrokerData();
        brokerA.setBrokerName("broker-a");
        Map<Long, String> brokerAAddrs = new HashMap<>();
        brokerAAddrs.put(0L, "127.0.0.1:10914");
        brokerA.setBrokerAddrs(brokerAAddrs);
        brokerDatas.add(brokerA);

        TopicRouteInfo.BrokerData brokerB = new TopicRouteInfo.BrokerData();
        brokerB.setBrokerName("broker-b");
        Map<Long, String> brokerBAddrs = new HashMap<>();
        brokerBAddrs.put(0L, "127.0.0.1:20909");
        brokerB.setBrokerAddrs(brokerBAddrs);
        brokerDatas.add(brokerB);
        routeInfo.setBrokerDatas(brokerDatas);
        routeInfo.setFilterServerTable(new HashMap<String, List<String>>());

        return routeInfo;
    }

    private static class RecordingNettyRemotingClient extends NettyRemotingClient {
        private final List<String> invokedAddrs = new ArrayList<>();
        private final Map<String, Deque<Object>> scriptedResults = new HashMap<>();

        RecordingNettyRemotingClient() {
            super(new NettyClientConfig());
        }

        void willReturn(String addr, RemotingCommand response) {
            scriptedResults.computeIfAbsent(addr, key -> new ArrayDeque<>()).addLast(response);
        }

        void willThrow(String addr, RuntimeException exception) {
            scriptedResults.computeIfAbsent(addr, key -> new ArrayDeque<>()).addLast(exception);
        }

        List<String> getInvokedAddrs() {
            return invokedAddrs;
        }

        @Override
        public void start() {
        }

        @Override
        protected RemotingCommand invokeSyncSingle(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
            invokedAddrs.add(addr);
            Deque<Object> scripted = scriptedResults.get(addr);
            if (scripted == null || scripted.isEmpty()) {
                throw new AssertionError("No scripted result for addr " + addr + ", requestCode=" + request.getCode());
            }
            Object result = scripted.removeFirst();
            if (result instanceof Exception) {
                throw (Exception) result;
            }
            return (RemotingCommand) result;
        }
    }
}
