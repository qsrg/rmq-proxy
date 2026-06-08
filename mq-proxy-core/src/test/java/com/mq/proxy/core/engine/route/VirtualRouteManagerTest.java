package com.mq.proxy.core.engine.route;

import com.mq.proxy.core.storage.model.TopicRouteInfo;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class VirtualRouteManagerTest {

    private VirtualRouteManager routeManager;

    @Before
    public void setUp() {
        routeManager = new VirtualRouteManager();
    }

    private void setProxyAddr(String addr) throws Exception {
        Field field = VirtualRouteManager.class.getDeclaredField("proxyAddr");
        field.setAccessible(true);
        field.set(routeManager, addr);
    }

    @Test
    public void testConvertToVirtualRoute() throws Exception {
        TopicRouteInfo realRoute = new TopicRouteInfo();
        realRoute.setOrderTopicConf(null);

        List<TopicRouteInfo.QueueData> queueDatas = new ArrayList<>();
        TopicRouteInfo.QueueData queueData = new TopicRouteInfo.QueueData();
        queueData.setBrokerName("broker-a");
        queueData.setReadQueueNums(4);
        queueData.setWriteQueueNums(4);
        queueData.setPerm(6);
        queueDatas.add(queueData);
        realRoute.setQueueDatas(queueDatas);

        List<TopicRouteInfo.BrokerData> brokerDatas = new ArrayList<>();
        TopicRouteInfo.BrokerData brokerData = new TopicRouteInfo.BrokerData();
        brokerData.setBrokerName("broker-a");
        Map<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "192.168.1.1:10911");
        brokerAddrs.put(1L, "192.168.1.2:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);

        TopicRouteInfo.BrokerData brokerData2 = new TopicRouteInfo.BrokerData();
        brokerData2.setBrokerName("broker-b");
        Map<Long, String> brokerAddrs2 = new HashMap<>();
        brokerAddrs2.put(0L, "192.168.1.3:10911");
        brokerData2.setBrokerAddrs(brokerAddrs2);
        brokerDatas.add(brokerData2);

        realRoute.setBrokerDatas(brokerDatas);

        setProxyAddr("10.0.0.1:8080");

        TopicRouteInfo virtualRoute = routeManager.convertToVirtualRoute(realRoute, "TestTopic");

        assertEquals("TestTopic", virtualRoute.getTopic());
        assertEquals(1, virtualRoute.getQueueDatas().size());
        assertEquals("broker-a", virtualRoute.getQueueDatas().get(0).getBrokerName());

        assertEquals(2, virtualRoute.getBrokerDatas().size());

        for (TopicRouteInfo.BrokerData bd : virtualRoute.getBrokerDatas()) {
            for (Map.Entry<Long, String> entry : bd.getBrokerAddrs().entrySet()) {
                assertEquals("10.0.0.1:8080", entry.getValue());
            }
        }

        TopicRouteInfo.BrokerData virtualBrokerA = virtualRoute.getBrokerDatas().get(0);
        assertEquals("broker-a", virtualBrokerA.getBrokerName());
        assertEquals(2, virtualBrokerA.getBrokerAddrs().size());
        assertEquals("10.0.0.1:8080", virtualBrokerA.getBrokerAddrs().get(0L));
        assertEquals("10.0.0.1:8080", virtualBrokerA.getBrokerAddrs().get(1L));

        TopicRouteInfo.BrokerData virtualBrokerB = virtualRoute.getBrokerDatas().get(1);
        assertEquals("broker-b", virtualBrokerB.getBrokerName());
        assertEquals("10.0.0.1:8080", virtualBrokerB.getBrokerAddrs().get(0L));
    }

    @Test
    public void testRouteCacheMechanism() throws Exception {
        Field routeCacheField = VirtualRouteManager.class.getDeclaredField("routeCache");
        routeCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, TopicRouteInfo> routeCache = (Map<String, TopicRouteInfo>) routeCacheField.get(routeManager);

        Field routeCacheTimestampField = VirtualRouteManager.class.getDeclaredField("routeCacheTimestamp");
        routeCacheTimestampField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Long> routeCacheTimestamp = (Map<String, Long>) routeCacheTimestampField.get(routeManager);

        TopicRouteInfo cachedRoute = new TopicRouteInfo();
        cachedRoute.setTopic("CachedTopic");
        List<TopicRouteInfo.BrokerData> brokerDatas = new ArrayList<>();
        TopicRouteInfo.BrokerData brokerData = new TopicRouteInfo.BrokerData();
        brokerData.setBrokerName("broker-a");
        Map<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "10.0.0.1:8080");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        cachedRoute.setBrokerDatas(brokerDatas);

        routeCache.put("CachedTopic", cachedRoute);
        routeCacheTimestamp.put("CachedTopic", System.currentTimeMillis());

        TopicRouteInfo result = routeManager.getRouteInfoByTopic("CachedTopic");
        assertNotNull(result);
        assertEquals("CachedTopic", result.getTopic());

        routeCacheTimestamp.remove("CachedTopic");
        TopicRouteInfo expiredResult = routeManager.getRouteInfoByTopic("CachedTopic");
        assertNull(expiredResult);

        routeCache.put("ExpiredTopic", cachedRoute);
        routeCacheTimestamp.put("ExpiredTopic", System.currentTimeMillis() - 60000);
        TopicRouteInfo expiredCacheResult = routeManager.getRouteInfoByTopic("ExpiredTopic");
        assertNull(expiredCacheResult);

        routeCache.put("RefreshTopic", cachedRoute);
        routeCacheTimestamp.put("RefreshTopic", System.currentTimeMillis());
        routeManager.refreshRouteCache();
        assertFalse(routeCacheTimestamp.containsKey("RefreshTopic"));
    }

    @Test
    public void testGetProxyAddr() throws Exception {
        assertNull(routeManager.getProxyAddr());

        setProxyAddr("10.0.0.1:8080");
        assertEquals("10.0.0.1:8080", routeManager.getProxyAddr());

        setProxyAddr("192.168.1.100:9090");
        assertEquals("192.168.1.100:9090", routeManager.getProxyAddr());
    }

    @Test
    public void testDiscoverBrokersFallsBackToNextNameServer() {
        RecordingNettyRemotingClient namesrvClient = new RecordingNettyRemotingClient();
        namesrvClient.willThrow("171.31.208.1:9876", new RuntimeException("connect failed"));
        namesrvClient.willReturn("171.31.208.2:9876",
                createSuccessResponse(("{\"brokerAddrTable\":{\"broker-a\":{\"brokerAddrs\":{\"0\":\"192.168.1.1:10911\"}}}}")
                        .getBytes(StandardCharsets.UTF_8)));

        VirtualRouteManager manager = new VirtualRouteManager(namesrvClient);
        manager.start("171.31.208.1:9876;171.31.208.2:9876", "127.0.0.1", 10911);

        manager.discoverBrokers();

        assertEquals("192.168.1.1:10911", manager.getRealBrokerAddr("broker-a"));
        assertEquals(
                Arrays.asList("171.31.208.1:9876", "171.31.208.2:9876"),
                namesrvClient.getInvokedAddrs()
        );
    }

    @Test
    public void testGetRouteInfoByTopicFallsBackToNextNameServer() {
        RecordingNettyRemotingClient namesrvClient = new RecordingNettyRemotingClient();
        namesrvClient.willThrow("171.31.208.1:9876", new RuntimeException("connect failed"));

        TopicRouteInfo realRoute = new TopicRouteInfo();
        realRoute.setTopic("TestTopic");
        TopicRouteInfo.QueueData queueData = new TopicRouteInfo.QueueData();
        queueData.setBrokerName("broker-a");
        queueData.setReadQueueNums(4);
        queueData.setWriteQueueNums(4);
        queueData.setPerm(6);
        realRoute.setQueueDatas(Collections.singletonList(queueData));

        TopicRouteInfo.BrokerData brokerData = new TopicRouteInfo.BrokerData();
        brokerData.setBrokerName("broker-a");
        Map<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "192.168.1.1:10911");
        brokerData.setBrokerAddrs(brokerAddrs);
        realRoute.setBrokerDatas(Collections.singletonList(brokerData));

        namesrvClient.willReturn("171.31.208.2:9876",
                createSuccessResponse(RouteInfoSerializer.encodeTopicRouteInfo(realRoute)));

        VirtualRouteManager manager = new VirtualRouteManager(namesrvClient);
        manager.start("171.31.208.1:9876;171.31.208.2:9876", "10.0.0.1", 8080);

        TopicRouteInfo virtualRoute = manager.getRouteInfoByTopic("TestTopic");

        assertNotNull(virtualRoute);
        assertEquals("10.0.0.1:8080", virtualRoute.getBrokerDatas().get(0).getBrokerAddrs().get(0L));
        assertEquals("192.168.1.1:10911", manager.getRealBrokerAddr("broker-a"));
        assertEquals(
                Arrays.asList("171.31.208.1:9876", "171.31.208.2:9876"),
                namesrvClient.getInvokedAddrs()
        );
    }

    @Test
    public void testRetryTopicRouteStaysStableWithinCacheWindow() {
        RecordingNettyRemotingClient namesrvClient = new RecordingNettyRemotingClient();
        namesrvClient.willReturn("171.31.208.1:9876",
                createSuccessResponse(RouteInfoSerializer.encodeTopicRouteInfo(createRoute("%RETRY%GroupA", "broker-b", "192.168.1.2:10911"))));
        namesrvClient.willReturn("171.31.208.1:9876",
                createSuccessResponse(RouteInfoSerializer.encodeTopicRouteInfo(createRoute("%RETRY%GroupA", "broker-a", "192.168.1.1:10911"))));

        VirtualRouteManager manager = new VirtualRouteManager(namesrvClient);
        manager.start("171.31.208.1:9876", "10.0.0.1", 8080);

        TopicRouteInfo first = manager.getRouteInfoByTopic("%RETRY%GroupA");
        TopicRouteInfo second = manager.getRouteInfoByTopic("%RETRY%GroupA");

        assertNotNull(first);
        assertNotNull(second);
        assertEquals("broker-b", first.getQueueDatas().get(0).getBrokerName());
        assertEquals("broker-b", second.getQueueDatas().get(0).getBrokerName());
        assertEquals(Collections.singletonList("171.31.208.1:9876"), namesrvClient.getInvokedAddrs());
    }

    private static TopicRouteInfo createRoute(String topic, String brokerName, String brokerAddr) {
        TopicRouteInfo routeInfo = new TopicRouteInfo();
        routeInfo.setTopic(topic);

        TopicRouteInfo.QueueData queueData = new TopicRouteInfo.QueueData();
        queueData.setBrokerName(brokerName);
        queueData.setReadQueueNums(1);
        queueData.setWriteQueueNums(1);
        queueData.setPerm(6);
        routeInfo.setQueueDatas(Collections.singletonList(queueData));

        TopicRouteInfo.BrokerData brokerData = new TopicRouteInfo.BrokerData();
        brokerData.setBrokerName(brokerName);
        Map<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, brokerAddr);
        brokerData.setBrokerAddrs(brokerAddrs);
        routeInfo.setBrokerDatas(Collections.singletonList(brokerData));
        routeInfo.setFilterServerTable(new HashMap<String, List<String>>());
        return routeInfo;
    }

    private static RemotingCommand createSuccessResponse(byte[] body) {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setBody(body);
        return response;
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
