package com.mq.proxy.sdk.consumer.rebalance;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import com.mq.proxy.sdk.consumer.ProxyConsumer;
import com.mq.proxy.sdk.consumer.model.MessageQueue;
import com.mq.proxy.sdk.consumer.push.ProxyPushConsumer;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class RebalanceLogicTest {

    private ProxyClientFacade mockFacade;
    private ProxyConsumer mockPullConsumer;
    private ProxyPushConsumer consumer;

    @Before
    public void setUp() throws Exception {
        mockFacade = mock(ProxyClientFacade.class);
        when(mockFacade.getRemotingClient()).thenReturn(mock(com.mq.proxy.sdk.remoting.ProxyRemotingClient.class));

        mockPullConsumer = mock(ProxyConsumer.class);
        when(mockPullConsumer.queryConsumerOffset(anyString(), anyString(), anyInt())).thenReturn(-1L);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("RebalanceTestGroup"));

        consumer = new ProxyPushConsumer("RebalanceTestGroup");
        consumer.setProxyAddrs("127.0.0.1:19876");
        consumer.subscribe("RebalanceTopic", "*");

        injectMockPullConsumer(consumer, mockPullConsumer);
        injectMockPullExecutor(consumer, mock(ScheduledExecutorService.class));
        setClientId(consumer, "PushSDK@RebalanceTestGroup@1");
        setStarted(consumer, true);
    }

    @Test
    public void testRouteInfoJsonParsing() {
        String topic = "TestTopic";
        String brokerName = "broker-a";
        int queueNum = 4;

        String json = buildRouteInfoBody(topic, brokerName, queueNum);
        TopicRouteInfo info = RouteInfoSerializer.decodeTopicRouteInfo(json.getBytes());

        assertNotNull(info);
        assertEquals(topic, info.getTopic());
        assertEquals(1, info.getQueueDatas().size());
        assertEquals(brokerName, info.getQueueDatas().get(0).getBrokerName());
        assertEquals(queueNum, info.getQueueDatas().get(0).getReadQueueNums());
        assertEquals(6, info.getQueueDatas().get(0).getPerm());
    }

    @Test
    public void testQueueAssignmentSingleConsumerGetsAllQueues() throws Exception {
        String topic = "RebalanceTopic";
        int queueCount = 4;

        String routeBody = buildRouteInfoBody(topic, "broker-a", queueCount);
        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\"]}";

        mockRebalanceResponses(routeBody, consumerListBody);

        invokeRebalance();

        Set<MessageQueue> assigned = getAssignedQueues();
        assertEquals(queueCount, assigned.size());
        for (int i = 0; i < queueCount; i++) {
            assertTrue("Should contain queue " + i,
                assigned.contains(new MessageQueue(topic, "broker-a", i)));
        }
    }

    @Test
    public void testQueueAssignmentTwoConsumersSplitsQueuesEvenly() throws Exception {
        String topic = "RebalanceTopic";
        int queueCount = 4;

        String routeBody = buildRouteInfoBody(topic, "broker-a", queueCount);
        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\",\"PushSDK@RebalanceTestGroup@2\"]}";

        mockRebalanceResponses(routeBody, consumerListBody);

        setClientId("PushSDK@RebalanceTestGroup@1");

        invokeRebalance();

        Set<MessageQueue> assigned = getAssignedQueues();
        assertEquals(2, assigned.size());

        for (MessageQueue mq : assigned) {
            assertTrue("First consumer should get even queues (0, 2)",
                mq.getQueueId() == 0 || mq.getQueueId() == 2);
        }
    }

    @Test
    public void testQueueAssignmentSecondConsumerGetsRemainingQueues() throws Exception {
        String topic = "RebalanceTopic";
        int queueCount = 4;

        String routeBody = buildRouteInfoBody(topic, "broker-a", queueCount);
        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\",\"PushSDK@RebalanceTestGroup@2\"]}";

        mockRebalanceResponses(routeBody, consumerListBody);

        setClientId("PushSDK@RebalanceTestGroup@2");

        invokeRebalance();

        Set<MessageQueue> assigned = getAssignedQueues();
        assertEquals(2, assigned.size());

        for (MessageQueue mq : assigned) {
            assertTrue("Second consumer should get odd queues (1, 3)",
                mq.getQueueId() == 1 || mq.getQueueId() == 3);
        }
    }

    @Test
    public void testRebalanceRemovesExtraQueuesWhenConsumerCountIncreases() throws Exception {
        String topic = "RebalanceTopic";
        int queueCount = 6;

        String routeBody = buildRouteInfoBody(topic, "broker-a", queueCount);
        String consumerList1 = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\",\"PushSDK@RebalanceTestGroup@2\"]}";
        String consumerList2 = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\",\"PushSDK@RebalanceTestGroup@2\",\"PushSDK@RebalanceTestGroup@3\"]}";

        mockRebalanceResponses(routeBody, consumerList1);
        invokeRebalance();
        int firstSize = getAssignedQueues().size();
        assertEquals("With 2 consumers and 6 queues, should get 3", 3, firstSize);

        mockRebalanceResponses(routeBody, consumerList2);
        invokeRebalance();
        int secondSize = getAssignedQueues().size();
        assertEquals("With 3 consumers and 6 queues, should get 2", 2, secondSize);

        assertTrue("Queue count should decrease when more consumers join", secondSize < firstSize);
    }

    @Test
    public void testOnlyWritableQueuesAreIncludedInAssignment() throws Exception {
        String topic = "RebalanceTopic";
        String routeBody = "{\"topic\":\"" + topic
            + "\",\"queueDatas\":["
            + "{\"brokerName\":\"broker-a\",\"readQueueNums\":4,\"writeQueueNums\":4,\"perm\":6,\"topicSysFlag\":0},"
            + "{\"brokerName\":\"broker-b\",\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":4,\"topicSysFlag\":0}"
            + "],\"brokerDatas\":["
            + "{\"cluster\":\"DefaultCluster\",\"brokerName\":\"broker-a\",\"brokerAddrs\":{\"0\":\"127.0.0.1:19876\"}},"
            + "{\"cluster\":\"DefaultCluster\",\"brokerName\":\"broker-b\",\"brokerAddrs\":{\"0\":\"127.0.0.1:19877\"}}"
            + "]}";

        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\"]}";
        mockRebalanceResponses(routeBody, consumerListBody);

        invokeRebalance();

        Set<MessageQueue> assigned = getAssignedQueues();
        assertEquals(4, assigned.size());

        for (MessageQueue mq : assigned) {
            assertEquals("Only broker-a queues (perm=6) should be assigned",
                "broker-a", mq.getBrokerName());
        }
    }

    @Test
    public void testRebalanceWithMultipleBrokers() throws Exception {
        String topic = "RebalanceTopic";
        String routeBody = "{\"topic\":\"" + topic
            + "\",\"queueDatas\":["
            + "{\"brokerName\":\"broker-a\",\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":0},"
            + "{\"brokerName\":\"broker-b\",\"readQueueNums\":2,\"writeQueueNums\":2,\"perm\":6,\"topicSysFlag\":0}"
            + "],\"brokerDatas\":["
            + "{\"cluster\":\"DefaultCluster\",\"brokerName\":\"broker-a\",\"brokerAddrs\":{\"0\":\"127.0.0.1:19876\"}},"
            + "{\"cluster\":\"DefaultCluster\",\"brokerName\":\"broker-b\",\"brokerAddrs\":{\"0\":\"127.0.0.1:19877\"}}"
            + "]}";

        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\"]}";
        mockRebalanceResponses(routeBody, consumerListBody);

        invokeRebalance();

        Set<MessageQueue> assigned = getAssignedQueues();
        assertEquals(4, assigned.size());

        int brokerA = 0;
        int brokerB = 0;
        for (MessageQueue mq : assigned) {
            if ("broker-a".equals(mq.getBrokerName())) brokerA++;
            if ("broker-b".equals(mq.getBrokerName())) brokerB++;
        }
        assertEquals(2, brokerA);
        assertEquals(2, brokerB);
    }

    @Test
    public void testUnevenDistributionWithSevenQueuesThreeConsumers() throws Exception {
        String topic = "RebalanceTopic";
        int queueCount = 7;

        String routeBody = buildRouteInfoBody(topic, "broker-a", queueCount);
        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\",\"PushSDK@RebalanceTestGroup@2\",\"PushSDK@RebalanceTestGroup@3\"]}";

        mockRebalanceResponses(routeBody, consumerListBody);
        setClientId("PushSDK@RebalanceTestGroup@1");
        invokeRebalance();
        int consumer1Queues = getAssignedQueues().size();
        assertEquals("Consumer 1 should get 3 queues (0,3,6)", 3, consumer1Queues);

        mockRebalanceResponses(routeBody, consumerListBody);
        setClientId("PushSDK@RebalanceTestGroup@2");
        invokeRebalance();
        int consumer2Queues = getAssignedQueues().size();
        assertEquals("Consumer 2 should get 2 queues (1,4)", 2, consumer2Queues);

        mockRebalanceResponses(routeBody, consumerListBody);
        setClientId("PushSDK@RebalanceTestGroup@3");
        invokeRebalance();
        int consumer3Queues = getAssignedQueues().size();
        assertEquals("Consumer 3 should get 2 queues (2,5)", 2, consumer3Queues);
    }

    @Test
    public void testMoreConsumersThanQueues() throws Exception {
        String topic = "RebalanceTopic";
        int queueCount = 2;

        String routeBody = buildRouteInfoBody(topic, "broker-a", queueCount);
        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\",\"PushSDK@RebalanceTestGroup@2\",\"PushSDK@RebalanceTestGroup@3\"]}";

        mockRebalanceResponses(routeBody, consumerListBody);
        setClientId("PushSDK@RebalanceTestGroup@1");
        invokeRebalance();
        assertEquals("Consumer 1 should get queue 0", 1, getAssignedQueues().size());

        mockRebalanceResponses(routeBody, consumerListBody);
        setClientId("PushSDK@RebalanceTestGroup@2");
        invokeRebalance();
        assertEquals("Consumer 2 should get queue 1", 1, getAssignedQueues().size());

        mockRebalanceResponses(routeBody, consumerListBody);
        setClientId("PushSDK@RebalanceTestGroup@3");
        invokeRebalance();
        assertEquals("Consumer 3 should get no queues", 0, getAssignedQueues().size());
    }

    @Test
    public void testRebalancePreservesExistingQueuesWhenNoChange() throws Exception {
        String topic = "RebalanceTopic";
        int queueCount = 4;

        String routeBody = buildRouteInfoBody(topic, "broker-a", queueCount);
        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\"]}";

        mockRebalanceResponses(routeBody, consumerListBody);
        invokeRebalance();

        Set<MessageQueue> firstAssign = getAssignedQueues();
        assertEquals(4, firstAssign.size());

        invokeRebalance();
        Set<MessageQueue> secondAssign = getAssignedQueues();
        assertEquals("Queues should be preserved", firstAssign, secondAssign);
    }

    @Test
    public void testRebalanceHandlesEmptyRouteResponse() throws Exception {
        String routeBody = "{\"topic\":\"RebalanceTopic\",\"queueDatas\":[],\"brokerDatas\":[]}";
        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@RebalanceTestGroup@1\"]}";

        mockRebalanceResponses(routeBody, consumerListBody);

        invokeRebalance();
        assertEquals("Should have no queues when route is empty", 0, getAssignedQueues().size());
    }

    private void mockRebalanceResponses(String routeBody, String consumerListBody) throws Exception {
        reset(mockPullConsumer);
        when(mockPullConsumer.getFacade()).thenReturn(mockFacade);
        when(mockFacade.getRemotingClient()).thenReturn(mock(com.mq.proxy.sdk.remoting.ProxyRemotingClient.class));

        RemotingCommand routeResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        routeResponse.setBody(routeBody.getBytes());
        RemotingCommand consumerListResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        consumerListResponse.setBody(consumerListBody.getBytes());

        doReturn(routeResponse, consumerListResponse).when(mockFacade).invokeSync(any(RemotingCommand.class), anyLong());
    }

    @SuppressWarnings("unchecked")
    private void invokeRebalance() throws Exception {
        java.lang.reflect.Method method = ProxyPushConsumer.class.getDeclaredMethod("doRebalance");
        method.setAccessible(true);
        method.invoke(consumer);
    }

    @SuppressWarnings("unchecked")
    private Set<MessageQueue> getAssignedQueues() throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("assignedQueues");
        field.setAccessible(true);
        return new HashSet<>((Set<MessageQueue>) field.get(consumer));
    }

    private void setClientId(ProxyPushConsumer c, String clientId) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("clientId");
        field.setAccessible(true);
        field.set(c, clientId);
    }

    private void setClientId(String clientId) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("clientId");
        field.setAccessible(true);
        field.set(consumer, clientId);
    }

    private void injectMockPullConsumer(ProxyPushConsumer consumer, ProxyConsumer pullConsumer) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("pullConsumer");
        field.setAccessible(true);
        field.set(consumer, pullConsumer);
    }

    private void injectMockPullExecutor(ProxyPushConsumer consumer, ScheduledExecutorService executor) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("pullExecutor");
        field.setAccessible(true);
        field.set(consumer, executor);
    }

    private void setStarted(ProxyPushConsumer consumer, boolean started) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("started");
        field.setAccessible(true);
        field.set(consumer, new java.util.concurrent.atomic.AtomicBoolean(started));
    }

    private String buildRouteInfoBody(String topic, String brokerName, int queueNum) {
        return "{\"topic\":\"" + topic + "\",\"queueDatas\":[{\"brokerName\":\"" + brokerName
            + "\",\"readQueueNums\":" + queueNum + ",\"writeQueueNums\":" + queueNum
            + ",\"perm\":6,\"topicSysFlag\":0}],\"brokerDatas\":[{\"cluster\":\"DefaultCluster\",\"brokerName\":\""
            + brokerName + "\",\"brokerAddrs\":{\"0\":\"127.0.0.1:19876\"}}]}";
    }
}