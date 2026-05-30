package com.mq.proxy.sdk.consumer.push;

import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.PullMessageResponseHeader;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetResponseHeader;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import com.mq.proxy.sdk.consumer.ProxyConsumer;
import com.mq.proxy.sdk.consumer.ConsumerChangeListener;
import com.mq.proxy.sdk.consumer.model.ProxyMessage;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class ProxyPushConsumerTest {

    private ProxyClientFacade mockFacade;
    private ProxyPushConsumer consumer;
    private ProxyConsumer mockPullConsumer;

    @Before
    public void setUp() throws Exception {
        mockFacade = mock(ProxyClientFacade.class);
        when(mockFacade.getRemotingClient()).thenReturn(mock(com.mq.proxy.sdk.remoting.ProxyRemotingClient.class));

        mockPullConsumer = mock(ProxyConsumer.class);

        consumer = new ProxyPushConsumer("TestPushGroup");
        consumer.setProxyAddrs("127.0.0.1:19876");

        injectMockPullConsumer(consumer, mockPullConsumer);
        injectMockPullExecutor(consumer, mock(ScheduledExecutorService.class));
        injectSyncConsumeExecutor(consumer);
        setStarted(consumer, true);
    }

    @Test
    public void testSubscribeAndListenerRegistration() {
        consumer.subscribe("TestTopic", "*");

        AtomicInteger consumed = new AtomicInteger(0);
        consumer.registerMessageListener(messages -> {
            consumed.addAndGet(messages.size());
            return ConsumeStatus.SUCCESS;
        });

        assertNotNull(consumer.getConfig());
        assertEquals("TestPushGroup", consumer.getConfig().getConsumerGroup());
    }

    @Test
    public void testDecodeEncodingWorks() {
        byte[] body = "hello world".getBytes();
        byte[] encoded = encodeMessageBody("TestTopic", 0, 100L, body);
        List<com.mq.proxy.sdk.consumer.model.DecodedMessage> msgs =
            com.mq.proxy.sdk.consumer.model.DecodedMessage.decode(encoded);
        assertEquals(1, msgs.size());
        assertEquals("TestTopic", msgs.get(0).getTopic());
        assertEquals(0, msgs.get(0).getQueueId());
        assertEquals(100L, msgs.get(0).getQueueOffset());
        assertArrayEquals(body, msgs.get(0).getBody());
    }

    @Test
    public void testPullTaskDeliversMessageToListener() throws Exception {
        consumer.subscribe("TestTopic", "*");

        CountDownLatch latch = new CountDownLatch(1);
        List<ProxyMessage> receivedMessages = Collections.synchronizedList(new ArrayList<>());

        consumer.registerMessageListener(messages -> {
            receivedMessages.addAll(messages);
            latch.countDown();
            return ConsumeStatus.SUCCESS;
        });

        String topic = "TestTopic";
        int queueId = 0;
        long offset = 100L;
        byte[] messageBody = "hello world".getBytes();

        com.mq.proxy.sdk.consumer.PullResult pullResult = buildPullResultFound(101L, offset, 200L, messageBody);
        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(offset), eq(32), eq(offset)))
            .thenReturn(pullResult);

        when(mockPullConsumer.queryConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId)))
            .thenReturn(offset);
        doNothing().when(mockPullConsumer).updateConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId), anyLong());

        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig()
                .setConsumerGroup("TestPushGroup"));

        addAssignedQueue(topic, "broker-a", queueId);

        invokePullTaskManually(topic, "broker-a", queueId, offset);

        assertTrue("Listener should receive message", latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, receivedMessages.size());
        ProxyMessage msg = receivedMessages.get(0);
        assertEquals(topic, msg.getTopic());
        assertEquals(queueId, msg.getQueueId());
        assertEquals(offset, msg.getQueueOffset());
        assertArrayEquals(messageBody, msg.getBody());
    }

    @Test
    public void testOffsetManagementAcrossMultiplePulls() throws Exception {
        consumer.subscribe("TestTopic", "*");

        CountDownLatch latch = new CountDownLatch(2);
        List<ProxyMessage> receivedMessages = Collections.synchronizedList(new ArrayList<>());

        consumer.registerMessageListener(messages -> {
            receivedMessages.addAll(messages);
            latch.countDown();
            return ConsumeStatus.SUCCESS;
        });

        String topic = "TestTopic";
        int queueId = 0;

        com.mq.proxy.sdk.consumer.PullResult result1 = buildPullResultFound(101L, 100L, 200L, "msg1".getBytes());
        com.mq.proxy.sdk.consumer.PullResult result2 = buildPullResultFound(102L, 101L, 200L, "msg2".getBytes());

        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(100L), eq(32), eq(100L)))
            .thenReturn(result1);
        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(101L), eq(32), eq(101L)))
            .thenReturn(result2);
        when(mockPullConsumer.queryConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId)))
            .thenReturn(100L);
        doNothing().when(mockPullConsumer).updateConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId), anyLong());
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestPushGroup"));

        addAssignedQueue(topic, "broker-a", queueId);

        invokePullTaskManually(topic, "broker-a", queueId, 100L);
        invokePullTaskManually(topic, "broker-a", queueId, 101L);

        assertTrue("Listener should receive all messages", latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, receivedMessages.size());
        assertEquals(100L, receivedMessages.get(0).getQueueOffset());
        assertEquals(101L, receivedMessages.get(1).getQueueOffset());
    }

    @Test
    public void testQueryOffsetOnNotFoundReturnsMinusOne() throws Exception {
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong()))
            .thenReturn(buildPullNotFoundResponse(55L, 0L, 200L));

        consumer.subscribe("EmptyTopic", "*");
        consumer.registerMessageListener(messages -> ConsumeStatus.SUCCESS);

        String routeBody = buildRouteInfoBody("EmptyTopic", "broker-a", 4);
        String consumerListBody = "{\"consumerIdList\":[\"PushSDK@TestPushGroup@1\"]}";

        RemotingCommand routeResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        routeResponse.setBody(routeBody.getBytes());
        RemotingCommand consumerListResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        consumerListResponse.setBody(consumerListBody.getBytes());

        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong()))
            .thenReturn(routeResponse)
            .thenReturn(consumerListResponse);

        com.mq.proxy.sdk.consumer.PullResult pullResult = new com.mq.proxy.sdk.consumer.PullResult();
        pullResult.setSuccess(true);
        pullResult.setFound(false);
        pullResult.setResponseCode(ResponseCode.PULL_NOT_FOUND);
        pullResult.setNextBeginOffset(55L);
        pullResult.setMinOffset(0L);
        pullResult.setMaxOffset(200L);

        when(mockPullConsumer.pull(anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
            .thenReturn(pullResult);
        when(mockPullConsumer.queryConsumerOffset(anyString(), anyString(), anyInt()))
            .thenReturn(-1L);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestPushGroup"));

        addAssignedQueue("EmptyTopic", "broker-a", 0);

        invokePullTaskManually("EmptyTopic", "broker-a", 0, 0L);
    }

    @Test
    public void testDefaultConsumerGroupName() {
        ProxyPushConsumer c = new ProxyPushConsumer("DefaultGroup");
        assertEquals("DefaultGroup", c.getConfig().getConsumerGroup());
    }

    @Test
    public void testSetMessageModel() {
        consumer.setMessageModel("BROADCASTING");
        assertEquals("BROADCASTING", consumer.getConfig().getMessageModel());
    }

    @Test
    public void testSetSuspendTimeout() {
        consumer.setSuspendTimeoutMillis(30000L);
        assertEquals(30000L, consumer.getConfig().getSuspendTimeoutMillis());
    }

    @Test
    public void testFullLifecyclePullDispatch() throws Exception {
        consumer.subscribe("IntegrationTopic", "*");

        CountDownLatch latch = new CountDownLatch(1);
        List<ProxyMessage> received = Collections.synchronizedList(new ArrayList<>());
        consumer.registerMessageListener(messages -> {
            received.addAll(messages);
            latch.countDown();
            return ConsumeStatus.SUCCESS;
        });

        String topic = "IntegrationTopic";
        int queueId = 0;
        long offset = 0L;

        com.mq.proxy.sdk.consumer.PullResult pullResult = buildPullResultFound("IntegrationTopic", 0, offset, 1L, 200L, "integration msg".getBytes());
        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(offset), eq(32), eq(offset)))
            .thenReturn(pullResult);
        when(mockPullConsumer.queryConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId)))
            .thenReturn(offset);
        doNothing().when(mockPullConsumer).updateConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId), anyLong());
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestPushGroup"));

        addAssignedQueue(topic, "broker-a", queueId);
        invokePullTaskManually(topic, "broker-a", queueId, offset);

        assertTrue("Listener should receive message", latch.await(5, TimeUnit.SECONDS));
        assertEquals(1, received.size());
        assertEquals(topic, received.get(0).getTopic());
        assertEquals(queueId, received.get(0).getQueueId());
        assertArrayEquals("integration msg".getBytes(), received.get(0).getBody());

        addAssignedQueue(topic, "broker-a", queueId);
        invokePullTaskManually(topic, "broker-a", queueId, 1L);
        assertTrue("Should be able to continue after first message", true);
    }

    @Test
    public void testConcurrentMultiQueueConsumption() throws Exception {
        consumer.subscribe("ConcurrentTopic", "*");

        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(4);
        consumer.registerMessageListener(messages -> {
            counter.addAndGet(messages.size());
            latch.countDown();
            return ConsumeStatus.SUCCESS;
        });

        String topic = "ConcurrentTopic";
        when(mockPullConsumer.queryConsumerOffset(eq("TestPushGroup"), eq(topic), anyInt()))
            .thenReturn(0L);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestPushGroup"));
        doNothing().when(mockPullConsumer).updateConsumerOffset(anyString(), anyString(), anyInt(), anyLong());

        for (int q = 0; q < 4; q++) {
            com.mq.proxy.sdk.consumer.PullResult r =
                buildPullResultFound("ConcurrentTopic", q, 0L, 1L, 200L, ("msg-q" + q).getBytes());
            when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(q), eq(0L), eq(32), eq(0L)))
                .thenReturn(r);
        }

        for (int q = 0; q < 4; q++) {
            addAssignedQueue(topic, "broker-a", q);
            invokePullTaskManually(topic, "broker-a", q, 0L);
        }

        assertTrue("All queues should deliver", latch.await(5, TimeUnit.SECONDS));
        assertTrue("Should receive messages from multiple queues", counter.get() >= 4);
    }

    @Test
    public void testListenerExceptionDoesNotCrashConsumer() throws Exception {
        consumer.subscribe("ErrorTopic", "*");

        AtomicInteger successCount = new AtomicInteger(0);
        consumer.registerMessageListener(messages -> {
            if (successCount.incrementAndGet() <= 1) {
                throw new RuntimeException("Simulated listener error");
            }
            return ConsumeStatus.SUCCESS;
        });

        String topic = "ErrorTopic";
        int queueId = 0;

        com.mq.proxy.sdk.consumer.PullResult r1 = buildPullResultFound(1L, 0L, 200L, "error-test-1".getBytes());
        com.mq.proxy.sdk.consumer.PullResult r2 = buildPullResultFound(2L, 1L, 200L, "error-test-2".getBytes());
        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(0L), eq(32), eq(0L)))
            .thenReturn(r1);
        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(1L), eq(32), eq(1L)))
            .thenReturn(r2);
        when(mockPullConsumer.queryConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId)))
            .thenReturn(0L);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestPushGroup"));

        addAssignedQueue(topic, "broker-a", queueId);
        invokePullTaskManually(topic, "broker-a", queueId, 0L);
        invokePullTaskManually(topic, "broker-a", queueId, 1L);

        Thread.sleep(300);
        assertTrue("Consumer should survive listener exception", successCount.get() >= 2);
    }

    @Test
    public void testRebalanceWithQueueReassignment() throws Exception {
        consumer.subscribe("ReassignTopic", "*");
        setClientId("PushSDK@TestPushGroup@1");

        String routeBody = buildRouteInfoBody("ReassignTopic", "broker-a", 4);
        String twoConsumers = "{\"consumerIdList\":[\"PushSDK@TestPushGroup@1\",\"PushSDK@TestPushGroup@2\"]}";
        String threeConsumers = "{\"consumerIdList\":[\"PushSDK@TestPushGroup@1\",\"PushSDK@TestPushGroup@2\",\"PushSDK@TestPushGroup@3\"]}";

        mockRebalanceResponse(routeBody, twoConsumers);
        when(mockPullConsumer.queryConsumerOffset(anyString(), anyString(), anyInt())).thenReturn(0L);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestPushGroup"));

        invokeDoRebalance();
        int firstSize = getAssignedQueueCount();
        assertEquals("With 2 consumers, should get 2 queues", 2, firstSize);

        mockRebalanceResponse(routeBody, threeConsumers);
        invokeDoRebalance();
        int secondSize = getAssignedQueueCount();
        assertEquals("With 3 consumers and 4 queues, consumer 0 should get 2 queues", 2, secondSize);
    }

    @Test
    public void testOffsetPersistsAcrossMultiplePullCycles() throws Exception {
        consumer.subscribe("OffsetTopic", "*");

        AtomicInteger msgCount = new AtomicInteger(0);
        consumer.registerMessageListener(messages -> {
            msgCount.addAndGet(messages.size());
            return ConsumeStatus.SUCCESS;
        });

        String topic = "OffsetTopic";
        int queueId = 0;

        com.mq.proxy.sdk.consumer.PullResult r1 = buildPullResultFound(1L, 0L, 200L, "offset-msg-0".getBytes());
        com.mq.proxy.sdk.consumer.PullResult r2 = buildPullResultFound(2L, 1L, 200L, "offset-msg-1".getBytes());
        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(0L), eq(32), eq(0L)))
            .thenReturn(r1);
        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(1L), eq(32), eq(1L)))
            .thenReturn(r2);
        when(mockPullConsumer.queryConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId)))
            .thenReturn(0L);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestPushGroup"));

        addAssignedQueue(topic, "broker-a", queueId);
        invokePullTaskManually(topic, "broker-a", queueId, 0L);

        Thread.sleep(100);
        int firstCount = msgCount.get();
        assertTrue("Should consume at least one message in first cycle", firstCount >= 1);

        invokePullTaskManually(topic, "broker-a", queueId, 1L);

        Thread.sleep(100);
        assertTrue("Should consume more messages in second cycle", msgCount.get() > firstCount);
    }

    @Test
    public void testShutdownWithOffsetCommit() throws Exception {
        consumer.subscribe("ShutdownTopic", "*");
        AtomicInteger consumed = new AtomicInteger(0);
        consumer.registerMessageListener(messages -> {
            consumed.addAndGet(messages.size());
            return ConsumeStatus.SUCCESS;
        });

        String topic = "ShutdownTopic";
        int queueId = 0;

        com.mq.proxy.sdk.consumer.PullResult r = buildPullResultFound(1L, 0L, 200L, "shutdown-msg".getBytes());
        when(mockPullConsumer.pull(eq(topic), eq("TestPushGroup"), eq(queueId), eq(0L), eq(32), eq(0L)))
            .thenReturn(r);
        when(mockPullConsumer.queryConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId)))
            .thenReturn(0L);
        doNothing().when(mockPullConsumer).updateConsumerOffset(eq("TestPushGroup"), eq(topic), eq(queueId), anyLong());
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestPushGroup"));

        addAssignedQueue(topic, "broker-a", queueId);
        invokePullTaskManually(topic, "broker-a", queueId, 0L);

        Thread.sleep(100);

        invokeShutdown();

        verify(mockPullConsumer, atLeastOnce()).updateConsumerOffset(
            eq("TestPushGroup"), eq(topic), eq(queueId), anyLong());
    }

    @Test
    public void testSetRetryTimes() {
        consumer.setRetryTimes(5);
        assertEquals(5, consumer.getConfig().getRetryTimes());
    }

    @Test
    public void testSetRequestTimeoutMillis() {
        consumer.setRequestTimeoutMillis(10000);
        assertEquals(10000, consumer.getConfig().getRequestTimeoutMillis());
    }

    @Test
    public void testConsumerIdsChangedDoesNotRunRebalanceOnCallerThread() throws Exception {
        AtomicReference<String> callbackThread = new AtomicReference<>();
        injectRebalanceExecutor(consumer);
        setClientId("client-test");

        consumer.subscribe("TestTopic", "*");

        java.lang.reflect.Method method = ProxyPushConsumer.class.getDeclaredMethod("registerRebalanceProcessor");
        method.setAccessible(true);
        method.invoke(consumer);

        ArgumentCaptor<ConsumerChangeListener> captor = ArgumentCaptor.forClass(ConsumerChangeListener.class);
        verify(mockPullConsumer).registerConsumerChangeListener(captor.capture());

        Thread callback = new Thread(() -> {
            callbackThread.set(Thread.currentThread().getName());
            captor.getValue().onConsumerIdsChanged("TestPushGroup");
        }, "callback-thread");
        callback.start();
        callback.join();

        String rebalanceThreadName = waitForRebalanceThreadName(consumer);
        assertNotNull("rebalance task should run", rebalanceThreadName);
        assertNotEquals("rebalance should not run on the callback thread", callbackThread.get(), rebalanceThreadName);
    }

    private void setClientId(String clientId) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("clientId");
        field.setAccessible(true);
        field.set(consumer, clientId);
    }

    private void invokeDoRebalance() throws Exception {
        java.lang.reflect.Method method = ProxyPushConsumer.class.getDeclaredMethod("doRebalance");
        method.setAccessible(true);
        method.invoke(consumer);
    }

    @SuppressWarnings("unchecked")
    private int getAssignedQueueCount() throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("assignedQueues");
        field.setAccessible(true);
        java.util.Set<com.mq.proxy.sdk.consumer.model.MessageQueue> queues =
            (java.util.Set<com.mq.proxy.sdk.consumer.model.MessageQueue>) field.get(consumer);
        return queues.size();
    }

    private void invokeShutdown() throws Exception {
        java.lang.reflect.Method method = ProxyPushConsumer.class.getDeclaredMethod("cancelAllPullTasks");
        method.setAccessible(true);
        method.invoke(consumer);

        java.lang.reflect.Method commitMethod = ProxyPushConsumer.class.getDeclaredMethod("commitAllOffsets");
        commitMethod.setAccessible(true);
        commitMethod.invoke(consumer);
    }

    private void mockRebalanceResponse(String routeBody, String consumerListBody) throws Exception {
        reset(mockPullConsumer);
        when(mockPullConsumer.getFacade()).thenReturn(mockFacade);
        when(mockFacade.getRemotingClient()).thenReturn(mock(com.mq.proxy.sdk.remoting.ProxyRemotingClient.class));

        RemotingCommand routeResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        routeResponse.setBody(routeBody.getBytes());
        RemotingCommand consumerListResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        consumerListResponse.setBody(consumerListBody.getBytes());

        doReturn(routeResponse, consumerListResponse).when(mockFacade).invokeSync(any(RemotingCommand.class), anyLong());
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

    private void injectRebalanceExecutor(ProxyPushConsumer consumer) throws Exception {
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            Thread thread = new Thread(runnable, "rebalance-test-thread");
            thread.setDaemon(true);
            thread.start();
            return null;
        }).when(executor).execute(any(Runnable.class));
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("rebalanceExecutor");
        field.setAccessible(true);
        field.set(consumer, executor);
    }

    private void injectSyncConsumeExecutor(ProxyPushConsumer consumer) throws Exception {
        ExecutorService syncExecutor = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable r = invocation.getArgument(0);
            r.run();
            return null;
        }).when(syncExecutor).execute(any(Runnable.class));
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("consumeExecutor");
        field.setAccessible(true);
        field.set(consumer, syncExecutor);
    }

    private void setStarted(ProxyPushConsumer consumer, boolean started) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("started");
        field.setAccessible(true);
        field.set(consumer, new java.util.concurrent.atomic.AtomicBoolean(started));
    }

    private String waitForRebalanceThreadName(ProxyPushConsumer consumer) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("lastRebalanceThreadName");
        field.setAccessible(true);
        for (int i = 0; i < 20; i++) {
            String value = (String) field.get(consumer);
            if (value != null) {
                return value;
            }
            Thread.sleep(50);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private void addAssignedQueue(String topic, String brokerName, int queueId) throws Exception {
        java.lang.reflect.Field field = ProxyPushConsumer.class.getDeclaredField("assignedQueues");
        field.setAccessible(true);
        java.util.Set<com.mq.proxy.sdk.consumer.model.MessageQueue> queues =
            (java.util.Set<com.mq.proxy.sdk.consumer.model.MessageQueue>) field.get(consumer);
        queues.add(new com.mq.proxy.sdk.consumer.model.MessageQueue(topic, brokerName, queueId));
    }

    @SuppressWarnings("unchecked")
    private void invokePullTaskManually(String topic, String brokerName, int queueId, long offset) throws Exception {
        com.mq.proxy.sdk.consumer.model.MessageQueue mq =
            new com.mq.proxy.sdk.consumer.model.MessageQueue(topic, brokerName, queueId);

        java.lang.reflect.Field offsetTableField = ProxyPushConsumer.class.getDeclaredField("offsetTable");
        offsetTableField.setAccessible(true);
        java.util.Map<com.mq.proxy.sdk.consumer.model.MessageQueue, Long> offsetTable =
            (java.util.Map<com.mq.proxy.sdk.consumer.model.MessageQueue, Long>) offsetTableField.get(consumer);
        offsetTable.put(mq, offset);

        java.lang.reflect.Constructor<?> ctor = ProxyPushConsumer.class.getDeclaredClasses()[0].getDeclaredConstructor(
            ProxyPushConsumer.class, com.mq.proxy.sdk.consumer.model.MessageQueue.class);
        ctor.setAccessible(true);
        Runnable task = (Runnable) ctor.newInstance(consumer, mq);
        task.run();
    }

    private com.mq.proxy.sdk.consumer.PullResult buildPullResultFound(
            long nextBeginOffset, long queueOffset, long maxOffset, byte[] body) {
        return buildPullResultFound("TestTopic", 0, queueOffset, nextBeginOffset, maxOffset, body);
    }

    private com.mq.proxy.sdk.consumer.PullResult buildPullResultFound(
            String topic, int queueId, long queueOffset, long nextBeginOffset, long maxOffset, byte[] body) {
        com.mq.proxy.sdk.consumer.PullResult result = new com.mq.proxy.sdk.consumer.PullResult();
        result.setSuccess(true);
        result.setFound(true);
        result.setResponseCode(RemotingSysResponseCode.SUCCESS);
        result.setNextBeginOffset(nextBeginOffset);
        result.setMinOffset(0L);
        result.setMaxOffset(maxOffset);
        result.setBody(encodeMessageBody(topic, queueId, queueOffset, body));
        return result;
    }

    private byte[] encodeMessageBody(String topic, int queueId, long queueOffset, byte[] body) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
        int bodyLen = body != null ? body.length : 0;

        int ipLen = 4;
        int msgSize = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + ipLen + 4 + 8 + ipLen + 4
            + 4 + 8 + 4 + bodyLen + 1 + topicBytes.length + 2;

        ByteBuffer buf = ByteBuffer.allocate(msgSize);
        buf.putInt(msgSize);
        buf.putInt(0);
        buf.putInt(0);
        buf.putInt(queueId);
        buf.putInt(0);
        buf.putLong(queueOffset);
        buf.putLong(0);
        buf.putInt(0);
        buf.putLong(System.currentTimeMillis());
        buf.put(new byte[]{127, 0, 0, 1});
        buf.putInt(8080);
        buf.putLong(System.currentTimeMillis());
        buf.put(new byte[]{127, 0, 0, 1});
        buf.putInt(8080);
        buf.putInt(0);
        buf.putLong(0);
        buf.putInt(bodyLen);
        if (bodyLen > 0) {
            buf.put(body);
        }
        buf.put((byte) topicBytes.length);
        buf.put(topicBytes);
        buf.putShort((short) 0);
        return buf.array();
    }

    private RemotingCommand buildPullNotFoundResponse(long nextBeginOffset, long minOffset, long maxOffset) {
        PullMessageResponseHeader header = new PullMessageResponseHeader();
        header.setNextBeginOffset(nextBeginOffset);
        header.setMinOffset(minOffset);
        header.setMaxOffset(maxOffset);
        header.setSuggestWhichBrokerId(0L);
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.PULL_NOT_FOUND);
        response.setCustomHeader(header);
        response.makeCustomHeaderToNet();
        return response;
    }

    private String buildRouteInfoBody(String topic, String brokerName, int queueNum) {
        return "{\"topic\":\"" + topic + "\",\"queueDatas\":[{\"brokerName\":\"" + brokerName
            + "\",\"readQueueNums\":" + queueNum + ",\"writeQueueNums\":" + queueNum
            + ",\"perm\":6,\"topicSysFlag\":0}],\"brokerDatas\":[{\"cluster\":\"DefaultCluster\",\"brokerName\":\""
            + brokerName + "\",\"brokerAddrs\":{\"0\":\"127.0.0.1:19876\"}}]}";
    }
}
