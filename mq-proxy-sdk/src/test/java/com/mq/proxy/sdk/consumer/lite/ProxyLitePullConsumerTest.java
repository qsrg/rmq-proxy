package com.mq.proxy.sdk.consumer.lite;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.sdk.consumer.ProxyConsumer;
import com.mq.proxy.sdk.consumer.model.MessageQueue;
import com.mq.proxy.sdk.consumer.model.ProxyMessage;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ProxyLitePullConsumerTest {

    private ProxyClientFacade mockFacade;
    private ProxyLitePullConsumer consumer;
    private ProxyConsumer mockPullConsumer;

    @Before
    public void setUp() throws Exception {
        mockFacade = mock(ProxyClientFacade.class);
        when(mockFacade.getRemotingClient()).thenReturn(mock(com.mq.proxy.sdk.remoting.ProxyRemotingClient.class));

        mockPullConsumer = mock(ProxyConsumer.class);

        consumer = new ProxyLitePullConsumer("TestLitePullGroup");
        consumer.setProxyAddrs("127.0.0.1:19876");

        injectMockPullConsumer(consumer, mockPullConsumer);
        injectMockPullExecutor(consumer, mock(ScheduledExecutorService.class));
        setStarted(consumer, true);
    }

    @Test
    public void testSubscribeAndPollMessages() throws Exception {
        consumer.subscribe("TestTopic", "*");

        String topic = "TestTopic";
        int queueId = 0;
        String brokerName = "broker-a";
        byte[] messageBody = "poll message".getBytes();

        com.mq.proxy.sdk.consumer.PullResult pullResult = buildPullResultFound(101L, 100L, 200L, messageBody);
        when(mockPullConsumer.pull(anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
            .thenReturn(pullResult);
        when(mockPullConsumer.queryConsumerOffset(anyString(), anyString(), anyInt()))
            .thenReturn(100L);

        addAssignedQueue(topic, brokerName, queueId);
        startPullTaskManually(topic, brokerName, queueId, 100L);

        Thread.sleep(500);

        List<ProxyMessage> messages = consumer.poll(2000);

        assertFalse("Should receive messages", messages.isEmpty());
        assertEquals(1, messages.size());
        ProxyMessage msg = messages.get(0);
        assertEquals(topic, msg.getTopic());
        assertEquals(queueId, msg.getQueueId());
        assertArrayEquals(messageBody, msg.getBody());
    }

    @Test
    public void testPollReturnsEmptyWhenNoMessages() throws Exception {
        consumer.subscribe("TestTopic", "*");
        consumer.setAutoCommit(false);

        when(mockPullConsumer.pull(anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
            .thenReturn(null);

        addAssignedQueue("TestTopic", "broker-a", 0);
        startPullTaskManually("TestTopic", "broker-a", 0, 0L);

        Thread.sleep(500);

        List<ProxyMessage> messages = consumer.poll(500);

        assertTrue("Should be empty", messages.isEmpty());
    }

    @Test
    public void testAssignmentReturnsAssignedQueues() throws Exception {
        consumer.subscribe("TestTopic", "*");

        addAssignedQueue("TestTopic", "broker-a", 0);
        addAssignedQueue("TestTopic", "broker-a", 1);

        Set<MessageQueue> assignment = consumer.assignment();
        assertEquals(2, assignment.size());
    }

    @Test
    public void testSeekToBegin() throws Exception {
        consumer.subscribe("TestTopic", "*");

        String topic = "TestTopic";
        int queueId = 0;
        String brokerName = "broker-a";

        addAssignedQueue(topic, brokerName, queueId);
        startPullTaskManually(topic, brokerName, queueId, 50L);

        consumer.seekToBegin(topic, queueId);

        clearSeekOffsetTable(topic, queueId);

        com.mq.proxy.sdk.consumer.PullResult pullResult = buildPullResultFound(1L, 0L, 200L, "after seek".getBytes());
        when(mockPullConsumer.pull(eq(topic), eq("TestLitePullGroup"), eq(queueId), eq(0L), eq(32), anyLong()))
            .thenReturn(pullResult);
        when(mockPullConsumer.queryConsumerOffset(eq("TestLitePullGroup"), eq(topic), eq(queueId)))
            .thenReturn(0L);

        setConsumeOffset(topic, queueId, 50L);

        invokePullTaskManually(topic, brokerName, queueId, 0L);

        Thread.sleep(500);

        List<ProxyMessage> messages = consumer.poll(2000);
        assertFalse("Should receive message after seek", messages.isEmpty());
        assertEquals(0L, messages.get(0).getQueueOffset());
    }

    @Test
    public void testAutoCommitConfiguration() {
        assertTrue("Auto commit should be enabled by default", consumer.getConfig().isAutoCommit());

        consumer.setAutoCommit(false);
        assertFalse("Auto commit should be disabled", consumer.getConfig().isAutoCommit());
    }

    @Test
    public void testDefaultConfig() {
        ProxyLitePullConsumer c = new ProxyLitePullConsumer("DefaultGroup");
        assertEquals("DefaultGroup", c.getConfig().getConsumerGroup());
        assertEquals(32, c.getConfig().getPullBatchSize());
        assertTrue(c.getConfig().isAutoCommit());
    }

    @Test
    public void testMultipleMessagesFromSameQueue() throws Exception {
        consumer.subscribe("TestTopic", "*");

        String topic = "TestTopic";
        int queueId = 0;
        String brokerName = "broker-a";

        com.mq.proxy.sdk.consumer.PullResult result1 = buildPullResultFound(101L, 100L, 200L, "msg1".getBytes());
        com.mq.proxy.sdk.consumer.PullResult result2 = buildPullResultFound(102L, 101L, 200L, "msg2".getBytes());
        com.mq.proxy.sdk.consumer.PullResult result3 = buildPullResultFound(103L, 102L, 200L, "msg3".getBytes());

        when(mockPullConsumer.pull(eq(topic), eq("TestLitePullGroup"), eq(queueId), eq(100L), eq(32), eq(100L)))
            .thenReturn(result1);
        when(mockPullConsumer.pull(eq(topic), eq("TestLitePullGroup"), eq(queueId), eq(101L), eq(32), eq(101L)))
            .thenReturn(result2);
        when(mockPullConsumer.pull(eq(topic), eq("TestLitePullGroup"), eq(queueId), eq(102L), eq(32), eq(102L)))
            .thenReturn(result3);
        when(mockPullConsumer.queryConsumerOffset(eq("TestLitePullGroup"), eq(topic), eq(queueId)))
            .thenReturn(100L);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestLitePullGroup"));

        addAssignedQueue(topic, brokerName, queueId);
        startPullTaskManually(topic, brokerName, queueId, 100L);

        invokePullTaskManually(topic, brokerName, queueId, 100L);
        invokePullTaskManually(topic, brokerName, queueId, 101L);
        invokePullTaskManually(topic, brokerName, queueId, 102L);

        Thread.sleep(500);

        List<ProxyMessage> messages = consumer.poll(2000);
        assertTrue("Should receive at least one message", messages.size() >= 1);
    }

    @Test
    public void testConsumerNotStartedThrowsException() {
        ProxyLitePullConsumer c = new ProxyLitePullConsumer("NotStartedGroup");
        try {
            c.poll(100);
            fail("Should throw IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not started"));
        }
    }

    @Test
    public void testFullLifecycleRebalancePollCommit() throws Exception {
        ProxyLitePullConsumer fullConsumer = new ProxyLitePullConsumer("FullLifecycleGroup");
        fullConsumer.setProxyAddrs("127.0.0.1:19876");
        fullConsumer.setAutoCommit(false);

        ProxyConsumer fullMockPull = mock(ProxyConsumer.class);

        injectMockPullConsumer(fullConsumer, fullMockPull);
        injectMockPullExecutor(fullConsumer, mock(ScheduledExecutorService.class));
        setStarted(fullConsumer, true);

        fullConsumer.subscribe("FullLifecycleTopic", "*");

        addAssignedQueueTo(fullConsumer, "FullLifecycleTopic", "broker-a", 0);

        com.mq.proxy.sdk.consumer.PullResult pullResult = buildPullResultFound("FullLifecycleTopic", 0, 0L, 1L, 200L, "lifecycle-msg".getBytes());
        when(fullMockPull.pull(eq("FullLifecycleTopic"), eq("FullLifecycleGroup"), eq(0), eq(0L), eq(32), eq(0L)))
            .thenReturn(pullResult);

        invokePullTaskForConsumer(fullConsumer, "FullLifecycleTopic", "broker-a", 0, 0L);

        Thread.sleep(500);

        List<ProxyMessage> messages = fullConsumer.poll(2000);
        assertFalse("Should receive messages", messages.isEmpty());
        assertEquals(1, messages.size());
        assertEquals("FullLifecycleTopic", messages.get(0).getTopic());
        assertArrayEquals("lifecycle-msg".getBytes(), messages.get(0).getBody());

        fullConsumer.commitAll();
        verify(fullMockPull, atLeastOnce()).updateConsumerOffset(
            eq("FullLifecycleGroup"), eq("FullLifecycleTopic"), eq(0), anyLong());
    }

    @Test
    public void testSeekToSpecificOffset() throws Exception {
        consumer.subscribe("SeekTopic", "*");
        consumer.setAutoCommit(false);

        String topic = "SeekTopic";
        int queueId = 0;
        String brokerName = "broker-a";

        addAssignedQueue(topic, brokerName, queueId);
        startPullTaskManually(topic, brokerName, queueId, 100L);

        consumer.seek(topic, queueId, 50L);

        clearSeekOffsetTable(topic, queueId);

        com.mq.proxy.sdk.consumer.PullResult pullResult = buildPullResultFound(51L, 50L, 200L, "seek-msg".getBytes());
        when(mockPullConsumer.pull(eq(topic), eq("TestLitePullGroup"), eq(queueId), eq(50L), eq(32), anyLong()))
            .thenReturn(pullResult);
        when(mockPullConsumer.queryConsumerOffset(eq("TestLitePullGroup"), eq(topic), eq(queueId)))
            .thenReturn(50L);

        setConsumeOffset(topic, queueId, 100L);
        invokePullTaskManually(topic, brokerName, queueId, 50L);

        Thread.sleep(500);

        List<ProxyMessage> messages = consumer.poll(2000);
        assertFalse("Should receive message after seek", messages.isEmpty());
        assertEquals(50L, messages.get(0).getQueueOffset());
    }

    @Test
    public void testAutoCommitTriggersAfterInterval() throws Exception {
        ProxyLitePullConsumer autoConsumer = new ProxyLitePullConsumer("AutoCommitGroup");
        autoConsumer.setProxyAddrs("127.0.0.1:19876");
        autoConsumer.setAutoCommit(true);

        ProxyConsumer autoMockPull = mock(ProxyConsumer.class);
        injectMockPullConsumer(autoConsumer, autoMockPull);
        injectMockPullExecutor(autoConsumer, mock(ScheduledExecutorService.class));
        setStarted(autoConsumer, true);

        autoConsumer.subscribe("AutoCommitTopic", "*");

        addAssignedQueueTo(autoConsumer, "AutoCommitTopic", "broker-a", 0);
        startPullTaskForConsumer(autoConsumer, "AutoCommitTopic", "broker-a", 0, 0L);

        com.mq.proxy.sdk.consumer.PullResult pullResult =
            buildPullResultFound("AutoCommitTopic", 0, 0L, 1L, 200L, "auto-commit-msg".getBytes());
        when(autoMockPull.pull(eq("AutoCommitTopic"), eq("AutoCommitGroup"), eq(0), eq(0L), eq(32), eq(0L)))
            .thenReturn(pullResult);
        when(autoMockPull.queryConsumerOffset(eq("AutoCommitGroup"), eq("AutoCommitTopic"), eq(0)))
            .thenReturn(0L);
        when(autoMockPull.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("AutoCommitGroup"));

        invokePullTaskForConsumer(autoConsumer, "AutoCommitTopic", "broker-a", 0, 0L);

        Thread.sleep(500);

        List<ProxyMessage> messages = autoConsumer.poll(1000);
        assertFalse("Should receive message", messages.isEmpty());

        assertTrue(autoConsumer.getConfig().isAutoCommit());
        autoConsumer.commitAll();
        verify(autoMockPull, atLeastOnce()).updateConsumerOffset(
            eq("AutoCommitGroup"), eq("AutoCommitTopic"), eq(0), anyLong());
    }

    @Test
    public void testPollWithTimeoutReturnsEmpty() throws Exception {
        consumer.subscribe("EmptyTopic", "*");
        consumer.setAutoCommit(false);

        addAssignedQueue("EmptyTopic", "broker-a", 0);

        when(mockPullConsumer.pull(anyString(), anyString(), anyInt(), anyLong(), anyInt(), anyLong()))
            .thenReturn(null);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestLitePullGroup"));

        long start = System.currentTimeMillis();
        List<ProxyMessage> messages = consumer.poll(200);
        long elapsed = System.currentTimeMillis() - start;

        assertTrue("Should return empty list", messages.isEmpty());
        assertTrue("Should respect timeout", elapsed >= 150);
    }

    @Test
    public void testMultipleQueuesPolling() throws Exception {
        consumer.subscribe("MultiQueueTopic", "*");
        consumer.setAutoCommit(false);

        String topic = "MultiQueueTopic";

        addAssignedQueue(topic, "broker-a", 0);
        addAssignedQueue(topic, "broker-a", 1);

        startPullTaskManually(topic, "broker-a", 0, 0L);
        startPullTaskManually(topic, "broker-a", 1, 0L);

        com.mq.proxy.sdk.consumer.PullResult r0 = buildPullResultFound("MultiQueueTopic", 0, 0L, 1L, 200L, "q0-msg".getBytes());
        com.mq.proxy.sdk.consumer.PullResult r1 = buildPullResultFound("MultiQueueTopic", 1, 0L, 1L, 200L, "q1-msg".getBytes());

        when(mockPullConsumer.pull(eq(topic), eq("TestLitePullGroup"), eq(0), eq(0L), eq(32), eq(0L)))
            .thenReturn(r0);
        when(mockPullConsumer.pull(eq(topic), eq("TestLitePullGroup"), eq(1), eq(0L), eq(32), eq(0L)))
            .thenReturn(r1);
        when(mockPullConsumer.queryConsumerOffset(eq("TestLitePullGroup"), eq(topic), anyInt()))
            .thenReturn(0L);
        when(mockPullConsumer.getConfig()).thenReturn(
            new com.mq.proxy.sdk.consumer.ProxyConsumerConfig().setConsumerGroup("TestLitePullGroup"));

        invokePullTaskManually(topic, "broker-a", 0, 0L);
        invokePullTaskManually(topic, "broker-a", 1, 0L);

        Thread.sleep(500);

        List<ProxyMessage> messages = consumer.poll(2000);
        assertTrue("Should receive messages from both queues", messages.size() >= 2);

        Set<Integer> queueIds = new HashSet<>();
        for (ProxyMessage msg : messages) {
            queueIds.add(msg.getQueueId());
        }
        assertTrue("Should contain queue 0", queueIds.contains(0));
        assertTrue("Should contain queue 1", queueIds.contains(1));
    }

    @Test
    public void testCommitAfterConsumption() throws Exception {
        consumer.subscribe("CommitTopic", "*");
        consumer.setAutoCommit(false);

        String topic = "CommitTopic";
        int queueId = 0;
        String brokerName = "broker-a";

        addAssignedQueue(topic, brokerName, queueId);
        startPullTaskManually(topic, brokerName, queueId, 0L);

        com.mq.proxy.sdk.consumer.PullResult pullResult = buildPullResultFound("CommitTopic", 0, 0L, 1L, 200L, "commit-msg".getBytes());
        when(mockPullConsumer.pull(eq(topic), eq("TestLitePullGroup"), eq(queueId), eq(0L), eq(32), anyLong()))
            .thenReturn(pullResult);
        when(mockPullConsumer.queryConsumerOffset(eq("TestLitePullGroup"), eq(topic), eq(queueId)))
            .thenReturn(0L);

        setConsumeOffset(topic, queueId, 50L);

        invokePullTaskManually(topic, brokerName, queueId, 0L);

        Thread.sleep(500);

        List<ProxyMessage> messages = consumer.poll(2000);
        assertFalse("Should receive message", messages.isEmpty());

        consumer.commitAll();

        verify(mockPullConsumer, atLeastOnce()).updateConsumerOffset(
            eq("TestLitePullGroup"), eq(topic), eq(queueId), anyLong());
    }

    private void injectMockPullConsumer(ProxyLitePullConsumer consumer, ProxyConsumer pullConsumer) throws Exception {
        java.lang.reflect.Field field = ProxyLitePullConsumer.class.getDeclaredField("pullConsumer");
        field.setAccessible(true);
        field.set(consumer, pullConsumer);
    }

    private void injectMockPullExecutor(ProxyLitePullConsumer consumer, ScheduledExecutorService executor) throws Exception {
        java.lang.reflect.Field field = ProxyLitePullConsumer.class.getDeclaredField("pullExecutor");
        field.setAccessible(true);
        field.set(consumer, executor);
    }

    @SuppressWarnings("unchecked")
    private void setConsumeOffset(String topic, int queueId, long offset) throws Exception {
        java.lang.reflect.Field field = ProxyLitePullConsumer.class.getDeclaredField("consumeOffsetTable");
        field.setAccessible(true);
        java.util.Map<MessageQueue, Long> table = (java.util.Map<MessageQueue, Long>) field.get(consumer);
        table.put(new MessageQueue(topic, "", queueId), offset);
    }

    @SuppressWarnings("unchecked")
    private void clearSeekOffsetTable(String topic, int queueId) throws Exception {
        java.lang.reflect.Field field = ProxyLitePullConsumer.class.getDeclaredField("seekOffsetTable");
        field.setAccessible(true);
        java.util.Map<MessageQueue, Long> table = (java.util.Map<MessageQueue, Long>) field.get(consumer);
        table.keySet().removeIf(mq -> mq.getTopic().equals(topic) && mq.getQueueId() == queueId);
    }

    private void setStarted(ProxyLitePullConsumer consumer, boolean started) throws Exception {
        java.lang.reflect.Field field = ProxyLitePullConsumer.class.getDeclaredField("started");
        field.setAccessible(true);
        field.set(consumer, new java.util.concurrent.atomic.AtomicBoolean(started));
    }

    @SuppressWarnings("unchecked")
    private void addAssignedQueue(String topic, String brokerName, int queueId) throws Exception {
        java.lang.reflect.Field field = ProxyLitePullConsumer.class.getDeclaredField("assignedQueues");
        field.setAccessible(true);
        java.util.Set<MessageQueue> queues = (java.util.Set<MessageQueue>) field.get(consumer);
        queues.add(new MessageQueue(topic, brokerName, queueId));
    }

    @SuppressWarnings("unchecked")
    private void startPullTaskManually(String topic, String brokerName, int queueId, long offset) throws Exception {
        MessageQueue mq = new MessageQueue(topic, brokerName, queueId);

        java.lang.reflect.Field pullOffsetField = ProxyLitePullConsumer.class.getDeclaredField("pullOffsetTable");
        pullOffsetField.setAccessible(true);
        java.util.Map<MessageQueue, Long> pullOffsetTable =
            (java.util.Map<MessageQueue, Long>) pullOffsetField.get(consumer);
        pullOffsetTable.put(mq, offset);

        java.lang.reflect.Field consumeOffsetField = ProxyLitePullConsumer.class.getDeclaredField("consumeOffsetTable");
        consumeOffsetField.setAccessible(true);
        java.util.Map<MessageQueue, Long> consumeOffsetTable =
            (java.util.Map<MessageQueue, Long>) consumeOffsetField.get(consumer);
        consumeOffsetTable.put(mq, offset);

        java.lang.reflect.Field cacheField = ProxyLitePullConsumer.class.getDeclaredField("messageCache");
        cacheField.setAccessible(true);
        java.util.Map<MessageQueue, java.util.concurrent.BlockingQueue<ProxyMessage>> cache =
            (java.util.Map<MessageQueue, java.util.concurrent.BlockingQueue<ProxyMessage>>) cacheField.get(consumer);
        cache.put(mq, new java.util.concurrent.LinkedBlockingQueue<>());

        java.lang.reflect.Constructor<?> ctor = ProxyLitePullConsumer.class.getDeclaredClasses()[0]
            .getDeclaredConstructor(ProxyLitePullConsumer.class, MessageQueue.class);
        ctor.setAccessible(true);
        Runnable task = (Runnable) ctor.newInstance(consumer, mq);

        java.lang.reflect.Field tasksField = ProxyLitePullConsumer.class.getDeclaredField("pullTasks");
        tasksField.setAccessible(true);
        java.util.Map<MessageQueue, Runnable> tasks =
            (java.util.Map<MessageQueue, Runnable>) tasksField.get(consumer);
        tasks.put(mq, task);

        task.run();
    }

    @SuppressWarnings("unchecked")
    private void invokePullTaskManually(String topic, String brokerName, int queueId, long offset) throws Exception {
        MessageQueue mq = new MessageQueue(topic, brokerName, queueId);

        java.lang.reflect.Field pullOffsetField = ProxyLitePullConsumer.class.getDeclaredField("pullOffsetTable");
        pullOffsetField.setAccessible(true);
        java.util.Map<MessageQueue, Long> pullOffsetTable =
            (java.util.Map<MessageQueue, Long>) pullOffsetField.get(consumer);
        pullOffsetTable.put(mq, offset);

        java.lang.reflect.Constructor<?> ctor = ProxyLitePullConsumer.class.getDeclaredClasses()[0]
            .getDeclaredConstructor(ProxyLitePullConsumer.class, MessageQueue.class);
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

    private void invokeLiteRebalance(ProxyLitePullConsumer c, String clientId) throws Exception {
        java.lang.reflect.Field clientIdField = ProxyLitePullConsumer.class.getDeclaredField("clientId");
        clientIdField.setAccessible(true);
        clientIdField.set(c, clientId);

        java.lang.reflect.Method method = ProxyLitePullConsumer.class.getDeclaredMethod("doRebalance");
        method.setAccessible(true);
        method.invoke(c);
    }

    @SuppressWarnings("unchecked")
    private void addAssignedQueueTo(ProxyLitePullConsumer c, String topic, String brokerName, int queueId) throws Exception {
        java.lang.reflect.Field field = ProxyLitePullConsumer.class.getDeclaredField("assignedQueues");
        field.setAccessible(true);
        java.util.Set<MessageQueue> queues = (java.util.Set<MessageQueue>) field.get(c);
        queues.add(new MessageQueue(topic, brokerName, queueId));
    }

    @SuppressWarnings("unchecked")
    private void startPullTaskForConsumer(ProxyLitePullConsumer c, String topic, String brokerName, int queueId, long offset) throws Exception {
        MessageQueue mq = new MessageQueue(topic, brokerName, queueId);

        java.lang.reflect.Field pullOffsetField = ProxyLitePullConsumer.class.getDeclaredField("pullOffsetTable");
        pullOffsetField.setAccessible(true);
        java.util.Map<MessageQueue, Long> pullOffsetTable =
            (java.util.Map<MessageQueue, Long>) pullOffsetField.get(c);
        pullOffsetTable.put(mq, offset);

        java.lang.reflect.Field consumeOffsetField = ProxyLitePullConsumer.class.getDeclaredField("consumeOffsetTable");
        consumeOffsetField.setAccessible(true);
        java.util.Map<MessageQueue, Long> consumeOffsetTable =
            (java.util.Map<MessageQueue, Long>) consumeOffsetField.get(c);
        consumeOffsetTable.put(mq, offset);

        java.lang.reflect.Field cacheField = ProxyLitePullConsumer.class.getDeclaredField("messageCache");
        cacheField.setAccessible(true);
        java.util.Map<MessageQueue, java.util.concurrent.BlockingQueue<ProxyMessage>> cache =
            (java.util.Map<MessageQueue, java.util.concurrent.BlockingQueue<ProxyMessage>>) cacheField.get(c);
        cache.put(mq, new java.util.concurrent.LinkedBlockingQueue<>());

        java.lang.reflect.Constructor<?> ctor = ProxyLitePullConsumer.class.getDeclaredClasses()[0]
            .getDeclaredConstructor(ProxyLitePullConsumer.class, MessageQueue.class);
        ctor.setAccessible(true);
        Runnable task = (Runnable) ctor.newInstance(c, mq);

        java.lang.reflect.Field tasksField = ProxyLitePullConsumer.class.getDeclaredField("pullTasks");
        tasksField.setAccessible(true);
        java.util.Map<MessageQueue, Runnable> tasks =
            (java.util.Map<MessageQueue, Runnable>) tasksField.get(c);
        tasks.put(mq, task);

        task.run();
    }

    @SuppressWarnings("unchecked")
    private void invokePullTaskForConsumer(ProxyLitePullConsumer c, String topic, String brokerName, int queueId, long offset) throws Exception {
        MessageQueue mq = new MessageQueue(topic, brokerName, queueId);

        java.lang.reflect.Field pullOffsetField = ProxyLitePullConsumer.class.getDeclaredField("pullOffsetTable");
        pullOffsetField.setAccessible(true);
        java.util.Map<MessageQueue, Long> pullOffsetTable =
            (java.util.Map<MessageQueue, Long>) pullOffsetField.get(c);
        pullOffsetTable.put(mq, offset);

        java.lang.reflect.Constructor<?> ctor = ProxyLitePullConsumer.class.getDeclaredClasses()[0]
            .getDeclaredConstructor(ProxyLitePullConsumer.class, MessageQueue.class);
        ctor.setAccessible(true);
        Runnable task = (Runnable) ctor.newInstance(c, mq);
        task.run();
    }

    private String buildRouteInfoJson(String topic, String brokerName, int queueNum) {
        return "{\"topic\":\"" + topic + "\",\"queueDatas\":[{\"brokerName\":\"" + brokerName
            + "\",\"readQueueNums\":" + queueNum + ",\"writeQueueNums\":" + queueNum
            + ",\"perm\":6,\"topicSysFlag\":0}],\"brokerDatas\":[{\"cluster\":\"DefaultCluster\",\"brokerName\":\""
            + brokerName + "\",\"brokerAddrs\":{\"0\":\"127.0.0.1:19876\"}}]}";
    }
}