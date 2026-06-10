package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.storage.PullMessageCallback;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PullResult;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PullMessageProcessorTest {

    private MessageEngine messageEngine;
    private StorageAdapter mockAdapter;
    private PullMessageProcessor processor;
    private Channel channel;

    @Before
    public void setUp() {
        mockAdapter = mock(StorageAdapter.class);
        messageEngine = new MessageEngine(mockAdapter);
        processor = new PullMessageProcessor(messageEngine);
        channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
    }

    @Test
    public void testPullMessageFound() throws Exception {
        byte[] mockBody = new byte[]{1, 2, 3};
        stubAsyncPullResult("testGroup", "TestTopic", 0, 0L, 32, PullResult.found(mockBody, 100L, 0L, 200L));

        RemotingCommand response = processAndCaptureResponse(channel, createPullRequest("testGroup", "TestTopic", 0, 0L, 32, 4, 0L, 0L, null, null));

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertArrayEquals(mockBody, response.getBody());
        assertEquals("100", response.getExtFields().get("nextBeginOffset"));
        assertEquals("0", response.getExtFields().get("minOffset"));
        assertEquals("200", response.getExtFields().get("maxOffset"));
    }

    @Test
    public void testPullMessageNotFound() throws Exception {
        stubAsyncPullResult("testGroup", "EmptyTopic", 0, 0L, 32, PullResult.notFound(0L, 0L, 0L));

        RemotingCommand response = processAndCaptureResponse(channel, createPullRequest("testGroup", "EmptyTopic", 0, 0L, 32, 4, 0L, 0L, null, null));

        assertEquals(ResponseCode.PULL_NOT_FOUND, response.getCode());
    }

    @Test
    public void testPullMessageWithBrokerName() throws Exception {
        byte[] mockBody = new byte[]{4, 5, 6};
        stubAsyncPullResult("testGroup", "TestTopic", 1, 50L, 16, PullResult.found(mockBody, 60L, 0L, 100L));

        RemotingCommand response = processAndCaptureResponse(channel, createPullRequest("testGroup", "TestTopic", 1, 50L, 16, 4, 0L, 0L, null, "broker-a"));

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testPullMessageWithDefaultValues() throws Exception {
        doAnswer(invocation -> {
            PullMessageCallback callback = invocation.getArgument(12);
            callback.onSuccess(PullResult.notFound(0L, 0L, 0L));
            return null;
        }).when(mockAdapter).pullMessageAsync(anyString(), anyString(), anyInt(), anyLong(), anyInt(),
                anyInt(), anyLong(), anyLong(), any(), any(), anyLong(), any(), any(PullMessageCallback.class));

        RemotingCommand response = processAndCaptureResponse(channel, createMinimalPullRequest("testGroup", "TestTopic"));

        assertEquals(ResponseCode.PULL_NOT_FOUND, response.getCode());
    }

    @Test
    public void testPullMessageWithMessageList() throws Exception {
        List<InternalMessage> messageList = new ArrayList<>();
        InternalMessage msg = new InternalMessage();
        msg.setTopic("TestTopic");
        msg.setQueueId(0);
        msg.setBody("hello".getBytes());
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setSysFlag(0);
        msg.setReconsumeTimes(0);
        messageList.add(msg);

        stubAsyncPullResult("testGroup", "TestTopic", 0, 0L, 32, PullResult.found(messageList, 1L, 0L, 1L));

        RemotingCommand response = processAndCaptureResponse(channel, createPullRequest("testGroup", "TestTopic", 0, 0L, 32, 0, 0L, 0L, null, null));

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().length > 0);
    }

    @Test
    public void testPullMessageWithCommitOffset() throws Exception {
        byte[] mockBody = new byte[]{1, 2, 3};
        stubAsyncPullResult("orderlyGroup", "OrderlyTopic", 2, 100L, 32, PullResult.found(mockBody, 101L, 0L, 200L));

        processAndCaptureResponse(channel, createPullRequest("orderlyGroup", "OrderlyTopic", 2, 100L, 32, 3, 500L, 0L, "TAG", null));

        verify(mockAdapter).pullMessageAsync(eq("orderlyGroup"), eq("OrderlyTopic"), eq(2), eq(100L), eq(32),
                eq(3), eq(500L), anyLong(), any(), any(), anyLong(), any(), any(PullMessageCallback.class));
    }

    @Test
    public void testPullMessageTopicNotExistIsConvertedToNotFound() throws Exception {
        PullResult pullResult = new PullResult();
        pullResult.setResponseCode(ResponseCode.TOPIC_NOT_EXIST);
        pullResult.setNextBeginOffset(0L);
        pullResult.setMinOffset(0L);
        pullResult.setMaxOffset(0L);
        stubAsyncPullResult("testGroup", "NewTopic", 0, 0L, 32, pullResult);

        RemotingCommand response = processAndCaptureResponse(channel, createPullRequest("testGroup", "NewTopic", 0, 0L, 32, 4, 0L, 15000L, null, null));

        assertEquals(ResponseCode.PULL_NOT_FOUND, response.getCode());
        assertEquals("0", response.getExtFields().get("nextBeginOffset"));
        assertEquals("0", response.getExtFields().get("minOffset"));
        assertEquals("0", response.getExtFields().get("maxOffset"));
    }

    @Test
    public void testPullMessageExceptionPreservesRequestedOffset() throws Exception {
        doAnswer(invocation -> {
            PullMessageCallback callback = invocation.getArgument(12);
            callback.onException(new RuntimeException("timeout"));
            return null;
        }).when(mockAdapter).pullMessageAsync(eq("testGroup"), eq("TestTopic"), eq(0), eq(5L), eq(32),
                anyInt(), anyLong(), anyLong(), any(), any(), anyLong(), any(), any(PullMessageCallback.class));

        RemotingCommand response = processAndCaptureResponse(channel, createPullRequest("testGroup", "TestTopic", 0, 5L, 32, 3, 5L, 15000L, null, null));

        assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, response.getCode());
        assertEquals("5", response.getExtFields().get("nextBeginOffset"));
    }

    @Test
    public void testRetryTopicPullUsesFreshRouteWhenRequestBrokerMissing() throws Exception {
        VirtualRouteManager routeManager = mock(VirtualRouteManager.class);
        messageEngine.setVirtualRouteManager(routeManager);
        processor.setVirtualRouteManager(routeManager);

        when(routeManager.findBrokerNameByTopicAndQueueId("%RETRY%testGroup", 0)).thenReturn("broker-a");
        when(routeManager.getRealBrokerAddr("broker-a")).thenReturn("127.0.0.1:10911");
        stubAsyncPullResult("testGroup", "%RETRY%testGroup", 0, 0L, 32, PullResult.notFound(0L, 0L, 0L));

        RemotingCommand response = processAndCaptureResponse(channel, createPullRequest("testGroup", "%RETRY%testGroup", 0, 0L, 32, 4, 0L, 15000L, null, null));

        assertEquals(ResponseCode.PULL_NOT_FOUND, response.getCode());
        verify(mockAdapter).pullMessageAsync(eq("testGroup"), eq("%RETRY%testGroup"), eq(0), eq(0L), eq(32),
                anyInt(), anyLong(), anyLong(), any(), any(), anyLong(), eq("127.0.0.1:10911"), any(PullMessageCallback.class));
    }

    @Test
    public void testRetryTopicPullHonorsRequestBrokerWhenPresent() throws Exception {
        VirtualRouteManager routeManager = mock(VirtualRouteManager.class);
        messageEngine.setVirtualRouteManager(routeManager);
        processor.setVirtualRouteManager(routeManager);

        when(routeManager.findBrokerNameByTopicAndQueueId("%RETRY%testGroup", 0)).thenReturn("broker-a");
        when(routeManager.getRealBrokerAddr("broker-b")).thenReturn("127.0.0.1:20911");
        stubAsyncPullResult("testGroup", "%RETRY%testGroup", 0, 0L, 32, PullResult.notFound(0L, 0L, 0L));

        RemotingCommand response = processAndCaptureResponse(channel, createPullRequest("testGroup", "%RETRY%testGroup", 0, 0L, 32, 4, 0L, 15000L, null, "broker-b"));

        assertEquals(ResponseCode.PULL_NOT_FOUND, response.getCode());
        verify(mockAdapter).pullMessageAsync(eq("testGroup"), eq("%RETRY%testGroup"), eq(0), eq(0L), eq(32),
                anyInt(), anyLong(), anyLong(), any(), any(), anyLong(), eq("127.0.0.1:20911"), any(PullMessageCallback.class));
    }

    @Test
    public void testPullMessageAsyncSkipsWriteWhenChannelInactive() throws Exception {
        when(channel.isActive()).thenReturn(false);
        stubAsyncPullResult("testGroup", "TestTopic", 0, 0L, 32, PullResult.notFound(0L, 0L, 0L));

        RemotingCommand request = createPullRequest("testGroup", "TestTopic", 0, 0L, 32, 4, 0L, 0L, null, null);
        RemotingCommand immediateResponse = processor.processRequest(channel, request);

        assertNull(immediateResponse);
        verify(channel, never()).writeAndFlush(any());
    }

    private RemotingCommand processAndCaptureResponse(Channel channel, RemotingCommand request) throws Exception {
        RemotingCommand immediateResponse = processor.processRequest(channel, request);
        assertNull(immediateResponse);

        ArgumentCaptor<RemotingCommand> captor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(channel).writeAndFlush(captor.capture());
        return captor.getValue();
    }

    private void stubAsyncPullResult(String consumerGroup, String topic, int queueId,
                                     long queueOffset, int maxMsgNums, PullResult pullResult) throws Exception {
        doAnswer(invocation -> {
            PullMessageCallback callback = invocation.getArgument(12);
            callback.onSuccess(pullResult);
            return null;
        }).when(mockAdapter).pullMessageAsync(eq(consumerGroup), eq(topic), eq(queueId), eq(queueOffset), eq(maxMsgNums),
                anyInt(), anyLong(), anyLong(), any(), any(), anyLong(), any(), any(PullMessageCallback.class));
    }

    private RemotingCommand createMinimalPullRequest(String consumerGroup, String topic) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", consumerGroup);
        extFields.put("topic", topic);
        request.setExtFields(extFields);
        return request;
    }

    private RemotingCommand createPullRequest(String consumerGroup, String topic, int queueId, long queueOffset,
                                              int maxMsgNums, int sysFlag, long commitOffset, long suspendTimeoutMillis,
                                              String expressionType, String brokerName) {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", consumerGroup);
        extFields.put("topic", topic);
        extFields.put("queueId", String.valueOf(queueId));
        extFields.put("queueOffset", String.valueOf(queueOffset));
        extFields.put("maxMsgNums", String.valueOf(maxMsgNums));
        extFields.put("sysFlag", String.valueOf(sysFlag));
        extFields.put("commitOffset", String.valueOf(commitOffset));
        extFields.put("suspendTimeoutMillis", String.valueOf(suspendTimeoutMillis));
        extFields.put("subVersion", String.valueOf(System.currentTimeMillis()));
        if (expressionType != null) {
            extFields.put("expressionType", expressionType);
        }
        if (brokerName != null) {
            extFields.put("bname", brokerName);
        }
        request.setExtFields(extFields);
        return request;
    }
}
