package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.PullMessageResponseHeader;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageAdapterManager;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PullResult;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PullMessageProcessorTest {

    private MessageEngine messageEngine;
    private StorageAdapterManager storageAdapterManager;
    private StorageAdapter mockAdapter;
    private PullMessageProcessor processor;

    @Before
    public void setUp() {
        storageAdapterManager = new StorageAdapterManager();
        mockAdapter = mock(StorageAdapter.class);
        storageAdapterManager.registerAdapter("default", mockAdapter, true);
        messageEngine = new MessageEngine(storageAdapterManager);
        processor = new PullMessageProcessor(messageEngine);
    }

    @Test
    public void testPullMessageFound() throws Exception {
        byte[] mockBody = new byte[]{1, 2, 3};
        when(mockAdapter.pullMessage(eq("testGroup"), eq("TestTopic"), eq(0), eq(0L), eq(32),
                anyLong(), any(), any(), any()))
                .thenReturn(PullResult.found(mockBody, 100L, 0L, 200L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        extFields.put("queueOffset", "0");
        extFields.put("maxMsgNums", "32");
        extFields.put("sysFlag", "4");
        extFields.put("commitOffset", "0");
        extFields.put("suspendTimeoutMillis", "0");
        extFields.put("subVersion", String.valueOf(System.currentTimeMillis()));
        extFields.put("expressionType", "TAG");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        HashMap<String, String> respExtFields = response.getExtFields();
        assertNotNull(respExtFields);
        assertEquals("100", respExtFields.get("nextBeginOffset"));
        assertEquals("0", respExtFields.get("minOffset"));
        assertEquals("200", respExtFields.get("maxOffset"));
    }

    @Test
    public void testPullMessageNotFound() throws Exception {
        when(mockAdapter.pullMessage(eq("testGroup"), eq("EmptyTopic"), eq(0), eq(0L), eq(32),
                anyLong(), any(), any(), any()))
                .thenReturn(PullResult.notFound(0L, 0L, 0L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "EmptyTopic");
        extFields.put("queueId", "0");
        extFields.put("queueOffset", "0");
        extFields.put("maxMsgNums", "32");
        extFields.put("sysFlag", "4");
        extFields.put("commitOffset", "0");
        extFields.put("suspendTimeoutMillis", "0");
        extFields.put("subVersion", String.valueOf(System.currentTimeMillis()));
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(ResponseCode.PULL_NOT_FOUND, response.getCode());
    }

    @Test
    public void testPullMessageWithBrokerName() throws Exception {
        byte[] mockBody = new byte[]{4, 5, 6};
        when(mockAdapter.pullMessage(eq("testGroup"), eq("TestTopic"), eq(1), eq(50L), eq(16),
                anyLong(), any(), any(), any()))
                .thenReturn(PullResult.found(mockBody, 60L, 0L, 100L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "1");
        extFields.put("queueOffset", "50");
        extFields.put("maxMsgNums", "16");
        extFields.put("sysFlag", "4");
        extFields.put("commitOffset", "0");
        extFields.put("suspendTimeoutMillis", "0");
        extFields.put("subVersion", String.valueOf(System.currentTimeMillis()));
        extFields.put("bname", "broker-a");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testPullMessageWithDefaultValues() throws Exception {
        when(mockAdapter.pullMessage(anyString(), anyString(), anyInt(), anyLong(), anyInt(),
                anyLong(), any(), any(), any()))
                .thenReturn(PullResult.notFound(0L, 0L, 0L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "TestTopic");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
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
        msg.setProperties(null);
        messageList.add(msg);

        when(mockAdapter.pullMessage(eq("testGroup"), eq("TestTopic"), eq(0), eq(0L), eq(32),
                anyLong(), any(), any(), any()))
                .thenReturn(PullResult.found(messageList, 1L, 0L, 1L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        extFields.put("queueOffset", "0");
        extFields.put("maxMsgNums", "32");
        extFields.put("sysFlag", "0");
        extFields.put("suspendTimeoutMillis", "0");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());
        assertTrue(response.getBody().length > 0);
    }
}