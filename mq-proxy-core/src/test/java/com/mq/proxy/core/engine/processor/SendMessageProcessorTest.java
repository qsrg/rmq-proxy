package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PutResult;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SendMessageProcessorTest {

    private MessageEngine messageEngine;
    private StorageAdapter mockAdapter;
    private SendMessageProcessor processor;

    @Before
    public void setUp() {
        mockAdapter = mock(StorageAdapter.class);
        messageEngine = new MessageEngine(mockAdapter);
        processor = new SendMessageProcessor(messageEngine);
    }

    @Test
    public void testProcessSendMessage() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any())).thenReturn(PutResult.success("msg123", 0, 100L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("producerGroup", "testProducerGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("defaultTopic", "defaultTopic");
        extFields.put("defaultTopicQueueNums", "4");
        extFields.put("queueId", "0");
        extFields.put("sysFlag", "0");
        extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        extFields.put("flag", "0");
        extFields.put("reconsumeTimes", "0");
        extFields.put("unitMode", "false");
        extFields.put("batch", "false");
        request.setExtFields(extFields);
        request.setBody("hello world".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        HashMap<String, String> respExtFields = response.getExtFields();
        assertNotNull(respExtFields);
        assertEquals("msg123", respExtFields.get("msgId"));
        assertEquals("0", respExtFields.get("queueId"));
        assertEquals("100", respExtFields.get("queueOffset"));

        verify(mockAdapter).putMessage(any(InternalMessage.class), any());
    }

    @Test
    public void testProcessSendMessageV2() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any())).thenReturn(PutResult.success("msgV2-456", 1, 200L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("a", "testProducerGroup");
        extFields.put("b", "TestTopicV2");
        extFields.put("c", "defaultTopic");
        extFields.put("d", "4");
        extFields.put("e", "1");
        extFields.put("f", "0");
        extFields.put("g", String.valueOf(System.currentTimeMillis()));
        extFields.put("h", "0");
        extFields.put("i", null);
        extFields.put("j", "0");
        extFields.put("k", "false");
        extFields.put("l", "0");
        extFields.put("m", "false");
        request.setExtFields(extFields);
        request.setBody("hello v2".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        HashMap<String, String> respExtFields = response.getExtFields();
        assertNotNull(respExtFields);
        assertEquals("msgV2-456", respExtFields.get("msgId"));
        assertEquals("1", respExtFields.get("queueId"));
        assertEquals("200", respExtFields.get("queueOffset"));

        verify(mockAdapter).putMessage(any(InternalMessage.class), any());
    }

    @Test
    public void testProcessSendMessageBrokerReject() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any()))
                .thenReturn(PutResult.fail(14, "TOPIC_NOT_EXIST"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("producerGroup", "testProducerGroup");
        extFields.put("topic", "NotExistTopic");
        extFields.put("defaultTopic", "defaultTopic");
        extFields.put("defaultTopicQueueNums", "4");
        extFields.put("queueId", "0");
        extFields.put("sysFlag", "0");
        extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        extFields.put("flag", "0");
        request.setExtFields(extFields);
        request.setBody("test".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(14, response.getCode());
        assertEquals("TOPIC_NOT_EXIST", response.getRemark());
    }

    @Test
    public void testProcessSendMessageBrokerUnavailable() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any()))
                .thenThrow(new RuntimeException("Connection refused"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("producerGroup", "testProducerGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("defaultTopic", "defaultTopic");
        extFields.put("defaultTopicQueueNums", "4");
        extFields.put("queueId", "0");
        extFields.put("sysFlag", "0");
        extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        extFields.put("flag", "0");
        request.setExtFields(extFields);
        request.setBody("test".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, response.getCode());
    }

    @Test
    public void testProcessSendMessageWithReconsumeTimes() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any())).thenReturn(PutResult.success("retry-msg-1", 0, 50L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("producerGroup", "testProducerGroup");
        extFields.put("topic", "%RETRY%testConsumerGroup");
        extFields.put("defaultTopic", "defaultTopic");
        extFields.put("defaultTopicQueueNums", "4");
        extFields.put("queueId", "0");
        extFields.put("sysFlag", "0");
        extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        extFields.put("flag", "0");
        extFields.put("reconsumeTimes", "3");
        extFields.put("maxReconsumeTimes", "16");
        extFields.put("unitMode", "false");
        extFields.put("batch", "false");
        request.setExtFields(extFields);
        request.setBody("retry message".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        verify(mockAdapter).putMessage(argThat(msg ->
                msg.getReconsumeTimes() == 3 && msg.getMaxReconsumeTimes() == 16
        ), any());
    }

    @Test
    public void testProcessSendBatchMessage() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any())).thenReturn(PutResult.success("batch-msg-1", 0, 300L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_BATCH_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("a", "testProducerGroup");
        extFields.put("b", "BatchTopic");
        extFields.put("c", "defaultTopic");
        extFields.put("d", "4");
        extFields.put("e", "0");
        extFields.put("f", "0");
        extFields.put("g", String.valueOf(System.currentTimeMillis()));
        extFields.put("h", "0");
        extFields.put("i", null);
        extFields.put("j", "0");
        extFields.put("k", "false");
        extFields.put("l", "0");
        extFields.put("m", "false");
        request.setExtFields(extFields);
        request.setBody("batch body".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        verify(mockAdapter).putMessage(argThat(msg -> msg.isBatch()), any());
    }

    @Test
    public void testProcessSendMessageWithDelayLevel() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any())).thenReturn(PutResult.success("delay-msg-1", 0, 150L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("producerGroup", "testProducerGroup");
        extFields.put("topic", "OrderTopic");
        extFields.put("defaultTopic", "defaultTopic");
        extFields.put("defaultTopicQueueNums", "4");
        extFields.put("queueId", "0");
        extFields.put("sysFlag", "0");
        extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        extFields.put("flag", "0");
        extFields.put("properties", "DELAY\u00013\u0002");
        extFields.put("reconsumeTimes", "0");
        extFields.put("unitMode", "false");
        extFields.put("batch", "false");
        request.setExtFields(extFields);
        request.setBody("delayed message".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        verify(mockAdapter).putMessage(argThat(msg ->
                msg.getProperties() != null && msg.getProperties().contains("DELAY")
        ), any());
    }

    @Test
    public void testProcessSendMessageV2WithDelayLevel() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any())).thenReturn(PutResult.success("delay-v2-msg", 0, 250L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("a", "testProducerGroup");
        extFields.put("b", "DelayTopic");
        extFields.put("c", "defaultTopic");
        extFields.put("d", "4");
        extFields.put("e", "0");
        extFields.put("f", "0");
        extFields.put("g", String.valueOf(System.currentTimeMillis()));
        extFields.put("h", "0");
        extFields.put("i", "DELAY\u00015\u0002");
        extFields.put("j", "0");
        extFields.put("k", "false");
        extFields.put("l", "0");
        extFields.put("m", "false");
        request.setExtFields(extFields);
        request.setBody("delayed v2 message".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        verify(mockAdapter).putMessage(argThat(msg ->
                msg.getProperties() != null && msg.getProperties().contains("DELAY")
        ), any());
    }

    @Test
    public void testProcessUnsupportedRequestCode() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(999, null);
        request.setExtFields(new HashMap<>());
        request.setBody("test".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, response.getCode());
    }

    @Test
    public void testProcessSendMessageWithSpecifiedQueueId() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class), any())).thenReturn(PutResult.success("orderly-msg-1", 2, 400L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("producerGroup", "orderlyProducerGroup");
        extFields.put("topic", "OrderlyTopic");
        extFields.put("defaultTopic", "defaultTopic");
        extFields.put("defaultTopicQueueNums", "4");
        extFields.put("queueId", "2");
        extFields.put("sysFlag", "0");
        extFields.put("bornTimestamp", String.valueOf(System.currentTimeMillis()));
        extFields.put("flag", "0");
        extFields.put("reconsumeTimes", "0");
        extFields.put("unitMode", "false");
        extFields.put("batch", "false");
        request.setExtFields(extFields);
        request.setBody("orderly message".getBytes());

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        HashMap<String, String> respExtFields = response.getExtFields();
        assertEquals("2", respExtFields.get("queueId"));

        verify(mockAdapter).putMessage(argThat(msg ->
                msg.getQueueId() != null && msg.getQueueId() == 2
        ), any());
    }
}
