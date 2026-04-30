package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeader;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeaderV2;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageAdapterManager;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PutResult;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SendMessageProcessorTest {

    private MessageEngine messageEngine;
    private StorageAdapterManager storageAdapterManager;
    private StorageAdapter mockAdapter;
    private SendMessageProcessor processor;

    @Before
    public void setUp() {
        storageAdapterManager = new StorageAdapterManager();
        mockAdapter = mock(StorageAdapter.class);
        storageAdapterManager.registerAdapter("default", mockAdapter, true);
        messageEngine = new MessageEngine(storageAdapterManager);
        processor = new SendMessageProcessor(messageEngine);
    }

    @Test
    public void testProcessSendMessage() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class))).thenReturn(PutResult.success("msg123", 0, 100L));

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

        verify(mockAdapter).putMessage(any(InternalMessage.class));
    }

    @Test
    public void testProcessSendMessageV2() throws Exception {
        when(mockAdapter.putMessage(any(InternalMessage.class))).thenReturn(PutResult.success("msgV2-456", 1, 200L));

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

        verify(mockAdapter).putMessage(any(InternalMessage.class));
    }
}
