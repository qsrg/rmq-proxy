package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.ConsumerSendMsgBackRequestHeader;
import com.mq.proxy.core.storage.StorageAdapter;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConsumerSendMsgBackProcessorTest {

    private MessageEngine messageEngine;
    private StorageAdapter mockAdapter;
    private ConsumerSendMsgBackProcessor processor;
    private VirtualRouteManager mockRouteManager;

    @Before
    public void setUp() {
        mockAdapter = mock(StorageAdapter.class);
        messageEngine = new MessageEngine(mockAdapter);
        processor = new ConsumerSendMsgBackProcessor(messageEngine);
        mockRouteManager = mock(VirtualRouteManager.class);
        processor.setVirtualRouteManager(mockRouteManager);
        when(mockRouteManager.findBrokerNameByTopicAndQueueId(anyString(), anyInt())).thenReturn("default-broker");
        when(mockRouteManager.getRealBrokerAddr(anyString())).thenReturn("127.0.0.1:10911");
    }

    @Test
    public void testConsumerSendMsgBackSuccess() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("offset", "100");
        extFields.put("group", "testConsumerGroup");
        extFields.put("delayLevel", "3");
        extFields.put("originMsgId", "msg123");
        extFields.put("originTopic", "TestTopic");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), any());
    }

    @Test
    public void testConsumerSendMsgBackWithBrokerName() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any()))
                .thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("offset", "200");
        extFields.put("group", "testConsumerGroup");
        extFields.put("delayLevel", "5");
        extFields.put("originTopic", "TestTopic");
        extFields.put("bname", "broker-a");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(mockAdapter).forwardToBroker(argThat(cmd -> {
            HashMap<String, String> ef = cmd.getExtFields();
            return ef != null && "broker-a".equals(ef.get("bname"));
        }), any());
    }

    @Test
    public void testConsumerSendMsgBackBrokerError() throws Exception {
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any()))
                .thenThrow(new RuntimeException("broker connection failed"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("offset", "100");
        extFields.put("group", "testConsumerGroup");
        extFields.put("delayLevel", "3");
        extFields.put("originTopic", "TestTopic");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, response.getCode());
        assertTrue(response.getRemark().contains("broker connection failed"));
    }

    @Test
    public void testConsumerSendMsgBackWithCustomHeader() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        ConsumerSendMsgBackRequestHeader header = new ConsumerSendMsgBackRequestHeader();
        header.setOffset(300L);
        header.setGroup("testGroup");
        header.setDelayLevel(1);
        header.setOriginMsgId("msg456");
        header.setOriginTopic("TestTopic");
        header.setMaxReconsumeTimes(16);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, header);
        request.makeCustomHeaderToNet();

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testParseRequestHeaderFromExtFields() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("offset", "50");
        extFields.put("group", "group1");
        extFields.put("delayLevel", "2");
        extFields.put("originMsgId", "id1");
        extFields.put("originTopic", "Topic1");
        extFields.put("unitMode", "true");
        extFields.put("maxReconsumeTimes", "5");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        verify(mockAdapter).forwardToBroker(argThat(cmd -> {
            HashMap<String, String> ef = cmd.getExtFields();
            return ef != null
                    && ef.get("offset").equals("50")
                    && ef.get("group").equals("group1")
                    && ef.get("delayLevel").equals("2")
                    && ef.get("originMsgId").equals("id1")
                    && ef.get("originTopic").equals("Topic1")
                    && ef.get("unitMode").equals("true")
                    && ef.get("maxReconsumeTimes").equals("5");
        }), any());
    }
}
