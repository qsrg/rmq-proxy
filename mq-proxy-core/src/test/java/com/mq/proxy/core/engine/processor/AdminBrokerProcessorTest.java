package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.storage.StorageAdapter;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AdminBrokerProcessorTest {

    private MessageEngine messageEngine;
    private StorageAdapter mockAdapter;
    private AdminBrokerProcessor processor;
    private VirtualRouteManager mockRouteManager;

    @Before
    public void setUp() {
        mockAdapter = mock(StorageAdapter.class);
        messageEngine = new MessageEngine(mockAdapter);
        processor = new AdminBrokerProcessor(messageEngine);
        mockRouteManager = mock(VirtualRouteManager.class);
        processor.setVirtualRouteManager(mockRouteManager);
        when(mockRouteManager.findBrokerNameByTopicAndQueueId(anyString(), anyInt())).thenReturn("default-broker");
        when(mockRouteManager.getRealBrokerAddr(anyString())).thenReturn("127.0.0.1:10911");
    }

    @Test
    public void testGetMaxOffset() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        HashMap<String, String> respFields = new HashMap<>();
        respFields.put("offset", "1000");
        brokerResponse.setExtFields(respFields);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), any());
    }

    @Test
    public void testGetMinOffset() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        HashMap<String, String> respFields = new HashMap<>();
        respFields.put("offset", "0");
        brokerResponse.setExtFields(respFields);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testSearchOffsetByTimestamp() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        HashMap<String, String> respFields = new HashMap<>();
        respFields.put("offset", "500");
        brokerResponse.setExtFields(respFields);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        extFields.put("timestamp", String.valueOf(System.currentTimeMillis()));
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testGetEarliestMsgStoretime() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        HashMap<String, String> respFields = new HashMap<>();
        respFields.put("timestamp", String.valueOf(System.currentTimeMillis()));
        brokerResponse.setExtFields(respFields);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testGetAllConsumerOffset() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        brokerResponse.setBody(new byte[]{1, 2, 3});
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testBrokerError() throws Exception {
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any()))
                .thenThrow(new RuntimeException("broker timeout"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, response.getCode());
    }

    @Test
    public void testUnsupportedRequestCode() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(999, null);
        request.setExtFields(new HashMap<>());

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, response.getCode());
    }

    @Test
    public void testOffsetQueryWithBrokerNameRouting() throws Exception {
        when(mockRouteManager.getRealBrokerAddr("broker-a")).thenReturn("192.168.1.1:10911");
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), eq("192.168.1.1:10911")))
                .thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        extFields.put("bname", "broker-a");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), eq("192.168.1.1:10911"));
    }

    @Test
    public void testOffsetQueryWithTopicRouting() throws Exception {
        when(mockRouteManager.findBrokerNameByTopicAndQueueId("TestTopic", 0)).thenReturn("broker-b");
        when(mockRouteManager.getRealBrokerAddr("broker-b")).thenReturn("192.168.1.2:10911");
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), eq("192.168.1.2:10911")))
                .thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), eq("192.168.1.2:10911"));
    }

    @Test
    public void testQueryMessage() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        brokerResponse.setBody(new byte[]{1, 2, 3});
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        extFields.put("key", "order123");
        extFields.put("maxNum", "32");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), any());
    }

    @Test
    public void testViewMessageById() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        brokerResponse.setBody(new byte[]{4, 5, 6});
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("offset", "1000");
        extFields.put("topic", "TestTopic");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testUpdateAndCreateTopic() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "NewTopic");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testGetAllTopicConfig() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        brokerResponse.setBody(new byte[]{7, 8, 9});
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TestTopic");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testDeleteTopicInBroker() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "OldTopic");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testConsumerOpsForwarding() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        int[] consumerOpsCodes = {
                RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP,
                RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG,
                RequestCode.GET_TOPIC_STATS_INFO,
                RequestCode.GET_CONSUMER_CONNECTION_LIST,
                RequestCode.GET_PRODUCER_CONNECTION_LIST,
                RequestCode.DELETE_SUBSCRIPTIONGROUP,
                RequestCode.GET_CONSUME_STATS,
                RequestCode.RESET_CONSUMER_OFFSET_IN_BROKER,
                RequestCode.QUERY_TOPIC_CONSUME_BY_WHO,
                RequestCode.QUERY_CONSUME_TIME_SPAN,
                RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER,
                RequestCode.GET_CONSUMER_RUNNING_INFO,
                RequestCode.INVOKE_BROKER_TO_RESET_OFFSET,
                RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS,
                RequestCode.CLONE_GROUP_OFFSET,
                RequestCode.GET_BROKER_CONSUME_STATS
        };

        for (int requestCode : consumerOpsCodes) {
            reset(mockAdapter);
            when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

            RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, null);
            HashMap<String, String> extFields = new HashMap<>();
            extFields.put("consumerGroup", "testGroup");
            extFields.put("topic", "TestTopic");
            request.setExtFields(extFields);

            RemotingCommand response = processor.processRequest(null, request);
            assertEquals("Expected SUCCESS for requestCode=" + requestCode,
                    RemotingSysResponseCode.SUCCESS, response.getCode());
            verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), any());
        }
    }

    @Test
    public void testGetAllDelayOffsetWithoutTopic() throws Exception {
        when(mockRouteManager.getAllRealBrokerAddrs()).thenReturn(Arrays.asList("192.168.1.1:10911", "192.168.1.2:10911"));
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        brokerResponse.setBody("{\"offsetTable\":{1:0,2:0,3:0}}".getBytes());
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), eq("192.168.1.1:10911")))
                .thenReturn(brokerResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());
        verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), eq("192.168.1.1:10911"));
    }

    @Test
    public void testAdminRequestsWithoutTopicFallbackToFirstBroker() throws Exception {
        when(mockRouteManager.getAllRealBrokerAddrs()).thenReturn(Arrays.asList("192.168.1.1:10911"));
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        brokerResponse.setBody("ok".getBytes());
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), eq("192.168.1.1:10911")))
                .thenReturn(brokerResponse);

        int[] codesWithoutTopic = {
                RequestCode.GET_ALL_CONSUMER_OFFSET,
                RequestCode.GET_BROKER_CONFIG,
                RequestCode.GET_BROKER_RUNTIME_INFO,
                RequestCode.GET_ALL_TOPIC_CONFIG,
                RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG
        };

        for (int requestCode : codesWithoutTopic) {
            reset(mockAdapter);
            when(mockAdapter.forwardToBroker(any(RemotingCommand.class), eq("192.168.1.1:10911")))
                    .thenReturn(brokerResponse);

            RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, null);
            RemotingCommand response = processor.processRequest(null, request);
            assertEquals("Expected SUCCESS for requestCode=" + requestCode,
                    RemotingSysResponseCode.SUCCESS, response.getCode());
            verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), eq("192.168.1.1:10911"));
        }
    }

    @Test
    public void testNoBrokerAddrAvailable() throws Exception {
        when(mockRouteManager.getAllRealBrokerAddrs()).thenReturn(null);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET, null);

        RemotingCommand response = processor.processRequest(null, request);

        assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, response.getCode());
    }

    @Test
    public void testBrokerConfigForwarding() throws Exception {
        RemotingCommand brokerResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

        int[] configCodes = {
                RequestCode.UPDATE_BROKER_CONFIG,
                RequestCode.GET_BROKER_CONFIG,
                RequestCode.GET_BROKER_RUNTIME_INFO
        };

        for (int requestCode : configCodes) {
            reset(mockAdapter);
            when(mockAdapter.forwardToBroker(any(RemotingCommand.class), any())).thenReturn(brokerResponse);

            RemotingCommand request = RemotingCommand.createRequestCommand(requestCode, null);
            HashMap<String, String> configExtFields = new HashMap<>();
            configExtFields.put("topic", "TestTopic");
            request.setExtFields(configExtFields);

            RemotingCommand response = processor.processRequest(null, request);
            assertEquals("Expected SUCCESS for requestCode=" + requestCode,
                    RemotingSysResponseCode.SUCCESS, response.getCode());
            verify(mockAdapter).forwardToBroker(any(RemotingCommand.class), any());
        }
    }
}
