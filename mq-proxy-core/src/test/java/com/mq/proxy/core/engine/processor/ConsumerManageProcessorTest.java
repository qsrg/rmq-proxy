package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.model.OffsetResult;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConsumerManageProcessorTest {

    private MessageEngine messageEngine;
    private StorageAdapter mockAdapter;
    private VirtualRouteManager routeManager;
    private ConsumerManageProcessor processor;

    @Before
    public void setUp() {
        mockAdapter = mock(StorageAdapter.class);
        routeManager = mock(VirtualRouteManager.class);
        messageEngine = new MessageEngine(mockAdapter);
        processor = new ConsumerManageProcessor(messageEngine);
    }

    @Test
    public void testQueryConsumerOffset() throws Exception {
        when(mockAdapter.queryConsumerOffset(eq("testGroup"), eq("TestTopic"), eq(0), any())).thenReturn(OffsetResult.success(500L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        HashMap<String, String> respExtFields = response.getExtFields();
        assertNotNull(respExtFields);
        assertEquals("500", respExtFields.get("offset"));

        verify(mockAdapter).queryConsumerOffset(eq("testGroup"), eq("TestTopic"), eq(0), any());
    }

    @Test
    public void testQueryConsumerOffsetUsesBrokerNameFromRequest() throws Exception {
        messageEngine.setVirtualRouteManager(routeManager);
        processor.setVirtualRouteManager(routeManager);
        when(routeManager.getRealBrokerAddr("broker-b")).thenReturn("192.168.1.2:10911");
        when(mockAdapter.queryConsumerOffset(eq("testGroup"), eq("TestTopic"), eq(0), eq("192.168.1.2:10911")))
                .thenReturn(OffsetResult.success(500L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        extFields.put("bname", "broker-b");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(routeManager).getRealBrokerAddr("broker-b");
        verify(mockAdapter).queryConsumerOffset(eq("testGroup"), eq("TestTopic"), eq(0), eq("192.168.1.2:10911"));
    }

    @Test
    public void testUpdateConsumerOffset() throws Exception {
        doNothing().when(mockAdapter).updateConsumerOffset(eq("testGroup"), eq("TestTopic"), eq(0), eq(600L), any());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        extFields.put("commitOffset", "600");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        verify(mockAdapter).updateConsumerOffset(eq("testGroup"), eq("TestTopic"), eq(0), eq(600L), any());
    }

    @Test
    public void testUpdateConsumerOffsetUsesBrokerNameFromRequest() throws Exception {
        messageEngine.setVirtualRouteManager(routeManager);
        processor.setVirtualRouteManager(routeManager);
        when(routeManager.getRealBrokerAddr("broker-b")).thenReturn("192.168.1.2:10911");
        doNothing().when(mockAdapter).updateConsumerOffset(eq("testGroup"), eq("TestTopic"), eq(0), eq(600L), eq("192.168.1.2:10911"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "TestTopic");
        extFields.put("queueId", "0");
        extFields.put("commitOffset", "600");
        extFields.put("bname", "broker-b");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(routeManager).getRealBrokerAddr("broker-b");
        verify(mockAdapter).updateConsumerOffset(eq("testGroup"), eq("TestTopic"), eq(0), eq(600L), eq("192.168.1.2:10911"));
    }

    @Test
    public void testRetryTopicQueryConsumerOffsetHonorsRequestBrokerWhenPresent() throws Exception {
        messageEngine.setVirtualRouteManager(routeManager);
        processor.setVirtualRouteManager(routeManager);
        when(routeManager.findBrokerNameByTopicAndQueueId("%RETRY%testGroup", 0)).thenReturn("broker-a");
        when(routeManager.getRealBrokerAddr("broker-b")).thenReturn("192.168.1.3:10911");
        when(mockAdapter.queryConsumerOffset(eq("testGroup"), eq("%RETRY%testGroup"), eq(0), eq("192.168.1.3:10911")))
                .thenReturn(OffsetResult.success(123L));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "%RETRY%testGroup");
        extFields.put("queueId", "0");
        extFields.put("bname", "broker-b");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(mockAdapter).queryConsumerOffset(eq("testGroup"), eq("%RETRY%testGroup"), eq(0), eq("192.168.1.3:10911"));
    }

    @Test
    public void testRetryTopicUpdateConsumerOffsetHonorsRequestBrokerWhenPresent() throws Exception {
        messageEngine.setVirtualRouteManager(routeManager);
        processor.setVirtualRouteManager(routeManager);
        when(routeManager.findBrokerNameByTopicAndQueueId("%RETRY%testGroup", 0)).thenReturn("broker-a");
        when(routeManager.getRealBrokerAddr("broker-b")).thenReturn("192.168.1.3:10911");
        doNothing().when(mockAdapter).updateConsumerOffset(eq("testGroup"), eq("%RETRY%testGroup"), eq(0), eq(456L), eq("192.168.1.3:10911"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "testGroup");
        extFields.put("topic", "%RETRY%testGroup");
        extFields.put("queueId", "0");
        extFields.put("commitOffset", "456");
        extFields.put("bname", "broker-b");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(null, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        verify(mockAdapter).updateConsumerOffset(eq("testGroup"), eq("%RETRY%testGroup"), eq(0), eq(456L), eq("192.168.1.3:10911"));
    }
}
