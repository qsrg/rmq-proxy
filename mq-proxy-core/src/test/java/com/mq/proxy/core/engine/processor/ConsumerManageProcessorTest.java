package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetRequestHeader;
import com.mq.proxy.core.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageAdapterManager;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConsumerManageProcessorTest {

    private MessageEngine messageEngine;
    private StorageAdapterManager storageAdapterManager;
    private StorageAdapter mockAdapter;
    private ConsumerManageProcessor processor;

    @Before
    public void setUp() {
        storageAdapterManager = new StorageAdapterManager();
        mockAdapter = mock(StorageAdapter.class);
        storageAdapterManager.registerAdapter("default", mockAdapter, true);
        messageEngine = new MessageEngine(storageAdapterManager);
        processor = new ConsumerManageProcessor(messageEngine);
    }

    @Test
    public void testQueryConsumerOffset() throws Exception {
        when(mockAdapter.queryConsumerOffset("testGroup", "TestTopic", 0)).thenReturn(OffsetResult.success(500L));

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

        verify(mockAdapter).queryConsumerOffset("testGroup", "TestTopic", 0);
    }

    @Test
    public void testUpdateConsumerOffset() throws Exception {
        doNothing().when(mockAdapter).updateConsumerOffset("testGroup", "TestTopic", 0, 600L);

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

        verify(mockAdapter).updateConsumerOffset("testGroup", "TestTopic", 0, 600L);
    }
}
