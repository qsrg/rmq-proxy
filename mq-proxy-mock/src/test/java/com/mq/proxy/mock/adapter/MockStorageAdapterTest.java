package com.mq.proxy.mock.adapter;

import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MockStorageAdapterTest {

    private MockStorageAdapter adapter;
    private static final String BROKER_ADDR = "127.0.0.1:10911";

    @Before
    public void setUp() {
        adapter = new MockStorageAdapter();
        StorageConfig config = new StorageConfig();
        try {
            adapter.initialize(config);
        } catch (Exception e) {
            fail("initialize failed: " + e.getMessage());
        }
    }

    @Test
    public void testInitializeAndShutdown() throws Exception {
        assertTrue(adapter.healthCheck());
        adapter.shutdown();
        assertFalse(adapter.healthCheck());
    }

    @Test
    public void testPutMessage() throws Exception {
        InternalMessage message = new InternalMessage();
        message.setTopic("TestTopic");
        message.setQueueId(0);
        message.setBody("hello".getBytes());

        PutResult result = adapter.putMessage(message, BROKER_ADDR);

        assertTrue(result.isSuccess());
        assertNotNull(result.getMsgId());
        assertTrue(result.getMsgId().startsWith("MOCK_MSG_"));
        assertEquals(0, result.getQueueId());
        assertEquals(0, result.getQueueOffset());
    }

    @Test
    public void testPullMessage() throws Exception {
        InternalMessage message = new InternalMessage();
        message.setTopic("TestTopic");
        message.setQueueId(1);
        message.setBody("hello".getBytes());

        adapter.putMessage(message, BROKER_ADDR);

        PullResult result = adapter.pullMessage("TestGroup", "TestTopic", 1, 0, 10, 3000, null, null, BROKER_ADDR);

        assertEquals(0, result.getResponseCode());
        assertNotNull(result.getMessageList());
        assertEquals(1, result.getMessageList().size());
        assertEquals(1, result.getNextBeginOffset());
        assertEquals(0, result.getMinOffset());
        assertEquals(1, result.getMaxOffset());
    }

    @Test
    public void testPullMessageNotFound() throws Exception {
        PullResult result = adapter.pullMessage("TestGroup", "NonExistTopic", 0, 0, 10, 3000, null, null, BROKER_ADDR);
        assertEquals(19, result.getResponseCode());
    }

    @Test
    public void testConsumerOffset() throws Exception {
        adapter.updateConsumerOffset("TestGroup", "TestTopic", 0, 100L, BROKER_ADDR);

        OffsetResult result = adapter.queryConsumerOffset("TestGroup", "TestTopic", 0, BROKER_ADDR);

        assertTrue(result.isSuccess());
        assertEquals(100L, result.getOffset());
    }

    @Test
    public void testQueryConsumerOffsetNotFound() throws Exception {
        OffsetResult result = adapter.queryConsumerOffset("NonExistGroup", "NonExistTopic", 0, BROKER_ADDR);

        assertFalse(result.isSuccess());
        assertEquals(22, result.getResponseCode());
    }

    @Test
    public void testHealthCheck() {
        assertTrue(adapter.healthCheck());
    }
}
