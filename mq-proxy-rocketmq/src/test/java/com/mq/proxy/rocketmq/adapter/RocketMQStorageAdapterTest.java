package com.mq.proxy.rocketmq.adapter;

import com.mq.proxy.core.storage.StorageConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RocketMQStorageAdapterTest {

    @Test
    public void testGetAdapterName() {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        assertEquals("rocketmq", adapter.getAdapterName());
    }

    @Test
    public void testInitializeAndShutdown() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        StorageConfig config = new StorageConfig();
        config.setNamesrvAddr("localhost:9876");
        config.setBrokerAddr("localhost:10911");

        adapter.initialize(config);
        assertTrue(adapter.healthCheck());

        adapter.shutdown();
        assertFalse(adapter.healthCheck());
    }

    @Test
    public void testHealthCheck() {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        assertFalse(adapter.healthCheck());
    }
}
