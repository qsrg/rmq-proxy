package com.mq.proxy.core.storage;

import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PutResult;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class StorageAdapterManagerTest {

    private StorageAdapterManager manager;
    private StorageAdapter mockAdapter1;
    private StorageAdapter mockAdapter2;

    @Before
    public void setUp() {
        manager = new StorageAdapterManager();
        mockAdapter1 = mock(StorageAdapter.class);
        mockAdapter2 = mock(StorageAdapter.class);
        when(mockAdapter1.getAdapterName()).thenReturn("rocketmq");
        when(mockAdapter2.getAdapterName()).thenReturn("mock");
        when(mockAdapter1.healthCheck()).thenReturn(true);
        when(mockAdapter2.healthCheck()).thenReturn(true);
    }

    @Test
    public void testRegisterAdapter() {
        manager.registerAdapter("rocketmq", mockAdapter1, true);
        manager.registerAdapter("mock", mockAdapter2, false);

        assertEquals(mockAdapter1, manager.getAdapter("rocketmq"));
        assertEquals(mockAdapter2, manager.getAdapter("mock"));
    }

    @Test
    public void testDefaultAdapter() {
        manager.registerAdapter("rocketmq", mockAdapter1, true);
        manager.registerAdapter("mock", mockAdapter2, false);

        assertEquals(mockAdapter1, manager.getAdapterByTopic(null));
        assertEquals(mockAdapter1, manager.getAdapterByTopic("UnknownTopic"));
    }

    @Test
    public void testTopicRouting() {
        manager.registerAdapter("rocketmq", mockAdapter1, true);
        manager.registerAdapter("mock", mockAdapter2, false);

        manager.routeTopic("TopicA", "rocketmq");
        manager.routeTopic("TopicB", "mock");

        assertEquals(mockAdapter1, manager.getAdapterByTopic("TopicA"));
        assertEquals(mockAdapter2, manager.getAdapterByTopic("TopicB"));
        assertEquals(mockAdapter1, manager.getAdapterByTopic("TopicC"));
    }

    @Test
    public void testDefaultAdapterOverridden() {
        manager.registerAdapter("mock", mockAdapter2, true);
        manager.registerAdapter("rocketmq", mockAdapter1, false);

        assertEquals(mockAdapter2, manager.getAdapterByTopic(null));
    }

    @Test
    public void testHealthCheckAll() {
        manager.registerAdapter("rocketmq", mockAdapter1, true);
        manager.registerAdapter("mock", mockAdapter2, false);

        assertTrue(manager.healthCheckAll());
    }

    @Test
    public void testHealthCheckAllWithFailure() {
        manager.registerAdapter("rocketmq", mockAdapter1, true);
        when(mockAdapter1.healthCheck()).thenReturn(false);

        assertFalse(manager.healthCheckAll());
    }

    @Test
    public void testHealthCheckAllEmpty() {
        assertFalse(manager.healthCheckAll());
    }

    @Test
    public void testShutdownAll() throws Exception {
        manager.registerAdapter("rocketmq", mockAdapter1, true);
        manager.registerAdapter("mock", mockAdapter2, false);

        manager.shutdownAll();

        verify(mockAdapter1).shutdown();
        verify(mockAdapter2).shutdown();
        assertNull(manager.getAdapterByTopic(null));
        assertFalse(manager.healthCheckAll());
    }

    @Test
    public void testRouteTopicWithUnknownAdapter() {
        manager.registerAdapter("rocketmq", mockAdapter1, true);

        manager.routeTopic("TopicA", "nonexistent");

        assertEquals(mockAdapter1, manager.getAdapterByTopic("TopicA"));
    }
}