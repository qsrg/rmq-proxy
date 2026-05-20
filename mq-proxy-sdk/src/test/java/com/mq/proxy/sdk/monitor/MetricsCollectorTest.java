package com.mq.proxy.sdk.monitor;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class MetricsCollectorTest {
    
    private ProxyClientConfig config;
    private MetricsCollector collector;
    
    @Before
    public void setUp() {
        config = new ProxyClientConfig();
        config.setEnableMetrics(true);
        collector = new MetricsCollector(config);
    }
    
    @Test
    public void testRecordSuccess() {
        collector.recordSuccess("192.168.1.100:10911", 100);
        
        ProxyMetricsSnapshot snapshot = collector.getSnapshot().get("192.168.1.100:10911");
        
        assertNotNull(snapshot);
        assertEquals(1, snapshot.getSuccessCount());
        assertEquals(100, snapshot.getTotalElapsedMillis());
        assertEquals(100, snapshot.getAvgElapsedMillis());
    }
    
    @Test
    public void testRecordFailure() {
        collector.recordFailure("192.168.1.100:10911", new RuntimeException("test"));
        
        ProxyMetricsSnapshot snapshot = collector.getSnapshot().get("192.168.1.100:10911");
        
        assertNotNull(snapshot);
        assertEquals(1, snapshot.getFailureCount());
        assertEquals("RuntimeException", snapshot.getLastException());
    }
    
    @Test
    public void testSuccessRate() {
        collector.recordSuccess("192.168.1.100:10911", 100);
        collector.recordSuccess("192.168.1.100:10911", 200);
        collector.recordFailure("192.168.1.100:10911", new RuntimeException("test"));
        
        ProxyMetricsSnapshot snapshot = collector.getSnapshot().get("192.168.1.100:10911");
        
        assertEquals(2.0 / 3.0, snapshot.getSuccessRate(), 0.001);
    }
}
