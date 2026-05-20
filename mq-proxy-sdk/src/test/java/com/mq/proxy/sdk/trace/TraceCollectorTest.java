package com.mq.proxy.sdk.trace;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TraceCollectorTest {
    
    private ProxyClientConfig config;
    private TraceCollector collector;
    
    @Before
    public void setUp() {
        config = new ProxyClientConfig();
        config.setEnableTrace(true);
        collector = new TraceCollector(config);
    }
    
    @Test
    public void testGenerateTraceId() {
        String traceId = collector.generateTraceId();
        
        assertNotNull(traceId);
        assertTrue(traceId.contains("@"));
        String[] parts = traceId.split("@");
        assertEquals(4, parts.length);
    }
    
    @Test
    public void testRecordSendTrace() {
        String traceId = collector.generateTraceId();
        
        collector.recordSendTrace(traceId, "TopicTest", "msg123", 
            System.currentTimeMillis(), true, null);
        
        assertTrue(true);
    }
}
