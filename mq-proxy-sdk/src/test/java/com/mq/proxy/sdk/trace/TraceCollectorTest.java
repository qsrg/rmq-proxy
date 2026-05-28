package com.mq.proxy.sdk.trace;

import com.mq.proxy.sdk.producer.ProxyProducerConfig;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TraceCollectorTest {
    
    private ProxyProducerConfig config;
    private TraceCollector collector;
    
    @Before
    public void setUp() {
        config = new ProxyProducerConfig();
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
        
        long countBefore = collector.getRecordedCount();
        collector.recordSendTrace(traceId, "TopicTest", "msg123", 
            System.currentTimeMillis(), true, null);
        long countAfter = collector.getRecordedCount();
        
        assertEquals("Recorded count should increase by 1", countBefore + 1, countAfter);
    }
    
    @Test
    public void testRecordSendTraceDisabled() {
        ProxyProducerConfig disabledConfig = new ProxyProducerConfig();
        disabledConfig.setEnableTrace(false);
        TraceCollector disabledCollector = new TraceCollector(disabledConfig);
        
        String traceId = disabledCollector.generateTraceId();
        
        disabledCollector.recordSendTrace(traceId, "TopicTest", "msg456", 
            System.currentTimeMillis(), true, null);
        
        assertEquals("When trace disabled, recorded count should be 0", 0, disabledCollector.getRecordedCount());
        
        disabledCollector.shutdown();
    }
    
    @Test
    public void testRecordSendTraceFailure() {
        String traceId = collector.generateTraceId();
        
        long countBefore = collector.getRecordedCount();
        collector.recordSendTrace(traceId, "TopicTest", "msg789", 
            System.currentTimeMillis(), false, "Send failed: timeout");
        long countAfter = collector.getRecordedCount();
        
        assertEquals("Failed trace should also be recorded", countBefore + 1, countAfter);
    }
}
