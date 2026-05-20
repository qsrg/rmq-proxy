package com.mq.proxy.sdk.client;

import org.junit.Test;
import static org.junit.Assert.*;

public class ProxyClientConfigTest {
    
    @Test
    public void testDefaultConfig() {
        ProxyClientConfig config = new ProxyClientConfig();
        
        assertEquals("127.0.0.1:10911", config.getProxyAddrs());
        assertEquals(3000, config.getConnectTimeoutMillis());
        assertEquals(3000, config.getRequestTimeoutMillis());
        assertEquals(3, config.getRetryTimes());
        assertEquals(30000L, config.getFaultIsolationDurationMillis());
        assertTrue(config.isEnableMetrics());
        assertTrue(config.isEnableTrace());
    }
    
    @Test
    public void testSetProxyAddrs() {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911");
        
        assertEquals("192.168.1.100:10911;192.168.1.101:10911", config.getProxyAddrs());
    }
    
    @Test
    public void testSetRetryTimes() {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setRetryTimes(5);
        
        assertEquals(5, config.getRetryTimes());
    }
}
