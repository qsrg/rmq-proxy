package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class ProxyAddressManagerTest {
    
    private ProxyClientConfig config;
    
    @Before
    public void setUp() {
        config = new ProxyClientConfig();
    }
    
    @Test
    public void testParseSingleAddress() {
        config.setProxyAddrs("192.168.1.100:10911");
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr = manager.selectProxyAddr();
        assertEquals("192.168.1.100:10911", addr);
    }
    
    @Test
    public void testParseMultipleAddresses() {
        config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911;192.168.1.102:10911");
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr1 = manager.selectProxyAddr();
        String addr2 = manager.selectProxyAddr();
        String addr3 = manager.selectProxyAddr();
        
        assertNotNull(addr1);
        assertNotNull(addr2);
        assertNotNull(addr3);
    }
    
    @Test
    public void testMarkFault() {
        config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911");
        config.setFaultIsolationDurationMillis(60000L);
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr = manager.selectProxyAddr();
        manager.markFault(addr);
        
        String nextAddr = manager.selectProxyAddr();
        assertNotEquals(addr, nextAddr);
    }
    
    @Test
    public void testClearFault() {
        config.setProxyAddrs("192.168.1.100:10911;192.168.1.101:10911");
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr = manager.selectProxyAddr();
        manager.markFault(addr);
        
        String nextAddr1 = manager.selectProxyAddr();
        assertNotEquals(addr, nextAddr1);
        
        manager.clearFault(addr);
        
        String nextAddr2 = manager.selectProxyAddr();
        assertNotNull(nextAddr2);
    }
    
    @Test
    public void testEmptyAddressList() {
        config.setProxyAddrs("");
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr = manager.selectProxyAddr();
        assertNull(addr);
    }
}
