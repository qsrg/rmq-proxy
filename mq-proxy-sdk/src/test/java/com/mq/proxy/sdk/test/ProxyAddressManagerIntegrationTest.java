package com.mq.proxy.sdk.test;

import com.mq.proxy.sdk.facade.ProxyAddressManager;
import com.mq.proxy.sdk.client.ProxyClientConfig;
import org.junit.Test;
import static org.junit.Assert.*;

public class ProxyAddressManagerIntegrationTest {
    
    @Test
    public void testAddressManagerBasic() {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911;127.0.0.1:11912;127.0.0.1:11913");
        config.setFaultIsolationDurationMillis(30000);
        
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        assertEquals(3, manager.getProxyAddrList().size());
        
        String addr = manager.selectProxyAddr();
        assertNotNull(addr);
        assertTrue(manager.getProxyAddrList().contains(addr));
    }
    
    @Test
    public void testFaultIsolation() {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911;127.0.0.1:11912");
        config.setFaultIsolationDurationMillis(60000);
        
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr1 = manager.selectProxyAddr();
        manager.markFault(addr1);
        
        String addr2 = manager.selectProxyAddr();
        assertNotNull(addr2);
        assertNotEquals(addr1, addr2);
    }
    
    @Test
    public void testFaultRecovery() {
        ProxyClientConfig config = new ProxyClientConfig();
        config.setProxyAddrs("127.0.0.1:11911;127.0.0.1:11912");
        config.setFaultIsolationDurationMillis(1000);
        
        ProxyAddressManager manager = new ProxyAddressManager(config);
        
        String addr1 = manager.selectProxyAddr();
        manager.markFault(addr1);
        
        String addr2 = manager.selectProxyAddr();
        assertNotEquals(addr1, addr2);
        
        manager.clearFault(addr1);
        
        String addr3 = manager.selectProxyAddr();
        assertEquals(addr1, addr3);
    }
}