package com.mq.proxy.sdk.facade;

import com.mq.proxy.sdk.producer.ProxyProducerConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ProxyAddressManagerTest {

    private ProxyProducerConfig config;

    @Before
    public void setUp() {
        config = new ProxyProducerConfig();
    }

    @Test
    public void testParseSingleAddress() {
        config.setProxyAddrs("192.168.1.100:19876");
        ProxyAddressManager manager = new ProxyAddressManager(config);

        String addr = manager.selectProxyAddr();
        assertEquals("192.168.1.100:19876", addr);
    }

    @Test
    public void testParseMultipleAddresses() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876;192.168.1.102:19876");
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
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setFaultIsolationDurationMillis(60000L);
        ProxyAddressManager manager = new ProxyAddressManager(config);

        String addr = manager.selectProxyAddr();
        manager.markFault(addr);

        String nextAddr = manager.selectProxyAddr();
        assertNotEquals(addr, nextAddr);
    }

    @Test
    public void testClearFault() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
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

    @Test
    public void testDynamicIsolationLowLatency() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L});
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876", 30L);

        assertTrue(manager.isAvailable("192.168.1.100:19876"));
    }

    @Test
    public void testZeroIsolationBucketRemainsAvailableAtThreshold() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L});
        config.setFaultIsolationDurationMillis(60000L);
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876", 50L);

        assertTrue(manager.isAvailable("192.168.1.100:19876"));
    }

    @Test
    public void testDynamicIsolationMediumLatency() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L});
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876", 1000L);

        assertFalse(manager.isAvailable("192.168.1.100:19876"));
    }

    @Test
    public void testDynamicIsolationHighLatency() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L});
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876", 20000L);

        assertFalse(manager.isAvailable("192.168.1.100:19876"));
    }

    @Test
    public void testDynamicIsolationExpires() throws Exception {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 100L, 5000L, 6000L, 10000L, 30000L});
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876", 1000L);

        assertFalse(manager.isAvailable("192.168.1.100:19876"));

        Thread.sleep(150);

        assertTrue(manager.isAvailable("192.168.1.100:19876"));
    }

    @Test
    public void testMarkFaultWithoutLatencyUsesFaultIsolationDuration() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setFaultIsolationDurationMillis(60000L);
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876");

        assertFalse(manager.isAvailable("192.168.1.100:19876"));
    }

    @Test
    public void testReachableFlag() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        ProxyAddressManager manager = new ProxyAddressManager(config);

        assertTrue(manager.isReachable("192.168.1.100:19876"));

        manager.markFault("192.168.1.100:19876", 5000L);

        assertFalse(manager.isReachable("192.168.1.100:19876"));

        manager.markReachable("192.168.1.100:19876");

        assertTrue(manager.isReachable("192.168.1.100:19876"));
    }

    @Test
    public void testClearFaultOnSuccess() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876", 5000L);
        assertFalse(manager.isAvailable("192.168.1.100:19876"));

        manager.clearFault("192.168.1.100:19876");
        assertTrue(manager.isAvailable("192.168.1.100:19876"));
        assertTrue(manager.isReachable("192.168.1.100:19876"));
    }

    @Test
    public void testFaultItemIsolationDurationUpgrade() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L});
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876", 1000L);

        manager.markFault("192.168.1.100:19876", 20000L);

        assertFalse(manager.isAvailable("192.168.1.100:19876"));
    }

    @Test
    public void testReachableButIsolatedSelectedAsFallback() {
        config.setProxyAddrs("192.168.1.100:19876;192.168.1.101:19876");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 2000L, 5000L, 6000L, 10000L, 30000L});
        ProxyAddressManager manager = new ProxyAddressManager(config);

        manager.markFault("192.168.1.100:19876", 5000L);
        manager.markFault("192.168.1.101:19876", 5000L);

        assertNull(manager.selectProxyAddr());

        manager.markReachable("192.168.1.100:19876");

        String addr = manager.selectProxyAddr();
        assertNotNull(addr);
        assertEquals("192.168.1.100:19876", addr);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateLatencyConfigNull() {
        config.setLatencyMax(null);
        new ProxyAddressManager(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateLatencyConfigLengthMismatch() {
        config.setLatencyMax(new long[]{50L, 100L});
        config.setNotAvailableDuration(new long[]{0L});
        new ProxyAddressManager(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateLatencyConfigEmpty() {
        config.setLatencyMax(new long[]{});
        config.setNotAvailableDuration(new long[]{});
        new ProxyAddressManager(config);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testValidateLatencyConfigNotNonDecreasing() {
        config.setLatencyMax(new long[]{100L, 50L});
        config.setNotAvailableDuration(new long[]{0L, 1000L});
        new ProxyAddressManager(config);
    }
}
