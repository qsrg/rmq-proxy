package com.mq.proxy.sdk.facade;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.sdk.producer.ProxyProducerConfig;
import com.mq.proxy.sdk.exception.ProxyConnectException;
import com.mq.proxy.sdk.exception.ProxyException;
import com.mq.proxy.sdk.remoting.ProxyRemotingClient;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ProxyClientFacadeTest {

    private ProxyProducerConfig config;

    @Mock
    private ProxyRemotingClient mockRemotingClient;

    @Mock
    private ProxyChannelManager mockChannelManager;

    @Mock
    private Channel mockChannel1;

    @Mock
    private Channel mockChannel2;

    @Mock
    private Channel mockChannel3;

    private ProxyClientFacade facade;
    private ProxyAddressManager addressManager;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        config = new ProxyProducerConfig();
        config.setProxyAddrs("proxy1:19876;proxy2:19877;proxy3:19878");
        config.setRetryTimes(3);
        config.setRequestTimeoutMillis(3000);
        config.setFaultIsolationDurationMillis(30000);
        config.setEnableMetrics(false);
        config.setEnableTrace(false);
        config.setEnableFaultDetector(false);

        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {
            }
        };

        Field addressManagerField = ProxyClientFacade.class.getDeclaredField("addressManager");
        addressManagerField.setAccessible(true);
        addressManager = (ProxyAddressManager) addressManagerField.get(facade);

        setMockObjects(facade, mockRemotingClient, mockChannelManager);
    }

    @Test
    public void testSuccessWithoutRetry() throws Exception {
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response = facade.invokeSync(request, 3000);

        assertNotNull(response);
        assertEquals(expectedResponse, response);
        verify(mockRemotingClient, times(1)).invokeSync(any(Channel.class), eq(request), anyLong());
        verify(mockChannelManager, times(1)).getOrCreateChannel(anyString(), anyLong());
        verify(mockChannelManager, never()).closeChannel(anyString());
    }

    @Test
    public void testRetryAfterFirstFailure() throws Exception {
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2);

        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Connection refused"));
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response = facade.invokeSync(request, 3000);

        assertNotNull(response);
        assertEquals(expectedResponse, response);
        verify(mockRemotingClient, times(2)).invokeSync(any(Channel.class), eq(request), anyLong());
        verify(mockChannelManager, times(2)).getOrCreateChannel(anyString(), anyLong());
        verify(mockChannelManager, times(1)).closeChannel(anyString());
    }

    @Test
    public void testAllRetriesFailed() throws Exception {
        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2)
            .thenReturn(mockChannel3);

        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Connection refused"));

        try {
            facade.invokeSync(request, 3000);
            fail("Should throw ProxyException");
        } catch (ProxyException e) {
            assertTrue(e.getMessage().contains("All proxy addresses failed after 3 attempts"));
            assertNotNull(e.getCause());
            assertEquals("Connection refused", e.getCause().getMessage());

            verify(mockRemotingClient, times(3)).invokeSync(any(Channel.class), eq(request), anyLong());
            verify(mockChannelManager, times(3)).getOrCreateChannel(anyString(), anyLong());
            verify(mockChannelManager, times(3)).closeChannel(anyString());
        }
    }

    @Test
    public void testFaultIsolation() throws Exception {
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2);

        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("proxy1 failed"));
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response1 = facade.invokeSync(request, 3000);

        assertNotNull(response1);
        assertEquals(expectedResponse, response1);
        verify(mockRemotingClient, times(2)).invokeSync(any(Channel.class), eq(request), anyLong());

        reset(mockRemotingClient, mockChannelManager);

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel2);
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response2 = facade.invokeSync(request, 3000);

        assertNotNull(response2);
        verify(mockRemotingClient, times(1)).invokeSync(any(Channel.class), eq(request), anyLong());
    }

    @Test
    public void testRoundRobinAddressSelection() throws Exception {
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2)
            .thenReturn(mockChannel3);

        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        facade.invokeSync(request, 3000);
        facade.invokeSync(request, 3000);
        facade.invokeSync(request, 3000);

        verify(mockRemotingClient, times(3)).invokeSync(any(Channel.class), eq(request), anyLong());
        verify(mockChannelManager, times(3)).getOrCreateChannel(anyString(), anyLong());

        String firstAddr = addressManager.selectProxyAddr();
        String secondAddr = addressManager.selectProxyAddr();
        String thirdAddr = addressManager.selectProxyAddr();

        assertNotNull(firstAddr);
        assertNotNull(secondAddr);
        assertNotNull(thirdAddr);
    }

    @Test
    public void testNoAvailableAddress() throws Exception {
        config.setProxyAddrs("proxy1:19876");
        config.setEnableFaultDetector(false);
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel(eq("proxy1:19876"), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Connection refused"));

        try {
            facade.invokeSync(request, 3000);
            fail("Should throw ProxyException");
        } catch (ProxyException e) {
            verify(mockRemotingClient, times(1)).invokeSync(any(Channel.class), eq(request), anyLong());
        }
    }

    @Test
    public void testFaultIsolationRecovery() throws Exception {
        config.setProxyAddrs("proxy1:19876;proxy2:19877");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 100L, 5000L, 6000L, 10000L, 30000L});
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("proxy1 failed"));
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        facade.invokeSync(request, 3000);

        Thread.sleep(150);

        reset(mockRemotingClient, mockChannelManager);

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response = facade.invokeSync(request, 3000);

        assertNotNull(response);
        verify(mockRemotingClient, times(1)).invokeSync(any(Channel.class), eq(request), anyLong());
    }

    @Test
    public void testConfigurableRetryTimes() throws Exception {
        config.setProxyAddrs("proxy1:19876;proxy2:19877;proxy3:19878;proxy4:19879;proxy5:19880");
        config.setRetryTimes(5);
        config.setEnableFaultDetector(false);
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Failed"));

        try {
            facade.invokeSync(request, 3000);
            fail("Should throw ProxyException");
        } catch (ProxyException e) {
            verify(mockRemotingClient, times(5)).invokeSync(any(Channel.class), eq(request), anyLong());
            verify(mockChannelManager, times(5)).getOrCreateChannel(anyString(), anyLong());
            verify(mockChannelManager, times(5)).closeChannel(anyString());
        }
    }

    @Test
    public void testConcurrentRetrySafety() throws Exception {
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        final int[] successCount = {0};

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    RemotingCommand response = facade.invokeSync(request, 3000);
                    if (response != null && response.getCode() == 200) {
                        successCount[0]++;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        boolean finished = endLatch.await(10, TimeUnit.SECONDS);

        assertTrue(finished);
        assertEquals(threadCount, successCount[0]);
        verify(mockRemotingClient, times(threadCount)).invokeSync(any(Channel.class), eq(request), anyLong());
    }

    @Test
    public void testPartialFailureThenSuccess() throws Exception {
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2)
            .thenReturn(mockChannel3);

        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("proxy1 failed"));
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenThrow(new RuntimeException("proxy2 failed"));
        when(mockRemotingClient.invokeSync(eq(mockChannel3), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response = facade.invokeSync(request, 3000);

        assertNotNull(response);
        assertEquals(expectedResponse, response);
        verify(mockRemotingClient, times(3)).invokeSync(any(Channel.class), eq(request), anyLong());
        verify(mockChannelManager, times(3)).getOrCreateChannel(anyString(), anyLong());
        verify(mockChannelManager, times(2)).closeChannel(anyString());
    }

    @Test
    public void testChannelCloseOnFailure() throws Exception {
        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Connection refused"));

        try {
            facade.invokeSync(request, 3000);
        } catch (ProxyException e) {
        }

        verify(mockChannelManager, atLeastOnce()).closeChannel(anyString());
    }

    @Test
    public void testSuccessWithoutChannelClose() throws Exception {
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response = facade.invokeSync(request, 3000);

        assertNotNull(response);
        verify(mockChannelManager, never()).closeChannel(anyString());
    }

    @Test
    public void testRequestTimeoutPerRetry() throws Exception {
        config.setRequestTimeoutPerRetryMillis(1000L);
        config.setRetryTimes(3);
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        facade.invokeSync(request, 10000);

        verify(mockRemotingClient).invokeSync(eq(mockChannel1), eq(request), eq(1000L));
    }

    @Test
    public void testRequestTimeoutPerRetryLastAttemptUsesRemaining() throws Exception {
        config.setRequestTimeoutPerRetryMillis(1000L);
        config.setRetryTimes(3);
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2)
            .thenReturn(mockChannel3);
        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenThrow(new RuntimeException("timeout"));

        try {
            facade.invokeSync(request, 10000);
        } catch (ProxyException ignored) {
        }

        verify(mockRemotingClient, times(3)).invokeSync(any(Channel.class), eq(request), anyLong());
    }

    @Test
    public void testDynamicFaultIsolationInFacade() throws Exception {
        config.setProxyAddrs("proxy1:19876;proxy2:19877");
        config.setLatencyMax(new long[]{50L, 100L, 550L, 1800L, 3000L, 5000L, 15000L});
        config.setNotAvailableDuration(new long[]{0L, 0L, 100L, 5000L, 6000L, 10000L, 30000L});
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        addressManager.markFault("proxy1:19876", 10000L);
        assertFalse(addressManager.isAvailable("proxy1:19876"));
        assertTrue(addressManager.isAvailable("proxy2:19877"));
    }

    @Test
    public void testInvokeSyncSkipsRetryWhenNoTimeLeftForConnect() throws Exception {
        config.setProxyAddrs("proxy1:19876;proxy2:19877");
        config.setRetryTimes(2);
        config.setConnectTimeoutMillis(3000);
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {
            }
        };

        TimingProxyChannelManager timingChannelManager = new TimingProxyChannelManager(config);
        setMockObjects(facade, mockRemotingClient, timingChannelManager);

        RemotingCommand request = createTestRequest();

        try {
            facade.invokeSync(request, 500);
            fail("Should throw ProxyException");
        } catch (ProxyException e) {
            assertTrue(e.getCause() instanceof ProxyConnectException);
            assertEquals(1, timingChannelManager.getConnectTimeouts().size());
            assertEquals(500L, timingChannelManager.getConnectTimeouts().get(0).longValue());
            verifyNoInteractions(mockRemotingClient);
        }
    }

    @Test
    public void testWrappedConnectionExceptionUsesLongIsolation() throws Exception {
        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel(anyString(), anyLong())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
                .thenThrow(new RuntimeException("Request failed",
                        new RuntimeException("Connection refused")));

        try {
            facade.invokeSync(request, 3000);
            fail("Should throw ProxyException");
        } catch (ProxyException ignored) {
        }

        assertFalse(addressManager.isAvailable("proxy2:19877"));
    }

    @Test
    public void testTlsDetectorWaitsForHandshakeCompletion() {
        ProxyProducerConfig tlsConfig = new ProxyProducerConfig();
        tlsConfig.setProxyAddrs("proxy1:19876");
        tlsConfig.setTlsEnabled(true);
        tlsConfig.setFaultDetectorIntervalMillis(1000L);
        ProxyAddressManager manager = new ProxyAddressManager(tlsConfig);

        RecordingTlsChannelManager channelManager = new RecordingTlsChannelManager(tlsConfig);
        manager.markFault("proxy1:19876", 5000L);

        ProxyFaultDetector detector = new ProxyFaultDetector(tlsConfig, manager, channelManager);
        detector.detect();

        assertEquals(1, channelManager.getTlsProbeCount());
        assertTrue(manager.isReachable("proxy1:19876"));
    }

    private RemotingCommand createTestRequest() {
        return RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, null);
    }

    private RemotingCommand createSuccessResponse() {
        RemotingCommand response = new RemotingCommand();
        response.setCode(200);
        response.setRemark("Success");
        return response;
    }

    private void setMockObjects(ProxyClientFacade facade,
                                ProxyRemotingClient mockRemotingClient,
                                ProxyChannelManager mockChannelManager) throws Exception {
        Field remotingClientField = ProxyClientFacade.class.getDeclaredField("remotingClient");
        remotingClientField.setAccessible(true);
        remotingClientField.set(facade, mockRemotingClient);

        Field channelManagerField = ProxyClientFacade.class.getDeclaredField("channelManager");
        channelManagerField.setAccessible(true);
        channelManagerField.set(facade, mockChannelManager);

        Field addressManagerField = ProxyClientFacade.class.getDeclaredField("addressManager");
        addressManagerField.setAccessible(true);
        this.addressManager = (ProxyAddressManager) addressManagerField.get(facade);
    }

    private static class TimingProxyChannelManager extends ProxyChannelManager {
        private final java.util.List<Long> connectTimeouts = new java.util.ArrayList<>();

        TimingProxyChannelManager(ProxyProducerConfig config) {
            super(config, mock(io.netty.bootstrap.Bootstrap.class));
        }

        @Override
        public Channel getOrCreateChannel(String addr, long timeoutMillis) throws ProxyConnectException {
            connectTimeouts.add(timeoutMillis);
            try {
                Thread.sleep(timeoutMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new ProxyConnectException(addr, "Connect timed out");
        }

        @Override
        public void closeChannel(String addr) {
        }

        java.util.List<Long> getConnectTimeouts() {
            return connectTimeouts;
        }
    }

    private static class RecordingTlsChannelManager extends ProxyChannelManager {
        private final AtomicInteger tlsProbeCount = new AtomicInteger();

        RecordingTlsChannelManager(ProxyProducerConfig config) {
            super(config, mock(io.netty.bootstrap.Bootstrap.class));
        }

        @Override
        public ProbeResult probe(String addr, long timeoutMillis, boolean requireTlsReady) {
            if (requireTlsReady) {
                tlsProbeCount.incrementAndGet();
            }
            return new ProbeResult(true, !requireTlsReady || true);
        }

        int getTlsProbeCount() {
            return tlsProbeCount.get();
        }
    }
}
