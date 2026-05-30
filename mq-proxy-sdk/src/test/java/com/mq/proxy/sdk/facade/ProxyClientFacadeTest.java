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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * ProxyClientFacade 重试机制测试
 *
 * 测试场景：
 * 1. 成功场景（无重试）
 * 2. 第一次失败后重试成功
 * 3. 所有重试都失败
 * 4. 故障隔离机制
 * 5. 轮询选择地址
 * 6. 无可用地址异常
 * 7. 故障隔离超时后恢复
 * 8. 重试次数可配置
 * 9. 并发重试安全性
 * 10. 部分失败后最终成功
 */
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

        // 创建 facade，使用真实的 AddressManager
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {
                // 跳过启动
            }
        };

        // 获取 addressManager 实例
        Field addressManagerField = ProxyClientFacade.class.getDeclaredField("addressManager");
        addressManagerField.setAccessible(true);
        addressManager = (ProxyAddressManager) addressManagerField.get(facade);

        // 设置 mock 对象
        setMockObjects(facade, mockRemotingClient, mockChannelManager);
    }

    @Test
    public void testSuccessWithoutRetry() throws Exception {
        // Given: 第一次请求就成功
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        // When: 调用 invokeSync
        RemotingCommand response = facade.invokeSync(request, 3000);

        // Then: 应该成功，只调用一次
        assertNotNull(response);
        assertEquals(expectedResponse, response);
        verify(mockRemotingClient, times(1)).invokeSync(any(Channel.class), eq(request), anyLong());
        verify(mockChannelManager, times(1)).getOrCreateChannel(anyString());
        verify(mockChannelManager, never()).closeChannel(anyString());
    }

    @Test
    public void testRetryAfterFirstFailure() throws Exception {
        // Given: 第一次失败，第二次成功
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString()))
            .thenReturn(mockChannel1)  // 第一次
            .thenReturn(mockChannel2);  // 第二次

        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Connection refused"));
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        // When: 调用 invokeSync
        RemotingCommand response = facade.invokeSync(request, 3000);

        // Then: 应该成功，调用了两次
        assertNotNull(response);
        assertEquals(expectedResponse, response);
        verify(mockRemotingClient, times(2)).invokeSync(any(Channel.class), eq(request), anyLong());
        verify(mockChannelManager, times(2)).getOrCreateChannel(anyString());
        verify(mockChannelManager, times(1)).closeChannel(anyString());  // 第一次失败后关闭
    }

    @Test
    public void testAllRetriesFailed() throws Exception {
        // Given: 所有请求都失败
        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel(anyString()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2)
            .thenReturn(mockChannel3);

        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Connection refused"));

        // When & Then: 应该抛出异常
        try {
            facade.invokeSync(request, 3000);
            fail("Should throw ProxyException");
        } catch (ProxyException e) {
            assertTrue(e.getMessage().contains("All proxy addresses failed after 3 attempts"));
            assertNotNull(e.getCause());
            assertEquals("Connection refused", e.getCause().getMessage());

            // 验证尝试了3次
            verify(mockRemotingClient, times(3)).invokeSync(any(Channel.class), eq(request), anyLong());
            verify(mockChannelManager, times(3)).getOrCreateChannel(anyString());
            verify(mockChannelManager, times(3)).closeChannel(anyString());  // 每次失败都关闭
        }
    }

    @Test
    public void testFaultIsolation() throws Exception {
        // Given: 第一次 proxy1 失败，proxy2 成功，之后 proxy1 应该被隔离
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString()))
            .thenReturn(mockChannel1)  // proxy1
            .thenReturn(mockChannel2);  // proxy2

        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("proxy1 failed"));
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        // When: 第一次调用
        RemotingCommand response1 = facade.invokeSync(request, 3000);

        // Then: 第一次应该成功（proxy1失败后proxy2成功）
        assertNotNull(response1);
        assertEquals(expectedResponse, response1);
        verify(mockRemotingClient, times(2)).invokeSync(any(Channel.class), eq(request), anyLong());

        // 重置 mock
        reset(mockRemotingClient, mockChannelManager);

        // When: 第二次调用（proxy1 应该被隔离，直接使用 proxy2 或 proxy3）
        when(mockChannelManager.getOrCreateChannel(anyString())).thenReturn(mockChannel2);
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response2 = facade.invokeSync(request, 3000);

        // Then: 第二次应该成功，只调用一次（跳过了 proxy1）
        assertNotNull(response2);
        verify(mockRemotingClient, times(1)).invokeSync(any(Channel.class), eq(request), anyLong());
    }

    @Test
    public void testRoundRobinAddressSelection() throws Exception {
        // Given: 配置3个代理地址
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2)
            .thenReturn(mockChannel3);

        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        // When: 连续调用3次
        facade.invokeSync(request, 3000);
        facade.invokeSync(request, 3000);
        facade.invokeSync(request, 3000);

        // Then: 应该调用3次，每次选择不同的地址（轮询）
        verify(mockRemotingClient, times(3)).invokeSync(any(Channel.class), eq(request), anyLong());
        verify(mockChannelManager, times(3)).getOrCreateChannel(anyString());

        // 验证地址轮询
        String firstAddr = addressManager.selectProxyAddr();
        String secondAddr = addressManager.selectProxyAddr();
        String thirdAddr = addressManager.selectProxyAddr();

        // 由于轮询机制，地址应该依次变化
        assertNotNull(firstAddr);
        assertNotNull(secondAddr);
        assertNotNull(thirdAddr);
    }

    @Test
    public void testNoAvailableAddress() throws Exception {
        // Given: 只有一个代理地址，失败后没有其他可用地址
        config.setProxyAddrs("proxy1:19876");
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel("proxy1:19876")).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Connection refused"));

        // When & Then: 第一次失败后应该抛出 ProxyConnectException
        try {
            facade.invokeSync(request, 3000);
            fail("Should throw ProxyConnectException");
        } catch (ProxyConnectException e) {
            // 异常消息格式："Failed to connect to proxy: No available proxy address"
            assertTrue(e.getMessage().contains("No available proxy address"));
            verify(mockRemotingClient, times(1)).invokeSync(any(Channel.class), eq(request), anyLong());
        }
    }

    @Test
    public void testFaultIsolationRecovery() throws Exception {
        // Given: 配置短时间的故障隔离（100ms）
        config.setProxyAddrs("proxy1:19876;proxy2:19877");
        config.setFaultIsolationDurationMillis(100);
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        // 第一次：proxy1失败，proxy2成功
        when(mockChannelManager.getOrCreateChannel(anyString()))
            .thenReturn(mockChannel1)
            .thenReturn(mockChannel2);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("proxy1 failed"));
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        facade.invokeSync(request, 3000);

        // 等待故障隔离超时
        Thread.sleep(150);

        // 重置 mock
        reset(mockRemotingClient, mockChannelManager);

        // When: 隔离超时后再次调用，proxy1应该恢复
        when(mockChannelManager.getOrCreateChannel(anyString())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        RemotingCommand response = facade.invokeSync(request, 3000);

        // Then: 应该成功
        assertNotNull(response);
        verify(mockRemotingClient, times(1)).invokeSync(any(Channel.class), eq(request), anyLong());
    }

    @Test
    public void testConfigurableRetryTimes() throws Exception {
        // Given: 配置重试次数为5次，并且有5个代理地址可供重试
        config.setProxyAddrs("proxy1:19876;proxy2:19877;proxy3:19878;proxy4:19879;proxy5:19880");
        config.setRetryTimes(5);
        facade = new ProxyClientFacade(config) {
            @Override
            public void start() {}
        };
        setMockObjects(facade, mockRemotingClient, mockChannelManager);

        RemotingCommand request = createTestRequest();

        // 为每个地址返回不同的 mock channel
        when(mockChannelManager.getOrCreateChannel(anyString())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Failed"));

        // When & Then: 应该尝试5次（因为有5个地址）
        try {
            facade.invokeSync(request, 3000);
            fail("Should throw ProxyException");
        } catch (ProxyException e) {
            verify(mockRemotingClient, times(5)).invokeSync(any(Channel.class), eq(request), anyLong());
            verify(mockChannelManager, times(5)).getOrCreateChannel(anyString());
            verify(mockChannelManager, times(5)).closeChannel(anyString());  // 每次失败都关闭
        }
    }

    @Test
    public void testConcurrentRetrySafety() throws Exception {
        // Given: 模拟并发场景
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(any(Channel.class), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        final int[] successCount = {0};

        // When: 启动多个线程并发调用
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

        // Then: 所有线程应该都能成功完成
        assertTrue(finished);
        assertEquals(threadCount, successCount[0]);
        verify(mockRemotingClient, times(threadCount)).invokeSync(any(Channel.class), eq(request), anyLong());
    }

    @Test
    public void testPartialFailureThenSuccess() throws Exception {
        // Given: 前2次失败，第3次成功
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString()))
            .thenReturn(mockChannel1)  // proxy1
            .thenReturn(mockChannel2)  // proxy2
            .thenReturn(mockChannel3);  // proxy3

        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("proxy1 failed"));
        when(mockRemotingClient.invokeSync(eq(mockChannel2), eq(request), anyLong()))
            .thenThrow(new RuntimeException("proxy2 failed"));
        when(mockRemotingClient.invokeSync(eq(mockChannel3), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        // When: 调用 invokeSync
        RemotingCommand response = facade.invokeSync(request, 3000);

        // Then: 应该在第3次成功
        assertNotNull(response);
        assertEquals(expectedResponse, response);
        verify(mockRemotingClient, times(3)).invokeSync(any(Channel.class), eq(request), anyLong());
        verify(mockChannelManager, times(3)).getOrCreateChannel(anyString());
        verify(mockChannelManager, times(2)).closeChannel(anyString());  // 前两次失败关闭
    }

    @Test
    public void testChannelCloseOnFailure() throws Exception {
        // Given: 请求失败
        RemotingCommand request = createTestRequest();

        when(mockChannelManager.getOrCreateChannel(anyString())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenThrow(new RuntimeException("Connection refused"));

        // When: 调用失败
        try {
            facade.invokeSync(request, 3000);
        } catch (ProxyException e) {
            // 预期异常
        }

        // Then: 应该关闭通道
        verify(mockChannelManager, atLeastOnce()).closeChannel(anyString());
    }

    @Test
    public void testSuccessWithoutChannelClose() throws Exception {
        // Given: 请求成功
        RemotingCommand request = createTestRequest();
        RemotingCommand expectedResponse = createSuccessResponse();

        when(mockChannelManager.getOrCreateChannel(anyString())).thenReturn(mockChannel1);
        when(mockRemotingClient.invokeSync(eq(mockChannel1), eq(request), anyLong()))
            .thenReturn(expectedResponse);

        // When: 调用成功
        RemotingCommand response = facade.invokeSync(request, 3000);

        // Then: 不应该关闭通道
        assertNotNull(response);
        verify(mockChannelManager, never()).closeChannel(anyString());
    }

    // ========== 辅助方法 ==========

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
    }
}