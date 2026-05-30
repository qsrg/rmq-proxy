package com.mq.proxy.sdk.producer;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.SendMessageResponseHeader;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * ProxyProducer协议测试 - 验证请求构建和响应解析
 */
public class ProxyProducerTest {

    private ProxyClientFacade mockFacade;
    private ProxyProducer producer;

    @Before
    public void setUp() {
        mockFacade = mock(ProxyClientFacade.class);

        // Mock TraceCollector和MetricsCollector
        com.mq.proxy.sdk.trace.TraceCollector mockTraceCollector = mock(com.mq.proxy.sdk.trace.TraceCollector.class);
        when(mockFacade.getTraceCollector()).thenReturn(mockTraceCollector);
        when(mockTraceCollector.generateTraceId()).thenReturn("trace123");

        com.mq.proxy.sdk.monitor.MetricsCollector mockMetricsCollector = mock(com.mq.proxy.sdk.monitor.MetricsCollector.class);
        when(mockFacade.getMetricsCollector()).thenReturn(mockMetricsCollector);

        producer = new ProxyProducer("TestProducerGroup");
        producer.getConfig().setEnableTrace(false); // 禁用trace避免调用getTraceCollector

        // 手动注入mock facade（测试用）
        try {
            java.lang.reflect.Field facadeField = ProxyProducer.class.getDeclaredField("facade");
            facadeField.setAccessible(true);
            facadeField.set(producer, mockFacade);

            java.lang.reflect.Field startedField = ProxyProducer.class.getDeclaredField("started");
            startedField.setAccessible(true);
            startedField.set(producer, true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSimplifiedAPI() {
        // 测试简化API（链式配置）
        ProxyProducer producer = new ProxyProducer("GroupA")
            .setProxyAddrs("127.0.0.1:19876")
            .setRetryTimes(3)
            .setEnableTrace(true);

        assertNotNull(producer);
        assertEquals("GroupA", producer.getConfig().getProducerGroup());
        assertEquals("127.0.0.1:19876", producer.getConfig().getProxyAddrs());
        assertEquals(3, producer.getConfig().getRetryTimes());
        assertTrue(producer.getConfig().isEnableTrace());
    }

    @Test
    public void testSendRequestContainsRequiredFields() throws Exception {
        RemotingCommand successResponse = buildSendSuccessResponse("msg123", 0, 100L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(successResponse);

        producer.send("TestTopic", "TagA", "Key1", "hello".getBytes());

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        RemotingCommand request = requestCaptor.getValue();
        HashMap<String, String> extFields = request.getExtFields();

        assertEquals(RequestCode.SEND_MESSAGE, request.getCode());
        assertEquals("TestTopic", extFields.get("topic"));
        assertEquals("TestProducerGroup", extFields.get("producerGroup"));
        assertNotNull(extFields.get("defaultTopic"));
        assertNotNull(request.getBody());
    }

    @Test
    public void testParseSendResultSuccess() throws Exception {
        RemotingCommand response = buildSendSuccessResponse("msg123", 3, 500L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        SendResult result = producer.send("TestTopic", "TagA", "hello".getBytes());

        assertTrue(result.isSuccess());
        assertEquals("msg123", result.getMsgId());
        assertEquals(3, result.getQueueId());
        assertEquals(500L, result.getQueueOffset());
    }

    @Test
    public void testParseSendResultFailure() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(
                RemotingSysResponseCode.SYSTEM_ERROR, "broker busy");
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        SendResult result = producer.send("TestTopic", "TagA", "hello".getBytes());

        assertFalse(result.isSuccess());
        assertEquals("broker busy", result.getErrorMsg());
    }

    @Test
    public void testSendWithQueueId() throws Exception {
        RemotingCommand successResponse = buildSendSuccessResponse("msg456", 2, 300L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(successResponse);

        SendResult result = producer.send("OrderlyTopic", "TagA", "Order_001", "order step 0".getBytes(), 0, 2);

        assertTrue(result.isSuccess());
        assertEquals("msg456", result.getMsgId());
        assertEquals(2, result.getQueueId());
        assertEquals(300L, result.getQueueOffset());

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        RemotingCommand request = requestCaptor.getValue();
        HashMap<String, String> extFields = request.getExtFields();
        assertEquals("2", extFields.get("queueId"));
    }

    private RemotingCommand buildSendSuccessResponse(String msgId, int queueId, long queueOffset) {
        SendMessageResponseHeader header = new SendMessageResponseHeader();
        header.setMsgId(msgId);
        header.setQueueId(queueId);
        header.setQueueOffset(queueOffset);
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setCustomHeader(header);
        response.makeCustomHeaderToNet();
        return response;
    }
}