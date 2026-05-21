package com.mq.proxy.sdk.client;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.SendMessageResponseHeader;
import com.mq.proxy.core.protocol.header.PullMessageResponseHeader;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetResponseHeader;
import com.mq.proxy.sdk.exception.ProxyException;
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
 * SDK协议层单元测试 — 验证ProxyClient请求构建和响应解析的协议正确性。
 * 对照RocketMQ 4.9.8源码的协议规范，验证修复后的实现。
 */
public class ProxyClientProtocolTest {

    private ProxyClientFacade mockFacade;
    private ProxyClientConfig config;
    private ProxyClient client;

    @Before
    public void setUp() {
        mockFacade = mock(ProxyClientFacade.class);
        config = new ProxyClientConfig();
        config.setEnableTrace(false);
        client = new ProxyClient(config, mockFacade);
    }

    // ========== SendMessage 请求构建 ==========

    @Test
    public void testSendRequestContainsRequiredFields() throws Exception {
        RemotingCommand successResponse = buildSendSuccessResponse("msg123", 0, 100L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(successResponse);

        client.send("TestTopic", "TagA", "Key1", "hello".getBytes());

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        RemotingCommand request = requestCaptor.getValue();
        HashMap<String, String> extFields = request.getExtFields();

        assertEquals(RequestCode.SEND_MESSAGE, request.getCode());
        assertEquals("TestTopic", extFields.get("topic"));
        assertEquals("TBW102", extFields.get("defaultTopic"));
        assertNotNull(extFields.get("defaultTopicQueueNums"));
        assertNotNull(extFields.get("queueId"));
        assertNotNull(extFields.get("sysFlag"));
        assertNotNull(extFields.get("bornTimestamp"));
        assertNotNull(extFields.get("flag"));
        assertNotNull(extFields.get("properties"));
        assertNotNull(request.getBody());
    }

    /**
     * 修复验证: producerGroup可通过ProxyClientConfig配置，不再硬编码。
     */
    @Test
    public void testSendRequestProducerGroupConfigurable() throws Exception {
        config.setProducerGroup("MyAppProducer");
        RemotingCommand successResponse = buildSendSuccessResponse("msg123", 0, 100L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(successResponse);

        client = new ProxyClient(config, mockFacade);
        client.send("TestTopic", "TagA", "hello".getBytes());

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        assertEquals("MyAppProducer", requestCaptor.getValue().getExtFields().get("producerGroup"));
    }

    @Test
    public void testSendRequestProducerGroupDefaultValue() throws Exception {
        RemotingCommand successResponse = buildSendSuccessResponse("msg123", 0, 100L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(successResponse);

        client.send("TestTopic", "TagA", "hello".getBytes());

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        assertEquals("SDKProducerGroup", requestCaptor.getValue().getExtFields().get("producerGroup"));
    }

    /**
     * 验证properties字符串格式与RocketMQ MessageDecoder.messageProperties2String一致:
     * 格式: name +  + value +  + name +  + value (最后一个被删除)
     */
    @Test
    public void testSendRequestPropertiesFormat() throws Exception {
        RemotingCommand successResponse = buildSendSuccessResponse("msg123", 0, 100L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(successResponse);

        client.send("TestTopic", "TagA", "Key1", "hello".getBytes());

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        String properties = requestCaptor.getValue().getExtFields().get("properties");

        String expected = "TAGS" + (char) 1 + "TagA" + (char) 2 + "KEYS" + (char) 1 + "Key1";
        assertEquals(expected, properties);
    }

    // ========== SendMessage 响应解析 ==========

    @Test
    public void testParseSendResultSuccess() throws Exception {
        RemotingCommand response = buildSendSuccessResponse("msg123", 3, 500L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        SendResult result = client.send("TestTopic", "TagA", "hello".getBytes());

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

        SendResult result = client.send("TestTopic", "TagA", "hello".getBytes());

        assertFalse(result.isSuccess());
        assertEquals("broker busy", result.getErrorMsg());
    }

    // ========== PullMessage 请求构建 ==========

    @Test
    public void testPullRequestContainsRequiredFields() throws Exception {
        RemotingCommand pullResponse = buildPullSuccessResponse(100L, 0L, 200L, 0L, new byte[]{1, 2, 3});
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(pullResponse);

        client.pull("TestTopic", "GroupA", 0, 50L, 32);

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        RemotingCommand request = requestCaptor.getValue();
        HashMap<String, String> extFields = request.getExtFields();

        assertEquals(RequestCode.PULL_MESSAGE, request.getCode());
        assertEquals("GroupA", extFields.get("consumerGroup"));
        assertEquals("TestTopic", extFields.get("topic"));
        assertEquals("0", extFields.get("queueId"));
        assertEquals("50", extFields.get("queueOffset"));
        assertEquals("32", extFields.get("maxMsgNums"));
        assertNotNull(extFields.get("sysFlag"));
        assertNotNull(extFields.get("commitOffset"));
        assertNotNull(extFields.get("suspendTimeoutMillis"));
        assertNotNull(extFields.get("subVersion"));
        assertEquals("*", extFields.get("subscription"));
        assertEquals("TAG", extFields.get("expressionType"));
    }

    /**
     * 默认suspendTimeoutMillis=0 → sysFlag=0(无SUSPEND位) → broker不等待，立即返回。
     */
    @Test
    public void testPullRequestNoSuspendByDefault() throws Exception {
        assertEquals(0L, config.getSuspendTimeoutMillis());

        RemotingCommand pullResponse = buildPullSuccessResponse(100L, 0L, 200L, 0L, new byte[]{1, 2, 3});
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(pullResponse);

        client.pull("TestTopic", "GroupA", 0, 50L, 32);

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        assertEquals("0", requestCaptor.getValue().getExtFields().get("suspendTimeoutMillis"));
        assertEquals("0", requestCaptor.getValue().getExtFields().get("sysFlag"));
    }

    /**
     * 设置suspendTimeoutMillis=15000 → sysFlag=0x02(SUSPEND位) → broker启用长轮询。
     * SDK的pull超时自动调整为 suspendTimeout + requestTimeout，确保不提前断开。
     */
    @Test
    public void testPullRequestSuspendEnabled() throws Exception {
        config.setSuspendTimeoutMillis(15000L);

        RemotingCommand pullResponse = buildPullSuccessResponse(100L, 0L, 200L, 0L, new byte[]{1, 2, 3});
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(pullResponse);

        client = new ProxyClient(config, mockFacade);
        client.pull("TestTopic", "GroupA", 0, 50L, 32);

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        assertEquals("15000", requestCaptor.getValue().getExtFields().get("suspendTimeoutMillis"));
        assertEquals("2", requestCaptor.getValue().getExtFields().get("sysFlag"));
    }

    /**
     * 验证长轮询时pull超时自动扩容: pullTimeout = suspendTimeout + requestTimeout。
     */
    @Test
    public void testPullTimeoutExpandedWhenSuspendEnabled() throws Exception {
        config.setSuspendTimeoutMillis(15000L);
        config.setRequestTimeoutMillis(3000);

        RemotingCommand pullResponse = buildPullSuccessResponse(100L, 0L, 200L, 0L, new byte[]{1, 2, 3});
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(pullResponse);

        client = new ProxyClient(config, mockFacade);
        client.pull("TestTopic", "GroupA", 0, 50L, 32);

        ArgumentCaptor<Long> timeoutCaptor = ArgumentCaptor.forClass(Long.class);
        verify(mockFacade).invokeSync(any(RemotingCommand.class), timeoutCaptor.capture());

        assertEquals(18000L, (long) timeoutCaptor.getValue());
    }

    /**
     * 默认不启用长轮询时，pull超时 = requestTimeout，不额外等待。
     */
    @Test
    public void testPullTimeoutNormalWhenNoSuspend() throws Exception {
        config.setRequestTimeoutMillis(3000);

        RemotingCommand pullResponse = buildPullSuccessResponse(100L, 0L, 200L, 0L, new byte[]{1, 2, 3});
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(pullResponse);

        client.pull("TestTopic", "GroupA", 0, 50L, 32);

        ArgumentCaptor<Long> timeoutCaptor = ArgumentCaptor.forClass(Long.class);
        verify(mockFacade).invokeSync(any(RemotingCommand.class), timeoutCaptor.capture());

        assertEquals(3000L, (long) timeoutCaptor.getValue());
    }

    // ========== PullMessage 响应解析 ==========

    @Test
    public void testParsePullResultSuccess() throws Exception {
        byte[] body = new byte[]{1, 2, 3, 4};
        RemotingCommand response = buildPullSuccessResponse(100L, 0L, 200L, 1L, body);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        PullResult result = client.pull("TestTopic", "GroupA", 0, 50L, 32);

        assertTrue(result.isSuccess());
        assertTrue(result.isFound());
        assertEquals(RemotingSysResponseCode.SUCCESS, result.getResponseCode());
        assertEquals(100L, result.getNextBeginOffset());
        assertEquals(0L, result.getMinOffset());
        assertEquals(200L, result.getMaxOffset());
        assertEquals(1L, result.getSuggestWhichBrokerId());
        assertArrayEquals(body, result.getBody());
    }

    /**
     * 修复验证: PULL_NOT_FOUND使用found=false区分"成功但无数据"和"成功有数据"。
     */
    @Test
    public void testParsePullResultNotFoundFoundFalse() throws Exception {
        RemotingCommand response = buildPullNotFoundResponse(55L, 0L, 200L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        PullResult result = client.pull("TestTopic", "GroupA", 0, 50L, 32);

        assertTrue(result.isSuccess());
        assertFalse(result.isFound());
        assertEquals(ResponseCode.PULL_NOT_FOUND, result.getResponseCode());
        assertNull(result.getBody());
        assertEquals(55L, result.getNextBeginOffset());
    }

    /**
     * 修复验证: PullResult现在包含suggestWhichBrokerId字段。
     */
    @Test
    public void testParsePullResultSuggestWhichBrokerId() throws Exception {
        RemotingCommand response = buildPullSuccessResponse(100L, 0L, 200L, 1L, new byte[]{1, 2, 3});
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        PullResult result = client.pull("TestTopic", "GroupA", 0, 50L, 32);

        assertEquals(1L, result.getSuggestWhichBrokerId());
    }

    @Test
    public void testParsePullResultError() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(
                ResponseCode.TOPIC_NOT_EXIST, "topic not exist");
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        PullResult result = client.pull("TestTopic", "GroupA", 0, 50L, 32);

        assertFalse(result.isSuccess());
        assertFalse(result.isFound());
        assertEquals(ResponseCode.TOPIC_NOT_EXIST, result.getResponseCode());
        assertEquals("topic not exist", result.getErrorMsg());
    }

    // ========== QueryConsumerOffset ==========

    @Test
    public void testQueryConsumerOffsetSuccess() throws Exception {
        RemotingCommand response = buildQueryOffsetSuccessResponse(500L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        long offset = client.queryConsumerOffset("GroupA", "TestTopic", 0);

        assertEquals(500L, offset);
    }

    /**
     * 修复验证: queryConsumerOffset对206(QUERY_NOT_FOUND)返回-1而非抛异常。
     * 新消费者首次查询offset时broker返回206是正常行为。
     */
    @Test
    public void testQueryConsumerOffsetNotFound206ReturnsMinusOne() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(206, "query not found");
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        long offset = client.queryConsumerOffset("GroupA", "NewTopic", 0);

        assertEquals(-1L, offset);
    }

    @Test
    public void testQueryConsumerOffsetRequestCode() throws Exception {
        RemotingCommand response = buildQueryOffsetSuccessResponse(0L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        client.queryConsumerOffset("GroupA", "TestTopic", 0);

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        assertEquals(RequestCode.QUERY_CONSUMER_OFFSET, requestCaptor.getValue().getCode());
        HashMap<String, String> extFields = requestCaptor.getValue().getExtFields();
        assertEquals("GroupA", extFields.get("consumerGroup"));
        assertEquals("TestTopic", extFields.get("topic"));
        assertEquals("0", extFields.get("queueId"));
    }

    @Test
    public void testQueryConsumerOffsetOtherErrorCodeThrowsException() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(
                RemotingSysResponseCode.SYSTEM_ERROR, "system error");
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        try {
            client.queryConsumerOffset("GroupA", "TestTopic", 0);
            fail("Should throw ProxyException for system error");
        } catch (ProxyException e) {
            assertTrue(e.getMessage().contains("failed"));
        }
    }

    // ========== UpdateConsumerOffset ==========

    @Test
    public void testUpdateConsumerOffsetSuccess() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        client.updateConsumerOffset("GroupA", "TestTopic", 0, 100L);

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        assertEquals(RequestCode.UPDATE_CONSUMER_OFFSET, requestCaptor.getValue().getCode());
        HashMap<String, String> extFields = requestCaptor.getValue().getExtFields();
        assertEquals("GroupA", extFields.get("consumerGroup"));
        assertEquals("TestTopic", extFields.get("topic"));
        assertEquals("0", extFields.get("queueId"));
        assertEquals("100", extFields.get("commitOffset"));
    }

    @Test
    public void testUpdateConsumerOffsetFailure() throws Exception {
        RemotingCommand response = RemotingCommand.createResponseCommand(
                RemotingSysResponseCode.SYSTEM_ERROR, "update failed");
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        try {
            client.updateConsumerOffset("GroupA", "TestTopic", 0, 100L);
            fail("Should throw ProxyException");
        } catch (ProxyException e) {
            assertTrue(e.getMessage().contains("failed"));
        }
    }

    // ========== Helper: 构建测试用的RemotingCommand ==========

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

    private RemotingCommand buildPullSuccessResponse(long nextBeginOffset, long minOffset,
                                                      long maxOffset, long suggestWhichBrokerId, byte[] body) {
        PullMessageResponseHeader header = new PullMessageResponseHeader();
        header.setNextBeginOffset(nextBeginOffset);
        header.setMinOffset(minOffset);
        header.setMaxOffset(maxOffset);
        header.setSuggestWhichBrokerId(suggestWhichBrokerId);
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setCustomHeader(header);
        response.makeCustomHeaderToNet();
        response.setBody(body);
        return response;
    }

    private RemotingCommand buildPullNotFoundResponse(long nextBeginOffset, long minOffset, long maxOffset) {
        PullMessageResponseHeader header = new PullMessageResponseHeader();
        header.setNextBeginOffset(nextBeginOffset);
        header.setMinOffset(minOffset);
        header.setMaxOffset(maxOffset);
        header.setSuggestWhichBrokerId(0L);
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.PULL_NOT_FOUND);
        response.setCustomHeader(header);
        response.makeCustomHeaderToNet();
        return response;
    }

    private RemotingCommand buildQueryOffsetSuccessResponse(long offset) {
        QueryConsumerOffsetResponseHeader header = new QueryConsumerOffsetResponseHeader();
        header.setOffset(offset);
        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setCustomHeader(header);
        response.makeCustomHeaderToNet();
        return response;
    }
}