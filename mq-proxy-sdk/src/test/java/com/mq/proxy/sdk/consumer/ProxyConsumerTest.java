package com.mq.proxy.sdk.consumer;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.PullMessageResponseHeader;
import com.mq.proxy.core.protocol.header.QueryConsumerOffsetResponseHeader;
import com.mq.proxy.sdk.facade.ProxyClientFacade;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Iterator;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * ProxyConsumer协议测试 - 验证请求构建和响应解析
 */
public class ProxyConsumerTest {

    private ProxyClientFacade mockFacade;
    private ProxyConsumer consumer;

    @Before
    public void setUp() {
        mockFacade = mock(ProxyClientFacade.class);
        when(mockFacade.getRemotingClient()).thenReturn(mock(com.mq.proxy.sdk.remoting.ProxyRemotingClient.class));

        consumer = new ProxyConsumer("TestConsumerGroup");
        injectFacade(consumer, mockFacade, true);
    }

    @Test
    public void testSimplifiedAPI() {
        // 测试简化API（链式配置）
        ProxyConsumer consumer = new ProxyConsumer("GroupA")
            .setProxyAddrs("127.0.0.1:19876")
            .setSuspendTimeoutMillis(15000)
            .setMessageModel("BROADCASTING");

        assertNotNull(consumer);
        assertEquals("GroupA", consumer.getConfig().getConsumerGroup());
        assertEquals("127.0.0.1:19876", consumer.getConfig().getProxyAddrs());
        assertEquals(15000L, consumer.getConfig().getSuspendTimeoutMillis());
        assertEquals("BROADCASTING", consumer.getConfig().getMessageModel());
    }

    @Test
    public void testPullRequestContainsRequiredFields() throws Exception {
        RemotingCommand pullResponse = buildPullSuccessResponse(100L, 0L, 200L, 0L, new byte[]{1, 2, 3});
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(pullResponse);

        consumer.pull("TestTopic", "GroupA", 0, 50L, 32);

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
    }

    @Test
    public void testParsePullResultSuccess() throws Exception {
        byte[] body = new byte[]{1, 2, 3, 4};
        RemotingCommand response = buildPullSuccessResponse(100L, 0L, 200L, 1L, body);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        PullResult result = consumer.pull("TestTopic", "GroupA", 0, 50L, 32);

        assertTrue(result.isSuccess());
        assertTrue(result.isFound());
        assertEquals(RemotingSysResponseCode.SUCCESS, result.getResponseCode());
        assertEquals(100L, result.getNextBeginOffset());
        assertEquals(0L, result.getMinOffset());
        assertEquals(200L, result.getMaxOffset());
        assertEquals(1L, result.getSuggestWhichBrokerId());
        assertArrayEquals(body, result.getBody());
    }

    @Test
    public void testParsePullResultNotFound() throws Exception {
        RemotingCommand response = buildPullNotFoundResponse(55L, 0L, 200L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        PullResult result = consumer.pull("TestTopic", "GroupA", 0, 50L, 32);

        assertTrue(result.isSuccess());
        assertFalse(result.isFound());
        assertEquals(ResponseCode.PULL_NOT_FOUND, result.getResponseCode());
        assertNull(result.getBody());
        assertEquals(55L, result.getNextBeginOffset());
    }

    @Test
    public void testQueryConsumerOffsetSuccess() throws Exception {
        RemotingCommand response = buildQueryOffsetSuccessResponse(500L);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(response);

        long offset = consumer.queryConsumerOffset("GroupA", "TestTopic", 0);

        assertEquals(500L, offset);
    }

    @Test
    public void testPullWithCommitOffset() throws Exception {
        byte[] body = new byte[]{1, 2, 3};
        RemotingCommand pullResponse = buildPullSuccessResponse(101L, 0L, 200L, 0L, body);
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong())).thenReturn(pullResponse);

        PullResult result = consumer.pull("OrderlyTopic", "GroupA", 2, 100L, 32, 100L);

        assertTrue(result.isSuccess());
        assertTrue(result.isFound());

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).invokeSync(requestCaptor.capture(), anyLong());

        RemotingCommand request = requestCaptor.getValue();
        HashMap<String, String> extFields = request.getExtFields();
        assertEquals("100", extFields.get("commitOffset"));
        assertEquals("1", extFields.get("sysFlag"));
    }

    @Test
    public void testStartRegistrationUsesFiniteSubscriptionVersion() throws Exception {
        ProxyConsumer startConsumer = new ProxyConsumer("TestConsumerGroup");
        injectFacade(startConsumer, mockFacade, false);

        startConsumer.subscribe("TestTopic", "TagA");
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong()))
                .thenReturn(RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS));

        startConsumer.start();

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade).start();
        verify(mockFacade, atLeastOnce()).invokeSync(requestCaptor.capture(), anyLong());

        RemotingCommand heartbeatRequest = requestCaptor.getAllValues().stream()
                .filter(command -> command.getCode() == RequestCode.HEART_BEAT)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing heartbeat registration request"));

        com.mq.proxy.core.protocol.heartbeat.HeartbeatData heartbeatData =
                com.mq.proxy.core.protocol.heartbeat.HeartbeatData.decode(heartbeatRequest.getBody());
        assertEquals(1, heartbeatData.getConsumerDataSet().size());

        Iterator<com.mq.proxy.core.protocol.heartbeat.HeartbeatData.ConsumerData> consumerIterator =
                heartbeatData.getConsumerDataSet().iterator();
        com.mq.proxy.core.protocol.heartbeat.HeartbeatData.ConsumerData consumerData = consumerIterator.next();
        assertEquals("TestConsumerGroup", consumerData.getGroupName());
        assertEquals(1, consumerData.getSubscriptionDataSet().size());

        com.mq.proxy.core.protocol.heartbeat.HeartbeatData.SubscriptionData subscriptionData =
                consumerData.getSubscriptionDataSet().iterator().next();
        assertNotEquals(Long.MAX_VALUE, subscriptionData.getSubVersion());
        assertTrue(subscriptionData.getSubVersion() > 0L);
    }

    @Test
    public void testPullRequestUsesSubscribedVersion() throws Exception {
        ProxyConsumer startConsumer = new ProxyConsumer("TestConsumerGroup");
        injectFacade(startConsumer, mockFacade, false);

        startConsumer.subscribe("TestTopic", "TagA");
        when(mockFacade.invokeSync(any(RemotingCommand.class), anyLong()))
                .thenReturn(RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS))
                .thenReturn(buildPullSuccessResponse(100L, 0L, 200L, 0L, new byte[]{1}));

        startConsumer.start();
        startConsumer.pull("TestTopic", "TestConsumerGroup", 0, 50L, 32);

        ArgumentCaptor<RemotingCommand> requestCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockFacade, atLeast(2)).invokeSync(requestCaptor.capture(), anyLong());

        RemotingCommand heartbeatRequest = requestCaptor.getAllValues().stream()
                .filter(command -> command.getCode() == RequestCode.HEART_BEAT)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing heartbeat registration request"));
        RemotingCommand pullRequest = requestCaptor.getAllValues().stream()
                .filter(command -> command.getCode() == RequestCode.PULL_MESSAGE)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing pull request"));

        com.mq.proxy.core.protocol.heartbeat.HeartbeatData heartbeatData =
                com.mq.proxy.core.protocol.heartbeat.HeartbeatData.decode(heartbeatRequest.getBody());
        long registeredVersion = heartbeatData.getConsumerDataSet().iterator().next()
                .getSubscriptionDataSet().iterator().next().getSubVersion();

        assertEquals(String.valueOf(registeredVersion), pullRequest.getExtFields().get("subVersion"));
    }

    private void injectFacade(ProxyConsumer targetConsumer, ProxyClientFacade targetFacade, boolean started) {
        try {
            java.lang.reflect.Field facadeField = ProxyConsumer.class.getDeclaredField("facade");
            facadeField.setAccessible(true);
            facadeField.set(targetConsumer, targetFacade);

            java.lang.reflect.Field startedField = ProxyConsumer.class.getDeclaredField("started");
            startedField.setAccessible(true);
            startedField.set(targetConsumer, started);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
