package com.mq.proxy.sdk.remoting;

import com.mq.proxy.core.protocol.RemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * 验证SDK ProxyClientHandler修复后对server-push通知的处理能力。
 *
 * 修复: handler现在通过isResponseType()区分request和response:
 * - response (isResponseType=true): 通过opaque匹配responseTable
 * - request (isResponseType=false): 查找processorTable处理
 */
public class ProxyClientHandlerTest {

    private ConcurrentHashMap<Integer, ProxyResponseFuture> responseTable;
    private ConcurrentHashMap<Integer, SDKRequestProcessor> processorTable;
    private ProxyClientHandler handler;
    private ChannelHandlerContext mockCtx;

    @Before
    public void setUp() {
        responseTable = new ConcurrentHashMap<>();
        processorTable = new ConcurrentHashMap<>();
        handler = new ProxyClientHandler(responseTable, processorTable);
        mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));
    }

    /**
     * 正常response: opaque在responseTable中，能正确匹配并complete future。
     */
    @Test
    public void testHandleNormalResponse() throws Exception {
        ProxyResponseFuture future = new ProxyResponseFuture(100, null, 3000);
        responseTable.put(100, future);

        RemotingCommand response = RemotingCommand.createResponseCommand(0, "ok");
        response.setOpaque(100);
        response.markResponseType();

        handler.channelRead0(mockCtx, response);

        assertEquals(response, future.getResponse());
        assertFalse(responseTable.containsKey(100));
    }

    /**
     * 修复验证: server-push request不再被当作response丢弃，
     * 而是查找processorTable处理。有注册processor时正确回调。
     */
    @Test
    public void testServerPushNotificationHandledByProcessor() throws Exception {
        final String[] capturedGroup = new String[1];
        // NOTIFY_CONSUMER_IDS_CHANGED = 40
        processorTable.put(40, new SDKRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                HashMap<String, String> ext = request.getExtFields();
                if (ext != null) {
                    capturedGroup[0] = ext.get("consumerGroup");
                }
                return RemotingCommand.createResponseCommand(0, "ok");
            }
        });

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "GroupA");
        RemotingCommand notification = RemotingCommand.createRequestCommand(40, null);
        notification.setExtFields(extFields);

        handler.channelRead0(mockCtx, notification);

        assertEquals("GroupA", capturedGroup[0]);
        ArgumentCaptor<RemotingCommand> responseCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockCtx).writeAndFlush(responseCaptor.capture());
        assertTrue(responseCaptor.getValue().isResponseType());
        assertEquals(notification.getOpaque(), responseCaptor.getValue().getOpaque());
    }

    /**
     * 修复验证: request不再与response混淆。
     * 即使opaque碰巧匹配responseTable中的future，request也不会被误当response。
     * 因为handler先检查isResponseType()，request走processorTable分支。
     */
    @Test
    public void testRequestNotMisinterpretedAsResponse() throws Exception {
        // SDK有一个pending request的future（opaque=200）
        ProxyResponseFuture future = new ProxyResponseFuture(200, null, 3000);
        responseTable.put(200, future);

        // server-push request opaque恰好是200，但有不同requestCode
        processorTable.put(40, new SDKRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                // 正常处理推送通知
                return RemotingCommand.createResponseCommand(0, "ok");
            }
        });

        RemotingCommand notification = RemotingCommand.createRequestCommand(40, null);
        notification.setOpaque(200);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "GroupA");
        notification.setExtFields(extFields);

        handler.channelRead0(mockCtx, notification);

        // request走processorTable分支，不影响responseTable中的future
        assertNull(future.getResponse()); // future未被complete
        assertTrue(responseTable.containsKey(200)); // future仍在table中等待真实response
    }

    /**
     * 无注册processor时，server-push request被忽略（warn日志）。
     */
    @Test
    public void testServerPushNoProcessorRegistered() throws Exception {
        RemotingCommand notification = RemotingCommand.createRequestCommand(40, null);
        notification.setOpaque(99991);

        handler.channelRead0(mockCtx, notification);

        // 无processor，request被忽略，不影响responseTable
        assertFalse(responseTable.containsKey(99991));
    }

    /**
     * response和request可以共存处理，互不干扰。
     */
    @Test
    public void testResponseAndRequestCoexist() throws Exception {
        // 注册processor
        final int[] processorCalled = new int[1];
        processorTable.put(40, new SDKRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
                processorCalled[0]++;
                return RemotingCommand.createResponseCommand(0, "ok");
            }
        });

        // SDK有pending request future
        ProxyResponseFuture future = new ProxyResponseFuture(1001, null, 3000);
        responseTable.put(1001, future);

        // 收到server-push request
        RemotingCommand notify = RemotingCommand.createRequestCommand(40, null);
        notify.setOpaque(99991);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", "GroupA");
        notify.setExtFields(extFields);
        handler.channelRead0(mockCtx, notify);

        // 收到SDK request的真实response
        RemotingCommand response = RemotingCommand.createResponseCommand(0, "ok");
        response.setOpaque(1001);
        response.markResponseType();
        handler.channelRead0(mockCtx, response);

        // request被processor处理，response被future接收
        assertEquals(1, processorCalled[0]);
        assertEquals(response, future.getResponse());
        assertFalse(responseTable.containsKey(1001));
    }

    @Test
    public void testProcessorExceptionReturnsErrorResponse() throws Exception {
        processorTable.put(40, new SDKRequestProcessor() {
            @Override
            public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
                throw new RuntimeException("boom");
            }
        });

        RemotingCommand notification = RemotingCommand.createRequestCommand(40, null);
        notification.setOpaque(321);

        handler.channelRead0(mockCtx, notification);

        ArgumentCaptor<RemotingCommand> responseCaptor = ArgumentCaptor.forClass(RemotingCommand.class);
        verify(mockCtx).writeAndFlush(responseCaptor.capture());
        assertTrue(responseCaptor.getValue().isResponseType());
        assertEquals(321, responseCaptor.getValue().getOpaque());
    }
}
