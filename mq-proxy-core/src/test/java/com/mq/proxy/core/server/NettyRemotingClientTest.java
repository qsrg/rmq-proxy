package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NettyRemotingClientTest {

    @Test
    public void testInvokeSyncFailsOverToNextNamesrvAddr() throws Exception {
        RecordingNettyRemotingClient client = new RecordingNettyRemotingClient();
        RemotingCommand expectedResponse = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "OK");

        client.willThrow("171.31.208.1:9876", new RuntimeException("connect failed"));
        client.willReturn("171.31.208.2:9876", expectedResponse);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        RemotingCommand response = client.invokeSync(
                "171.31.208.1:9876;171.31.208.2:9876",
                request,
                3000
        );

        assertSame(expectedResponse, response);
        assertEquals(
                Arrays.asList("171.31.208.1:9876", "171.31.208.2:9876"),
                client.getInvokedAddrs()
        );
    }

    @Test
    public void testInvokeSyncRoundRobinsAcrossNamesrvAddrs() throws Exception {
        RecordingNettyRemotingClient client = new RecordingNettyRemotingClient();
        client.willReturn("171.31.208.1:9876",
                RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "first"));
        client.willReturn("171.31.208.2:9876",
                RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "second"));

        client.invokeSync("171.31.208.1:9876;171.31.208.2:9876",
                RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null), 3000);
        client.invokeSync("171.31.208.1:9876;171.31.208.2:9876",
                RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null), 3000);

        assertEquals(
                Arrays.asList("171.31.208.1:9876", "171.31.208.2:9876"),
                client.getInvokedAddrs()
        );
    }

    @Test
    public void testInvokeSyncSingleKeepsChannelAfterTimeout() throws Exception {
        ExposedNettyRemotingClient client = new ExposedNettyRemotingClient();
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        channelTable(client).put("171.31.208.1:9876", channel);

        try {
            client.invokeOne("171.31.208.1:9876",
                    RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null), 1);
        } catch (RuntimeException expected) {
            assertEquals("invokeSync timeout, addr: 171.31.208.1:9876, timeoutMillis: 1", expected.getMessage());
        }

        verify(channel, never()).close();
        assertSame(channel, channelTable(client).get("171.31.208.1:9876"));
    }

    @Test
    public void testClientHandlerRemovesChannelWhenRemoteDisconnects() throws Exception {
        ExposedNettyRemotingClient client = new ExposedNettyRemotingClient();
        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        channelTable(client).put("171.31.208.1:9876", channel);

        client.newHandler().channelInactive(ctx);

        assertFalse(channelTable(client).containsKey("171.31.208.1:9876"));
    }

    @Test
    public void testClientHandlerRemovesChannelWhenIdle() throws Exception {
        ExposedNettyRemotingClient client = new ExposedNettyRemotingClient();
        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        channelTable(client).put("171.31.208.1:9876", channel);

        client.newHandler().userEventTriggered(ctx, IdleStateEvent.ALL_IDLE_STATE_EVENT);

        verify(ctx).close();
        assertFalse(channelTable(client).containsKey("171.31.208.1:9876"));
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<String, Channel> channelTable(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("channelTable");
        field.setAccessible(true);
        return (ConcurrentHashMap<String, Channel>) field.get(client);
    }

    private static class RecordingNettyRemotingClient extends NettyRemotingClient {
        private final List<String> invokedAddrs = new ArrayList<>();
        private final Map<String, Deque<Object>> scriptedResults = new HashMap<>();

        RecordingNettyRemotingClient() {
            super(new NettyClientConfig());
        }

        void willReturn(String addr, RemotingCommand response) {
            scriptedResults.computeIfAbsent(addr, key -> new ArrayDeque<>()).addLast(response);
        }

        void willThrow(String addr, RuntimeException exception) {
            scriptedResults.computeIfAbsent(addr, key -> new ArrayDeque<>()).addLast(exception);
        }

        List<String> getInvokedAddrs() {
            return invokedAddrs;
        }

        @Override
        protected RemotingCommand invokeSyncSingle(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
            invokedAddrs.add(addr);
            Deque<Object> scripted = scriptedResults.get(addr);
            if (scripted == null || scripted.isEmpty()) {
                throw new AssertionError("No scripted result for addr " + addr);
            }
            Object result = scripted.removeFirst();
            if (result instanceof Exception) {
                throw (Exception) result;
            }
            return (RemotingCommand) result;
        }
    }

    private static class ExposedNettyRemotingClient extends NettyRemotingClient {
        ExposedNettyRemotingClient() {
            super(new NettyClientConfig());
        }

        RemotingCommand invokeOne(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
            return invokeSyncSingle(addr, request, timeoutMillis);
        }

        NettyClientHandler newHandler() {
            return new NettyClientHandler();
        }
    }
}
