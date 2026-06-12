package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NettyRemotingClientTest {

    @Test
    public void testClientsShareInjectedRuntimeExecutors() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientWorkerThreadNums(1);
        config.setClientCallbackExecutorThreads(1);
        NettyClientRuntime runtime = new NettyClientRuntime(config, "SharedRuntimeTest");
        NettyRemotingClient first = new NettyRemotingClient(config, runtime);
        NettyRemotingClient second = new NettyRemotingClient(config, runtime);

        try {
            first.start();
            second.start();

            assertSame(eventLoopGroup(first), eventLoopGroup(second));
            assertSame(callbackExecutor(first), callbackExecutor(second));
            assertSame(responseTableScanExecutor(first), responseTableScanExecutor(second));

            first.shutdown();

            assertFalse(runtime.isShutdown());
        } finally {
            second.shutdown();
            runtime.shutdown();
        }
    }

    @Test
    public void testRuntimeUsesSingleSharedResponseScanLoopForRegistrations() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientWorkerThreadNums(1);
        config.setClientCallbackExecutorThreads(1);
        NettyClientRuntime runtime = new NettyClientRuntime(config, "SharedScanTest");
        AtomicReference<NettyClientRuntime.ResponseTableScanRegistration> firstRegistration = new AtomicReference<>();
        AtomicReference<NettyClientRuntime.ResponseTableScanRegistration> secondRegistration = new AtomicReference<>();
        java.util.concurrent.atomic.AtomicInteger firstScans = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger secondScans = new java.util.concurrent.atomic.AtomicInteger();

        try {
            runtime.start();
            firstRegistration.set(runtime.scheduleResponseTableScan(new Runnable() {
                @Override
                public void run() {
                    firstScans.incrementAndGet();
                }
            }));
            secondRegistration.set(runtime.scheduleResponseTableScan(new Runnable() {
                @Override
                public void run() {
                    secondScans.incrementAndGet();
                }
            }));

            Thread.sleep(1200L);
            assertSame(runtime.getResponseTableScanFuture(), firstRegistration.get().getSharedFuture());
            assertSame(runtime.getResponseTableScanFuture(), secondRegistration.get().getSharedFuture());
            assertTrue(firstScans.get() > 0);
            assertTrue(secondScans.get() > 0);

            firstRegistration.get().cancel();
            int firstAfterCancel = firstScans.get();
            int secondAfterCancel = secondScans.get();
            Thread.sleep(1200L);

            assertEquals(firstAfterCancel, firstScans.get());
            assertTrue(secondScans.get() > secondAfterCancel);
        } finally {
            if (firstRegistration.get() != null) {
                firstRegistration.get().cancel();
            }
            if (secondRegistration.get() != null) {
                secondRegistration.get().cancel();
            }
            runtime.shutdown();
        }
    }

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

    @Test
    public void testClientHandlerSendsKeepAliveOnWriterIdleWhenConfigured() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientKeepAliveRequestCode(RequestCode.CHECK_CLIENT_CONFIG);
        config.setClientKeepAliveIntervalSeconds(30);
        ExposedNettyRemotingClient client = new ExposedNettyRemotingClient(config);
        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        channelTable(client).put("171.31.208.1:9876", channel);

        client.newHandler().userEventTriggered(ctx, IdleStateEvent.WRITER_IDLE_STATE_EVENT);

        verify(ctx).writeAndFlush(argThat(argument -> {
            RemotingCommand command = (RemotingCommand) argument;
            return command.getCode() == RequestCode.CHECK_CLIENT_CONFIG && command.isOnewayRPC()
                    && command.getBody() == null;
        }));
        verify(ctx, never()).close();
        assertSame(channel, channelTable(client).get("171.31.208.1:9876"));
    }

    @Test
    public void testInvokeAsyncCallbackReceivesResponse() throws Exception {
        ExposedNettyRemotingClient client = new ExposedNettyRemotingClient();
        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(channel.isActive()).thenReturn(true);
        when(ctx.channel()).thenReturn(channel);
        client.setFixedChannel(channel);
        channelTable(client).put("171.31.208.1:9876", channel);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<RemotingCommand> callbackResponse = new AtomicReference<>();

        client.invokeAsync("171.31.208.1:9876", request, 3000, new InvokeCallback() {
            @Override
            public void operationSucceed(RemotingCommand response) {
                callbackResponse.set(response);
                latch.countDown();
            }

            @Override
            public void operationFail(Throwable throwable) {
            }
        });

        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "OK");
        response.setOpaque(request.getOpaque());
        client.newHandler().channelRead0(ctx, response);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertSame(response, callbackResponse.get());
        assertTrue(responseTable(client).isEmpty());
    }

    @Test
    public void testInvokeAsyncRejectsWhenClientAsyncSemaphoreExhausted() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientAsyncSemaphoreValue(1);
        ExposedNettyRemotingClient client = new ExposedNettyRemotingClient(config);
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        client.setFixedChannel(channel);
        channelTable(client).put("171.31.208.1:9876", channel);

        client.invokeAsync("171.31.208.1:9876",
                RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null),
                3000,
                new InvokeCallback() {
                    @Override
                    public void operationSucceed(RemotingCommand response) {
                    }

                    @Override
                    public void operationFail(Throwable throwable) {
                    }
                });

        CountDownLatch rejectedLatch = new CountDownLatch(1);
        AtomicReference<Throwable> rejected = new AtomicReference<>();
        client.invokeAsync("171.31.208.1:9876",
                RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null),
                3000,
                new InvokeCallback() {
                    @Override
                    public void operationSucceed(RemotingCommand response) {
                    }

                    @Override
                    public void operationFail(Throwable throwable) {
                        rejected.set(throwable);
                        rejectedLatch.countDown();
                    }
                });

        assertTrue(rejectedLatch.await(1, TimeUnit.SECONDS));
        assertNotNull(rejected.get());
        assertEquals("too many async requests, addr: 171.31.208.1:9876, limit: 1",
                rejected.get().getMessage());
        assertEquals(1, responseTable(client).size());
    }

    @Test
    public void testClientHandlerFailsPendingAsyncRequestsWhenRemoteDisconnects() throws Exception {
        ExposedNettyRemotingClient client = new ExposedNettyRemotingClient();
        Channel channel = mock(Channel.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(channel.isActive()).thenReturn(true);
        when(ctx.channel()).thenReturn(channel);
        client.setFixedChannel(channel);
        channelTable(client).put("171.31.208.1:9876", channel);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();

        client.invokeAsync("171.31.208.1:9876",
                RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null),
                3000,
                new InvokeCallback() {
                    @Override
                    public void operationSucceed(RemotingCommand response) {
                    }

                    @Override
                    public void operationFail(Throwable throwable) {
                        callbackFailure.set(throwable);
                        latch.countDown();
                    }
                });

        client.newHandler().channelInactive(ctx);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals("channel closed while waiting for response, addr: 171.31.208.1:9876, reason: remote-inactive",
                callbackFailure.get().getMessage());
        assertTrue(responseTable(client).isEmpty());
    }

    @Test
    public void testScanResponseTableTimesOutPendingAsyncRequest() throws Exception {
        ExposedNettyRemotingClient client = new ExposedNettyRemotingClient();
        Channel channel = mock(Channel.class);
        when(channel.isActive()).thenReturn(true);
        client.setFixedChannel(channel);
        channelTable(client).put("171.31.208.1:9876", channel);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();

        client.invokeAsync("171.31.208.1:9876",
                RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null),
                1,
                new InvokeCallback() {
                    @Override
                    public void operationSucceed(RemotingCommand response) {
                    }

                    @Override
                    public void operationFail(Throwable throwable) {
                        callbackFailure.set(throwable);
                        latch.countDown();
                    }
                });

        Thread.sleep(10L);
        client.scanResponses();

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals("invokeAsync timeout, addr: 171.31.208.1:9876, timeoutMillis: 1",
                callbackFailure.get().getMessage());
        assertTrue(responseTable(client).isEmpty());
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<String, Channel> channelTable(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("channelTable");
        field.setAccessible(true);
        return (ConcurrentHashMap<String, Channel>) field.get(client);
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<Integer, ResponseFuture> responseTable(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("responseTable");
        field.setAccessible(true);
        return (ConcurrentHashMap<Integer, ResponseFuture>) field.get(client);
    }

    private static EventLoopGroup eventLoopGroup(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("eventLoopGroup");
        field.setAccessible(true);
        return (EventLoopGroup) field.get(client);
    }

    private static ExecutorService callbackExecutor(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("callbackExecutor");
        field.setAccessible(true);
        return (ExecutorService) field.get(client);
    }

    private static ScheduledExecutorService responseTableScanExecutor(NettyRemotingClient client) throws Exception {
        Field field = NettyRemotingClient.class.getDeclaredField("responseTableScanExecutor");
        field.setAccessible(true);
        return (ScheduledExecutorService) field.get(client);
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
        private Channel fixedChannel;

        ExposedNettyRemotingClient() {
            super(new NettyClientConfig());
        }

        ExposedNettyRemotingClient(NettyClientConfig config) {
            super(config);
        }

        RemotingCommand invokeOne(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
            return invokeSyncSingle(addr, request, timeoutMillis);
        }

        void setFixedChannel(Channel fixedChannel) {
            this.fixedChannel = fixedChannel;
        }

        @Override
        public Channel getAndCreateChannel(String addr) throws Exception {
            if (fixedChannel != null) {
                return fixedChannel;
            }
            return channelTable(this).get(addr);
        }

        NettyClientHandler newHandler() {
            return new NettyClientHandler();
        }

        void scanResponses() throws Exception {
            java.lang.reflect.Method method = NettyRemotingClient.class.getDeclaredMethod("scanResponseTable");
            method.setAccessible(true);
            method.invoke(this);
        }
    }
}
