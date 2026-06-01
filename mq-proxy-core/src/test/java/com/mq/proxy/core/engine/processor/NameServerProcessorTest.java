package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import io.netty.channel.Channel;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NameServerProcessorTest {

    @Test
    public void testForwardToNameServerFallsBackToNextAddress() throws Exception {
        RecordingNettyRemotingClient namesrvClient = new RecordingNettyRemotingClient();
        RemotingCommand expected = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, "OK");
        expected.setBody(new byte[]{1, 2, 3});

        namesrvClient.willThrow("171.31.208.1:9876", new RuntimeException("connect failed"));
        namesrvClient.willReturn("171.31.208.2:9876", expected);

        VirtualRouteManager routeManager = new VirtualRouteManager(namesrvClient);
        routeManager.start("171.31.208.1:9876;171.31.208.2:9876", "127.0.0.1", 10911);

        NameServerProcessor processor = new NameServerProcessor(routeManager);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        request.setExtFields(new HashMap<String, String>());

        Channel channel = mock(Channel.class);
        when(channel.remoteAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 12345));

        RemotingCommand response = processor.processRequest(channel, request);

        assertSame(expected, response);
        assertEquals(
                Arrays.asList("171.31.208.1:9876", "171.31.208.2:9876"),
                namesrvClient.getInvokedAddrs()
        );
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
        public void start() {
        }

        @Override
        protected RemotingCommand invokeSyncSingle(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
            invokedAddrs.add(addr);
            Deque<Object> scripted = scriptedResults.get(addr);
            if (scripted == null || scripted.isEmpty()) {
                throw new AssertionError("No scripted result for addr " + addr + ", requestCode=" + request.getCode());
            }
            Object result = scripted.removeFirst();
            if (result instanceof Exception) {
                throw (Exception) result;
            }
            return (RemotingCommand) result;
        }
    }
}
