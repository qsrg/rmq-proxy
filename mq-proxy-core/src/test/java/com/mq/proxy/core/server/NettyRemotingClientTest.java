package com.mq.proxy.core.server;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
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
}
