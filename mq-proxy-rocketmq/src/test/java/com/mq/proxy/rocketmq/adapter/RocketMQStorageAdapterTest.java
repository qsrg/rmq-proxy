package com.mq.proxy.rocketmq.adapter;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.server.InvokeCallback;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.PullMessageCallback;
import com.mq.proxy.core.storage.PutMessageCallback;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RocketMQStorageAdapterTest {

    @Test
    public void testInitializeAndShutdown() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        StorageConfig config = new StorageConfig();
        config.setNamesrvAddr("localhost:9876");

        adapter.initialize(config);
        assertTrue(adapter.healthCheck());

        adapter.shutdown();
        assertFalse(adapter.healthCheck());
    }

    @Test
    public void testHealthCheck() {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        assertFalse(adapter.healthCheck());
    }

    @Test
    public void testPullMessageDoesNotRewindOffsetOnNotFound() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        setField(adapter, "initialized", true);
        setField(adapter, "remotingClient", new FixedResponseRemotingClient(createPullNotFoundResponse(0L, 0L, 0L)));

        PullResult result = adapter.pullMessage(
                "CID_TEST", "TestTopic", 0, 2L, 32, 0, 2L,
                15000L, "*", "TAG", 0L, "127.0.0.1:10911");

        assertEquals(ResponseCode.PULL_NOT_FOUND, result.getResponseCode());
        assertEquals(2L, result.getNextBeginOffset());
    }

    @Test
    public void testPullMessageUsesNativeCompatibleSuspendTimeoutBudget() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        RecordingRemotingClient remotingClient = new RecordingRemotingClient(createPullNotFoundResponse(2L, 0L, 2L));
        setField(adapter, "initialized", true);
        setField(adapter, "remotingClient", remotingClient);

        adapter.pullMessage(
                "CID_TEST", "TestTopic", 0, 2L, 32, 0, 2L,
                15000L, "*", "TAG", 0L, "127.0.0.1:10911");

        assertEquals(30000L, remotingClient.lastTimeoutMillis);
    }

    @Test
    public void testPullMessageAsyncDoesNotRewindOffsetOnNotFound() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        setField(adapter, "initialized", true);
        setField(adapter, "remotingClient", new AsyncFixedResponseRemotingClient(createPullNotFoundResponse(0L, 0L, 0L)));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<PullResult> callbackResult = new AtomicReference<>();

        adapter.pullMessageAsync("CID_TEST", "TestTopic", 0, 2L, 32, 0, 2L,
                15000L, "*", "TAG", 0L, "127.0.0.1:10911", new PullMessageCallback() {
                    @Override
                    public void onSuccess(PullResult pullResult) {
                        callbackResult.set(pullResult);
                        latch.countDown();
                    }

                    @Override
                    public void onException(Throwable throwable) {
                    }
                });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertEquals(ResponseCode.PULL_NOT_FOUND, callbackResult.get().getResponseCode());
        assertEquals(2L, callbackResult.get().getNextBeginOffset());
    }

    @Test
    public void testPullMessageAsyncUsesNativeCompatibleSuspendTimeoutBudget() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        AsyncRecordingRemotingClient remotingClient = new AsyncRecordingRemotingClient(createPullNotFoundResponse(2L, 0L, 2L));
        setField(adapter, "initialized", true);
        setField(adapter, "remotingClient", remotingClient);

        adapter.pullMessageAsync("CID_TEST", "TestTopic", 0, 2L, 32, 0, 2L,
                15000L, "*", "TAG", 0L, "127.0.0.1:10911", new PullMessageCallback() {
                    @Override
                    public void onSuccess(PullResult pullResult) {
                    }

                    @Override
                    public void onException(Throwable throwable) {
                    }
                });

        assertEquals(30000L, remotingClient.lastTimeoutMillis);
    }

    @Test
    public void testPullMessageAsyncRoundRobinsConfiguredRemotingClientPool() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        CountingAsyncRemotingClient first = new CountingAsyncRemotingClient(createPullNotFoundResponse(2L, 0L, 2L));
        CountingAsyncRemotingClient second = new CountingAsyncRemotingClient(createPullNotFoundResponse(3L, 0L, 3L));
        setField(adapter, "initialized", true);
        setField(adapter, "remotingClients", new NettyRemotingClient[]{first, second});

        PullMessageCallback noopCallback = new PullMessageCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
            }

            @Override
            public void onException(Throwable throwable) {
            }
        };

        adapter.pullMessageAsync("CID_TEST", "TestTopic", 0, 2L, 32, 0, 2L,
                15000L, "*", "TAG", 0L, "127.0.0.1:10911", noopCallback);
        adapter.pullMessageAsync("CID_TEST", "TestTopic", 0, 3L, 32, 0, 3L,
                15000L, "*", "TAG", 0L, "127.0.0.1:10911", noopCallback);

        assertEquals(1, first.invokeCount);
        assertEquals(1, second.invokeCount);
    }

    @Test
    public void testPutAndPullAsyncUseDedicatedRemotingClientPools() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        CountingAsyncRemotingClient producerClient = new CountingAsyncRemotingClient(createPutSuccessResponse("async-msg-1", 1, 500L));
        CountingAsyncRemotingClient pullClient = new CountingAsyncRemotingClient(createPullNotFoundResponse(2L, 0L, 2L));
        setField(adapter, "initialized", true);
        setField(adapter, "producerRemotingClients", new NettyRemotingClient[]{producerClient});
        setField(adapter, "pullRemotingClients", new NettyRemotingClient[]{pullClient});

        InternalMessage message = new InternalMessage();
        message.setProducerGroup("producer-group");
        message.setTopic("AsyncTopic");
        message.setQueueId(1);
        message.setBody("async body".getBytes());

        adapter.pullMessageAsync("CID_TEST", "TestTopic", 0, 2L, 32, 0, 2L,
                15000L, "*", "TAG", 0L, "127.0.0.1:10911", new PullMessageCallback() {
                    @Override
                    public void onSuccess(PullResult pullResult) {
                    }

                    @Override
                    public void onException(Throwable throwable) {
                    }
                });
        adapter.putMessageAsync(message, "127.0.0.1:10911", new PutMessageCallback() {
            @Override
            public void onSuccess(PutResult putResult) {
            }

            @Override
            public void onException(Throwable throwable) {
            }
        });

        assertEquals(1, producerClient.invokeCount);
        assertEquals(1, pullClient.invokeCount);
    }

    @Test
    public void testBuildUpstreamClientStatsLogSeparatesProducerAndPullPools() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        setField(adapter, "producerRemotingClients", new NettyRemotingClient[]{
                new StatsRemotingClient(10, 90, 100)
        });
        setField(adapter, "pullRemotingClients", new NettyRemotingClient[]{
                new StatsRemotingClient(20, 80, 100)
        });

        String stats = adapter.buildUpstreamClientStatsLog();

        assertTrue(stats.contains("producer[0]{inFlight=10, availablePermits=90, limit=100}"));
        assertTrue(stats.contains("pull[0]{inFlight=20, availablePermits=80, limit=100}"));
    }

    @Test
    public void testPutMessageAsyncReturnsBrokerResult() throws Exception {
        RocketMQStorageAdapter adapter = new RocketMQStorageAdapter();
        RemotingCommand response = createPutSuccessResponse("async-msg-1", 1, 500L);
        setField(adapter, "initialized", true);
        setField(adapter, "remotingClient", new AsyncFixedResponseRemotingClient(response));

        InternalMessage message = new InternalMessage();
        message.setProducerGroup("producer-group");
        message.setTopic("AsyncTopic");
        message.setQueueId(1);
        message.setBody("async body".getBytes());
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<PutResult> callbackResult = new AtomicReference<>();

        adapter.putMessageAsync(message, "127.0.0.1:10911", new PutMessageCallback() {
            @Override
            public void onSuccess(PutResult putResult) {
                callbackResult.set(putResult);
                latch.countDown();
            }

            @Override
            public void onException(Throwable throwable) {
            }
        });

        assertTrue(latch.await(1, TimeUnit.SECONDS));
        assertTrue(callbackResult.get().isSuccess());
        assertEquals("async-msg-1", callbackResult.get().getMsgId());
        assertEquals(1, callbackResult.get().getQueueId());
        assertEquals(500L, callbackResult.get().getQueueOffset());
    }

    private static RemotingCommand createPutSuccessResponse(String msgId, int queueId, long queueOffset) {
        RemotingCommand response = RemotingCommand.createResponseCommand(0);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("msgId", msgId);
        extFields.put("queueId", String.valueOf(queueId));
        extFields.put("queueOffset", String.valueOf(queueOffset));
        response.setExtFields(extFields);
        return response;
    }

    private static RemotingCommand createPullNotFoundResponse(long nextBeginOffset, long minOffset, long maxOffset) {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.PULL_NOT_FOUND);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("nextBeginOffset", String.valueOf(nextBeginOffset));
        extFields.put("minOffset", String.valueOf(minOffset));
        extFields.put("maxOffset", String.valueOf(maxOffset));
        extFields.put("suggestWhichBrokerId", "0");
        response.setExtFields(extFields);
        return response;
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class FixedResponseRemotingClient extends NettyRemotingClient {
        private final RemotingCommand response;

        FixedResponseRemotingClient(RemotingCommand response) {
            super(new NettyClientConfig());
            this.response = response;
        }

        @Override
        public void start() {
        }

        @Override
        protected RemotingCommand invokeSyncSingle(String addr, RemotingCommand request, long timeoutMillis) {
            return response;
        }
    }

    private static class RecordingRemotingClient extends FixedResponseRemotingClient {
        private long lastTimeoutMillis;

        RecordingRemotingClient(RemotingCommand response) {
            super(response);
        }

        @Override
        public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
            this.lastTimeoutMillis = timeoutMillis;
            return super.invokeSync(addr, request, timeoutMillis);
        }
    }

    private static class AsyncFixedResponseRemotingClient extends NettyRemotingClient {
        private final RemotingCommand response;

        AsyncFixedResponseRemotingClient(RemotingCommand response) {
            super(new NettyClientConfig());
            this.response = response;
        }

        @Override
        public void start() {
        }

        @Override
        public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {
            invokeCallback.operationSucceed(response);
        }
    }

    private static class AsyncRecordingRemotingClient extends AsyncFixedResponseRemotingClient {
        private long lastTimeoutMillis;

        AsyncRecordingRemotingClient(RemotingCommand response) {
            super(response);
        }

        @Override
        public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {
            this.lastTimeoutMillis = timeoutMillis;
            super.invokeAsync(addr, request, timeoutMillis, invokeCallback);
        }
    }

    private static class CountingAsyncRemotingClient extends AsyncFixedResponseRemotingClient {
        private int invokeCount;

        CountingAsyncRemotingClient(RemotingCommand response) {
            super(response);
        }

        @Override
        public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) {
            this.invokeCount++;
            super.invokeAsync(addr, request, timeoutMillis, invokeCallback);
        }
    }

    private static class StatsRemotingClient extends AsyncFixedResponseRemotingClient {
        private final int inFlightRequestCount;
        private final int availablePermits;
        private final int semaphoreLimit;

        StatsRemotingClient(int inFlightRequestCount, int availablePermits, int semaphoreLimit) {
            super(createPullNotFoundResponse(0L, 0L, 0L));
            this.inFlightRequestCount = inFlightRequestCount;
            this.availablePermits = availablePermits;
            this.semaphoreLimit = semaphoreLimit;
        }

        @Override
        public int getInFlightRequestCount() {
            return inFlightRequestCount;
        }

        @Override
        public int getAsyncSemaphoreAvailablePermits() {
            return availablePermits;
        }

        @Override
        public int getAsyncSemaphoreLimit() {
            return semaphoreLimit;
        }
    }
}
