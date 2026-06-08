package com.mq.proxy.rocketmq.adapter;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.core.storage.model.PullResult;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;

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
}
