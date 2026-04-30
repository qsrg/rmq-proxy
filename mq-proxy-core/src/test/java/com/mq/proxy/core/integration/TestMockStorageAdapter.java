package com.mq.proxy.core.integration;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TestMockStorageAdapter implements StorageAdapter {

    private final AtomicInteger msgIdCounter = new AtomicInteger(0);
    private final AtomicLong offsetCounter = new AtomicLong(0);
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, List<InternalMessage>>> topicQueueMessages = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>> consumerOffsets = new ConcurrentHashMap<>();
    private volatile boolean initialized = false;
    private static final int DEFAULT_QUEUE_NUM = 4;
    private final Random random = new Random();

    @Override
    public void initialize(StorageConfig config) {
        initialized = true;
    }

    @Override
    public void shutdown() {
        topicQueueMessages.clear();
        consumerOffsets.clear();
        initialized = false;
    }

    @Override
    public String getAdapterName() {
        return "mock";
    }

    @Override
    public PutResult putMessage(InternalMessage message) {
        String msgId = "MOCK_MSG_" + msgIdCounter.incrementAndGet();
        int queueId;
        if (message.getQueueId() >= 0) {
            queueId = message.getQueueId();
        } else {
            queueId = random.nextInt(DEFAULT_QUEUE_NUM);
        }

        ConcurrentHashMap<Integer, List<InternalMessage>> queueMap = topicQueueMessages.get(message.getTopic());
        if (queueMap == null) {
            queueMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, List<InternalMessage>> existing = topicQueueMessages.putIfAbsent(message.getTopic(), queueMap);
            if (existing != null) {
                queueMap = existing;
            }
        }

        List<InternalMessage> messageList = queueMap.get(queueId);
        if (messageList == null) {
            messageList = new ArrayList<>();
            List<InternalMessage> existing = queueMap.putIfAbsent(queueId, messageList);
            if (existing != null) {
                messageList = existing;
            }
        }

        message.setMsgId(msgId);
        long queueOffset = messageList.size();
        messageList.add(message);

        return PutResult.success(msgId, queueId, queueOffset);
    }

    @Override
    public PullResult pullMessage(String consumerGroup, String topic, int queueId, long queueOffset, int maxMsgNums, long suspendTimeoutMillis, String subscription, String expressionType) {
        ConcurrentHashMap<Integer, List<InternalMessage>> queueMap = topicQueueMessages.get(topic);
        if (queueMap == null) {
            return PullResult.notFound(queueOffset, 0, 0);
        }

        List<InternalMessage> messageList = queueMap.get(queueId);
        if (messageList == null || messageList.isEmpty()) {
            return PullResult.notFound(queueOffset, 0, 0);
        }

        if (queueOffset >= messageList.size()) {
            return PullResult.notFound(queueOffset, 0, messageList.size());
        }

        int endIndex = (int) Math.min(queueOffset + maxMsgNums, messageList.size());
        List<InternalMessage> messages = new ArrayList<>(messageList.subList((int) queueOffset, endIndex));

        long nextBeginOffset = endIndex;
        long minOffset = 0;
        long maxOffset = messageList.size();

        return PullResult.found(messages, nextBeginOffset, minOffset, maxOffset);
    }

    @Override
    public OffsetResult queryConsumerOffset(String consumerGroup, String topic, int queueId) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> topicMap = consumerOffsets.get(consumerGroup);
        if (topicMap != null) {
            ConcurrentHashMap<Integer, Long> queueOffsetMap = topicMap.get(topic);
            if (queueOffsetMap != null) {
                Long offset = queueOffsetMap.get(queueId);
                if (offset != null) {
                    return OffsetResult.success(offset);
                }
            }
        }
        return OffsetResult.fail(22, "offset not found");
    }

    @Override
    public void updateConsumerOffset(String consumerGroup, String topic, int queueId, long commitOffset) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> topicMap = consumerOffsets.get(consumerGroup);
        if (topicMap == null) {
            topicMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> existing = consumerOffsets.putIfAbsent(consumerGroup, topicMap);
            if (existing != null) {
                topicMap = existing;
            }
        }

        ConcurrentHashMap<Integer, Long> queueOffsetMap = topicMap.get(topic);
        if (queueOffsetMap == null) {
            queueOffsetMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, Long> existing = topicMap.putIfAbsent(topic, queueOffsetMap);
            if (existing != null) {
                queueOffsetMap = existing;
            }
        }

        queueOffsetMap.put(queueId, commitOffset);
    }

    @Override
    public boolean healthCheck() {
        return initialized;
    }

    @Override
    public RemotingCommand forwardToBroker(RemotingCommand request) throws Exception {
        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, null);
    }
}
