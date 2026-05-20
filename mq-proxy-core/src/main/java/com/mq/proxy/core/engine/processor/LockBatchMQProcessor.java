package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.body.LockBatchRequestBody;
import com.mq.proxy.core.protocol.body.LockBatchResponseBody;
import com.mq.proxy.core.protocol.body.MessageQueue;
import com.mq.proxy.core.protocol.body.UnlockBatchRequestBody;
import com.mq.proxy.core.server.RemotingProcessor;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockBatchMQProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(LockBatchMQProcessor.class);

    private final ClientConnectionManager clientConnectionManager;

    private final ConcurrentHashMap<String, ConcurrentHashMap<MessageQueue, LockEntry>> lockTable = new ConcurrentHashMap<>();

    public LockBatchMQProcessor(ClientConnectionManager clientConnectionManager) {
        this.clientConnectionManager = clientConnectionManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        int code = request.getCode();
        if (code == RequestCode.LOCK_BATCH_MQ) {
            return lockBatchMQ(channel, request);
        } else if (code == RequestCode.UNLOCK_BATCH_MQ) {
            return unlockBatchMQ(channel, request);
        } else {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, "unsupported request code");
        }
    }

    private RemotingCommand lockBatchMQ(Channel channel, RemotingCommand request) throws Exception {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "request body is empty");
        }

        LockBatchRequestBody requestBody = LockBatchRequestBody.decode(body);
        String group = requestBody.getConsumerGroup();
        String clientId = requestBody.getClientId();
        Set<MessageQueue> mqSet = requestBody.getMqSet();

        if (group == null || mqSet == null || mqSet.isEmpty()) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "invalid request parameters");
        }

        log.info("LockBatchMQ request: group={}, clientId={}, mqSet={}", group, clientId, mqSet);

        Set<MessageQueue> lockOKMQSet = new HashSet<>();
        long currentTime = System.currentTimeMillis();

        ConcurrentHashMap<MessageQueue, LockEntry> groupLocks = lockTable.computeIfAbsent(group, k -> new ConcurrentHashMap<>());

        for (MessageQueue mq : mqSet) {
            LockEntry lockEntry = groupLocks.get(mq);
            if (lockEntry == null) {
                lockEntry = new LockEntry(clientId, channel, currentTime);
                groupLocks.put(mq, lockEntry);
                lockOKMQSet.add(mq);
                log.info("Lock success: mq={}, clientId={}", mq, clientId);
            } else if (lockEntry.isOwnedBy(clientId)) {
                lockEntry.updateTimestamp(currentTime);
                lockOKMQSet.add(mq);
                log.info("Lock renew: mq={}, clientId={}", mq, clientId);
            } else if (lockEntry.isExpired(currentTime)) {
                lockEntry.update(clientId, channel, currentTime);
                lockOKMQSet.add(mq);
                log.info("Lock takeover: mq={}, oldClientId={}, newClientId={}", mq, lockEntry.getClientId(), clientId);
            } else {
                log.warn("Lock fail: mq={}, locked by clientId={}, lockTime={}", mq, lockEntry.getClientId(), lockEntry.getLockTime());
            }
        }

        LockBatchResponseBody responseBody = new LockBatchResponseBody();
        responseBody.setLockOKMQSet(lockOKMQSet);

        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        response.setBody(responseBody.encode());
        return response;
    }

    private RemotingCommand unlockBatchMQ(Channel channel, RemotingCommand request) throws Exception {
        byte[] body = request.getBody();
        if (body == null || body.length == 0) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "request body is empty");
        }

        UnlockBatchRequestBody requestBody = UnlockBatchRequestBody.decode(body);
        String group = requestBody.getConsumerGroup();
        String clientId = requestBody.getClientId();
        Set<MessageQueue> mqSet = requestBody.getMqSet();

        if (group == null || mqSet == null || mqSet.isEmpty()) {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "invalid request parameters");
        }

        log.info("UnlockBatchMQ request: group={}, clientId={}, mqSet={}", group, clientId, mqSet);

        ConcurrentHashMap<MessageQueue, LockEntry> groupLocks = lockTable.get(group);
        if (groupLocks != null) {
            for (MessageQueue mq : mqSet) {
                LockEntry lockEntry = groupLocks.get(mq);
                if (lockEntry != null && lockEntry.isOwnedBy(clientId)) {
                    groupLocks.remove(mq);
                    log.info("Unlock success: mq={}, clientId={}", mq, clientId);
                } else {
                    log.warn("Unlock fail: mq={}, not owned by clientId={}", mq, clientId);
                }
            }
        }

        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        return response;
    }

    public boolean isLockOwned(String group, MessageQueue mq, String clientId) {
        ConcurrentHashMap<MessageQueue, LockEntry> groupLocks = lockTable.get(group);
        if (groupLocks == null) {
            return true;
        }
        LockEntry lockEntry = groupLocks.get(mq);
        if (lockEntry == null) {
            return true;
        }
        return lockEntry.isOwnedBy(clientId);
    }

    private static class LockEntry {
        private String clientId;
        private Channel channel;
        private long lockTime;

        public LockEntry(String clientId, Channel channel, long lockTime) {
            this.clientId = clientId;
            this.channel = channel;
            this.lockTime = lockTime;
        }

        public boolean isOwnedBy(String clientId) {
            return this.clientId != null && this.clientId.equals(clientId);
        }

        public boolean isExpired(long currentTime) {
            return currentTime - this.lockTime > 30000;
        }

        public void updateTimestamp(long currentTime) {
            this.lockTime = currentTime;
        }

        public void update(String clientId, Channel channel, long lockTime) {
            this.clientId = clientId;
            this.channel = channel;
            this.lockTime = lockTime;
        }

        public String getClientId() {
            return clientId;
        }

        public long getLockTime() {
            return lockTime;
        }

        public Channel getChannel() {
            return channel;
        }
    }
}