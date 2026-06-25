package com.mq.proxy.core.engine;

import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.storage.PullMessageCallback;
import com.mq.proxy.core.storage.PutMessageCallback;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MessageEngine {

    private static final Logger log = LoggerFactory.getLogger(MessageEngine.class);
    private static final long MASTER_BROKER_ID = 0L;
    private static final int FLAG_COMMIT_OFFSET = 0x1;
    private static final long PULL_FAILURE_ROUTE_REFRESH_INTERVAL_MILLIS = 1000L;

    private final StorageAdapter storageAdapter;
    private final ConcurrentHashMap<String, AtomicLong> pullFromWhichNodeTable =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> pullFailureRouteRefreshTime =
            new ConcurrentHashMap<>();
    private VirtualRouteManager virtualRouteManager;
    private NettyRemotingServer remotingServer;
    private Runnable onSubscriptionNotLatest;

    public MessageEngine(StorageAdapter storageAdapter) {
        this.storageAdapter = storageAdapter;
    }

    public void setOnSubscriptionNotLatest(Runnable onSubscriptionNotLatest) {
        this.onSubscriptionNotLatest = onSubscriptionNotLatest;
    }

    public void triggerHeartbeatForward() {
        if (onSubscriptionNotLatest != null) {
            onSubscriptionNotLatest.run();
        }
    }

    public void setVirtualRouteManager(VirtualRouteManager virtualRouteManager) {
        this.virtualRouteManager = virtualRouteManager;
    }

    public void setRemotingServer(NettyRemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public StorageAdapter getStorageAdapter() {
        return storageAdapter;
    }

    public PutResult putMessage(InternalMessage message) {
        try {
            String brokerAddr = resolveBrokerAddr(message.getBrokerName(), message.getTopic());
            return storageAdapter.putMessage(message, brokerAddr);
        } catch (Exception e) {
            return PutResult.fail(1, e.getMessage());
        }
    }

    public void putMessageAsync(final InternalMessage message, final PutMessageCallback callback) {
        final AtomicBoolean completed = new AtomicBoolean(false);
        final PutMessageCallback onceCallback = new PutMessageCallback() {
            @Override
            public void onSuccess(PutResult putResult) {
                if (completed.compareAndSet(false, true)) {
                    callback.onSuccess(putResult);
                }
            }

            @Override
            public void onException(Throwable throwable) {
                if (completed.compareAndSet(false, true)) {
                    callback.onException(throwable);
                }
            }
        };

        try {
            String brokerAddr = resolveBrokerAddr(message.getBrokerName(), message.getTopic());
            storageAdapter.putMessageAsync(message, brokerAddr, onceCallback);
        } catch (Throwable throwable) {
            onceCallback.onException(throwable);
        }
    }

    public PullResult pullMessage(String consumerGroup, String topic, int queueId, long queueOffset, int maxMsgNums, int sysFlag, long commitOffset, long suspendTimeoutMillis, String subscription, String expressionType, long subVersion, String brokerName) {
        PullBrokerResolution brokerResolution = null;
        try {
            brokerResolution = resolvePullBroker(consumerGroup, topic, queueId, brokerName, sysFlag);
            PullResult pullResult = storageAdapter.pullMessage(consumerGroup, topic, queueId, queueOffset,
                    maxMsgNums, brokerResolution.getSysFlag(), commitOffset, suspendTimeoutMillis,
                    subscription, expressionType, subVersion, brokerResolution.getBrokerAddr());
            updatePullFromWhichNode(brokerResolution.getPullFromWhichNodeKey(), pullResult);
            return pullResult;
        } catch (Exception e) {
            handlePullFailure(topic, brokerResolution);
            log.warn("pullMessage failed, returning system error with preserved offset. group={}, topic={}, queueId={}, queueOffset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeoutMillis={}, subscription={}, expressionType={}, subVersion={}, brokerName={}, brokerAddr={}, preservedNextBeginOffset={}",
                    consumerGroup, topic, queueId, queueOffset, maxMsgNums, sysFlag, commitOffset,
                    suspendTimeoutMillis, subscription, expressionType, subVersion, brokerName,
                    brokerResolution != null ? brokerResolution.getBrokerAddr() : null,
                    queueOffset, e);
            return PullResult.fail(RemotingSysResponseCode.SYSTEM_ERROR, queueOffset, 0, queueOffset);
        }
    }

    public void pullMessageAsync(final String consumerGroup, final String topic, final int queueId, final long queueOffset,
                                 final int maxMsgNums, final int sysFlag, final long commitOffset,
                                 final long suspendTimeoutMillis, final String subscription,
                                 final String expressionType, final long subVersion, final String brokerName,
                                 final PullMessageCallback callback) {
        PullBrokerResolution brokerResolution = null;
        try {
            brokerResolution = resolvePullBroker(consumerGroup, topic, queueId, brokerName, sysFlag);
            final PullBrokerResolution resolvedBroker = brokerResolution;
            storageAdapter.pullMessageAsync(consumerGroup, topic, queueId, queueOffset, maxMsgNums,
                    resolvedBroker.getSysFlag(), commitOffset, suspendTimeoutMillis, subscription, expressionType, subVersion,
                    resolvedBroker.getBrokerAddr(),
                    new PullMessageCallback() {
                        @Override
                        public void onSuccess(PullResult pullResult) {
                            updatePullFromWhichNode(resolvedBroker.getPullFromWhichNodeKey(), pullResult);
                            callback.onSuccess(pullResult);
                        }

                        @Override
                        public void onException(Throwable throwable) {
                            handlePullFailure(topic, resolvedBroker);
                            log.warn("pullMessage failed, returning system error with preserved offset. group={}, topic={}, queueId={}, queueOffset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeoutMillis={}, subscription={}, expressionType={}, subVersion={}, brokerName={}, brokerAddr={}, preservedNextBeginOffset={}",
                                    consumerGroup, topic, queueId, queueOffset, maxMsgNums, sysFlag, commitOffset,
                                    suspendTimeoutMillis, subscription, expressionType, subVersion, brokerName,
                                    resolvedBroker.getBrokerAddr(), queueOffset, throwable);
                            callback.onSuccess(PullResult.fail(RemotingSysResponseCode.SYSTEM_ERROR, queueOffset, 0, queueOffset));
                        }
                    });
        } catch (Exception e) {
            handlePullFailure(topic, brokerResolution);
            log.warn("pullMessage failed, returning system error with preserved offset. group={}, topic={}, queueId={}, queueOffset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeoutMillis={}, subscription={}, expressionType={}, subVersion={}, brokerName={}, brokerAddr={}, preservedNextBeginOffset={}",
                    consumerGroup, topic, queueId, queueOffset, maxMsgNums, sysFlag, commitOffset,
                    suspendTimeoutMillis, subscription, expressionType, subVersion, brokerName,
                    brokerResolution != null ? brokerResolution.getBrokerAddr() : null,
                    queueOffset, e);
            callback.onSuccess(PullResult.fail(RemotingSysResponseCode.SYSTEM_ERROR, queueOffset, 0, queueOffset));
        }
    }

    public OffsetResult queryConsumerOffset(String consumerGroup, String topic, int queueId, String brokerName) {
        try {
            String brokerAddr = resolveBrokerAddr(brokerName, topic);
            return storageAdapter.queryConsumerOffset(consumerGroup, topic, queueId, brokerAddr);
        } catch (Exception e) {
            return OffsetResult.fail(1, e.getMessage());
        }
    }

    public void updateConsumerOffset(String consumerGroup, String topic, int queueId, long commitOffset, String brokerName) {
        try {
            String brokerAddr = resolveBrokerAddr(brokerName, topic);
            storageAdapter.updateConsumerOffset(consumerGroup, topic, queueId, commitOffset, brokerAddr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String resolveBrokerAddr(String brokerName, String topic) {
        if (brokerName != null && virtualRouteManager != null) {
            String realAddr = virtualRouteManager.getRealBrokerAddr(brokerName);
            if (realAddr != null) {
                log.debug("resolveBrokerAddr: brokerName={}, realAddr={}", brokerName, realAddr);
                return realAddr;
            }
        }

        if (topic != null && virtualRouteManager != null) {
            TopicRouteInfo routeInfo = virtualRouteManager.getRouteInfoByTopic(topic);
            if (routeInfo != null && routeInfo.getBrokerDatas() != null && !routeInfo.getBrokerDatas().isEmpty()) {
                List<TopicRouteInfo.BrokerData> brokerDatas = routeInfo.getBrokerDatas();
                for (TopicRouteInfo.BrokerData brokerData : brokerDatas) {
                    String realAddr = virtualRouteManager.getRealBrokerAddr(brokerData.getBrokerName());
                    if (realAddr != null) {
                        log.debug("resolveBrokerAddr: topic={}, brokerName={}, realAddr={}", topic, brokerData.getBrokerName(), realAddr);
                        return realAddr;
                    }
                }
            }
        }

        if (virtualRouteManager != null) {
            List<String> allBrokers = virtualRouteManager.getAllRealBrokerAddrs();
            if (!allBrokers.isEmpty()) {
                String addr = allBrokers.get(0);
                log.warn("resolveBrokerAddr: fallback to first broker, addr={}", addr);
                return addr;
            }
        }

        if (virtualRouteManager != null) {
            throw new IllegalStateException("No route info found for brokerName=" + brokerName + ", topic=" + topic);
        }

        return null;
    }

    private PullBrokerResolution resolvePullBroker(String consumerGroup, String topic, int queueId,
                                                   String brokerName, int sysFlag) {
        if (topic != null && virtualRouteManager != null) {
            virtualRouteManager.getRouteInfoByTopic(topic);
        }

        String key = buildPullFromWhichNodeKey(consumerGroup, topic, queueId, brokerName);
        long brokerId = getPullFromWhichNode(key);
        String brokerAddr = null;
        boolean slave = false;

        if (brokerName != null && virtualRouteManager != null) {
            if (brokerId != MASTER_BROKER_ID) {
                brokerAddr = virtualRouteManager.getRealBrokerAddr(brokerName, brokerId);
                String masterAddr = virtualRouteManager.getRealBrokerAddr(brokerName);
                if (brokerAddr == null || (masterAddr != null && masterAddr.equals(brokerAddr))) {
                    brokerId = MASTER_BROKER_ID;
                    setPullFromWhichNode(key, MASTER_BROKER_ID);
                    brokerAddr = masterAddr;
                } else {
                    slave = true;
                }
            } else {
                brokerAddr = virtualRouteManager.getRealBrokerAddr(brokerName, MASTER_BROKER_ID);
                if (brokerAddr == null) {
                    brokerAddr = virtualRouteManager.getRealBrokerAddr(brokerName);
                }
            }
        }

        if (brokerAddr == null) {
            brokerAddr = resolveBrokerAddr(brokerName, topic);
        }

        if (brokerAddr == null && virtualRouteManager != null) {
            throw new IllegalStateException("No route info found for brokerName=" + brokerName + ", topic=" + topic);
        }

        int finalSysFlag = slave ? clearCommitOffsetFlag(sysFlag) : sysFlag;
        return new PullBrokerResolution(key, brokerName, brokerId, brokerAddr, finalSysFlag, slave);
    }

    private void updatePullFromWhichNode(String key, PullResult pullResult) {
        if (pullResult == null || pullResult.getSuggestWhichBrokerId() == null) {
            return;
        }
        try {
            setPullFromWhichNode(key, Long.parseLong(pullResult.getSuggestWhichBrokerId()));
        } catch (NumberFormatException e) {
            log.warn("ignore invalid suggestWhichBrokerId: key={}, suggestWhichBrokerId={}",
                    key, pullResult.getSuggestWhichBrokerId());
        }
    }

    private long getPullFromWhichNode(String key) {
        AtomicLong brokerId = this.pullFromWhichNodeTable.get(key);
        return brokerId != null ? brokerId.get() : MASTER_BROKER_ID;
    }

    private void setPullFromWhichNode(String key, long brokerId) {
        AtomicLong current = this.pullFromWhichNodeTable.get(key);
        if (current == null) {
            AtomicLong newValue = new AtomicLong(brokerId);
            AtomicLong previous = this.pullFromWhichNodeTable.putIfAbsent(key, newValue);
            current = previous != null ? previous : newValue;
        }
        current.set(brokerId);
    }

    private void handlePullFailure(String topic, PullBrokerResolution brokerResolution) {
        refreshRouteOnPullFailure(topic);
        if (brokerResolution != null && brokerResolution.isSlave()) {
            if (!hasRefreshedSlaveAddress(brokerResolution)) {
                setPullFromWhichNode(brokerResolution.getPullFromWhichNodeKey(), MASTER_BROKER_ID);
            }
        }
    }

    private boolean refreshRouteOnPullFailure(String topic) {
        if (virtualRouteManager == null || topic == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        AtomicLong lastRefreshTime = this.pullFailureRouteRefreshTime.get(topic);
        if (lastRefreshTime == null) {
            AtomicLong newValue = new AtomicLong(0L);
            AtomicLong previous = this.pullFailureRouteRefreshTime.putIfAbsent(topic, newValue);
            lastRefreshTime = previous != null ? previous : newValue;
        }
        while (true) {
            long previousRefreshTime = lastRefreshTime.get();
            if (now - previousRefreshTime < PULL_FAILURE_ROUTE_REFRESH_INTERVAL_MILLIS) {
                return false;
            }
            if (lastRefreshTime.compareAndSet(previousRefreshTime, now)) {
                try {
                    virtualRouteManager.refreshRouteInfoByTopic(topic);
                    return true;
                } catch (Exception e) {
                    log.warn("refresh route after pull failure failed. topic={}, error={}", topic, e.getMessage());
                    return false;
                }
            }
        }
    }

    private boolean hasRefreshedSlaveAddress(PullBrokerResolution brokerResolution) {
        if (virtualRouteManager == null || brokerResolution.getBrokerName() == null) {
            return false;
        }
        String refreshedAddr = virtualRouteManager.getRealBrokerAddr(
                brokerResolution.getBrokerName(), brokerResolution.getBrokerId());
        String masterAddr = virtualRouteManager.getRealBrokerAddr(brokerResolution.getBrokerName());
        return refreshedAddr != null
                && !refreshedAddr.equals(brokerResolution.getBrokerAddr())
                && (masterAddr == null || !masterAddr.equals(refreshedAddr));
    }

    private String buildPullFromWhichNodeKey(String consumerGroup, String topic, int queueId, String brokerName) {
        return String.valueOf(consumerGroup) + "|" + String.valueOf(topic) + "|" + queueId
                + "|" + String.valueOf(brokerName);
    }

    private int clearCommitOffsetFlag(int sysFlag) {
        return sysFlag & ~FLAG_COMMIT_OFFSET;
    }

    public TopicRouteInfo getRouteInfoByTopic(String topic) {
        if (virtualRouteManager != null) {
            return virtualRouteManager.getRouteInfoByTopic(topic);
        }
        return null;
    }

    private static class PullBrokerResolution {
        private final String pullFromWhichNodeKey;
        private final String brokerName;
        private final long brokerId;
        private final String brokerAddr;
        private final int sysFlag;
        private final boolean slave;

        private PullBrokerResolution(String pullFromWhichNodeKey, String brokerName, long brokerId,
                                     String brokerAddr, int sysFlag, boolean slave) {
            this.pullFromWhichNodeKey = pullFromWhichNodeKey;
            this.brokerName = brokerName;
            this.brokerId = brokerId;
            this.brokerAddr = brokerAddr;
            this.sysFlag = sysFlag;
            this.slave = slave;
        }

        private String getPullFromWhichNodeKey() {
            return pullFromWhichNodeKey;
        }

        private String getBrokerName() {
            return brokerName;
        }

        private long getBrokerId() {
            return brokerId;
        }

        private String getBrokerAddr() {
            return brokerAddr;
        }

        private int getSysFlag() {
            return sysFlag;
        }

        private boolean isSlave() {
            return slave;
        }
    }
}
