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
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageEngine {

    private static final Logger log = LoggerFactory.getLogger(MessageEngine.class);

    private final StorageAdapter storageAdapter;
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
        String brokerAddr = null;
        try {
            brokerAddr = resolveBrokerAddr(brokerName, topic);
            return storageAdapter.pullMessage(consumerGroup, topic, queueId, queueOffset, maxMsgNums, sysFlag, commitOffset, suspendTimeoutMillis, subscription, expressionType, subVersion, brokerAddr);
        } catch (Exception e) {
            log.warn("pullMessage failed, returning system error with preserved offset. group={}, topic={}, queueId={}, queueOffset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeoutMillis={}, subscription={}, expressionType={}, subVersion={}, brokerName={}, brokerAddr={}, preservedNextBeginOffset={}",
                    consumerGroup, topic, queueId, queueOffset, maxMsgNums, sysFlag, commitOffset,
                    suspendTimeoutMillis, subscription, expressionType, subVersion, brokerName, brokerAddr,
                    queueOffset, e);
            return PullResult.fail(RemotingSysResponseCode.SYSTEM_ERROR, queueOffset, 0, queueOffset);
        }
    }

    public void pullMessageAsync(final String consumerGroup, final String topic, final int queueId, final long queueOffset,
                                 final int maxMsgNums, final int sysFlag, final long commitOffset,
                                 final long suspendTimeoutMillis, final String subscription,
                                 final String expressionType, final long subVersion, final String brokerName,
                                 final PullMessageCallback callback) {
        String brokerAddr = null;
        try {
            brokerAddr = resolveBrokerAddr(brokerName, topic);
            final String resolvedBrokerAddr = brokerAddr;
            storageAdapter.pullMessageAsync(consumerGroup, topic, queueId, queueOffset, maxMsgNums, sysFlag,
                    commitOffset, suspendTimeoutMillis, subscription, expressionType, subVersion, brokerAddr,
                    new PullMessageCallback() {
                        @Override
                        public void onSuccess(PullResult pullResult) {
                            callback.onSuccess(pullResult);
                        }

                        @Override
                        public void onException(Throwable throwable) {
                            log.warn("pullMessage failed, returning system error with preserved offset. group={}, topic={}, queueId={}, queueOffset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeoutMillis={}, subscription={}, expressionType={}, subVersion={}, brokerName={}, brokerAddr={}, preservedNextBeginOffset={}",
                                    consumerGroup, topic, queueId, queueOffset, maxMsgNums, sysFlag, commitOffset,
                                    suspendTimeoutMillis, subscription, expressionType, subVersion, brokerName,
                                    resolvedBrokerAddr, queueOffset, throwable);
                            callback.onSuccess(PullResult.fail(RemotingSysResponseCode.SYSTEM_ERROR, queueOffset, 0, queueOffset));
                        }
                    });
        } catch (Exception e) {
            log.warn("pullMessage failed, returning system error with preserved offset. group={}, topic={}, queueId={}, queueOffset={}, maxMsgNums={}, sysFlag={}, commitOffset={}, suspendTimeoutMillis={}, subscription={}, expressionType={}, subVersion={}, brokerName={}, brokerAddr={}, preservedNextBeginOffset={}",
                    consumerGroup, topic, queueId, queueOffset, maxMsgNums, sysFlag, commitOffset,
                    suspendTimeoutMillis, subscription, expressionType, subVersion, brokerName, brokerAddr,
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

    public TopicRouteInfo getRouteInfoByTopic(String topic) {
        if (virtualRouteManager != null) {
            return virtualRouteManager.getRouteInfoByTopic(topic);
        }
        return null;
    }
}
