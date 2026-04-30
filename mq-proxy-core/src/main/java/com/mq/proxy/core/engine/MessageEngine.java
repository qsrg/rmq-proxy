package com.mq.proxy.core.engine;

import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.storage.StorageAdapter;
import com.mq.proxy.core.storage.StorageAdapterManager;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.OffsetResult;
import com.mq.proxy.core.storage.model.PullResult;
import com.mq.proxy.core.storage.model.PutResult;
import com.mq.proxy.core.storage.model.TopicRouteInfo;

public class MessageEngine {

    private final StorageAdapterManager storageAdapterManager;
    private VirtualRouteManager virtualRouteManager;
    private NettyRemotingServer remotingServer;

    public MessageEngine(StorageAdapterManager storageAdapterManager) {
        this.storageAdapterManager = storageAdapterManager;
    }

    public void setVirtualRouteManager(VirtualRouteManager virtualRouteManager) {
        this.virtualRouteManager = virtualRouteManager;
    }

    public void setRemotingServer(NettyRemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public StorageAdapter getStorageAdapter(String topic) {
        return storageAdapterManager.getAdapterByTopic(topic);
    }

    public PutResult putMessage(InternalMessage message) {
        try {
            StorageAdapter adapter = getStorageAdapter(message.getTopic());
            if (adapter == null) {
                return PutResult.fail(14, "no storage adapter available");
            }
            return adapter.putMessage(message);
        } catch (Exception e) {
            return PutResult.fail(1, e.getMessage());
        }
    }

    public PullResult pullMessage(String consumerGroup, String topic, int queueId, long queueOffset, int maxMsgNums, long suspendTimeoutMillis, String subscription, String expressionType) {
        try {
            StorageAdapter adapter = getStorageAdapter(topic);
            if (adapter == null) {
                return PullResult.notFound(0, 0, 0);
            }
            return adapter.pullMessage(consumerGroup, topic, queueId, queueOffset, maxMsgNums, suspendTimeoutMillis, subscription, expressionType);
        } catch (Exception e) {
            return PullResult.notFound(0, 0, 0);
        }
    }

    public OffsetResult queryConsumerOffset(String consumerGroup, String topic, int queueId) {
        try {
            StorageAdapter adapter = getStorageAdapter(topic);
            if (adapter == null) {
                return OffsetResult.fail(1, "no storage adapter available");
            }
            return adapter.queryConsumerOffset(consumerGroup, topic, queueId);
        } catch (Exception e) {
            return OffsetResult.fail(1, e.getMessage());
        }
    }

    public void updateConsumerOffset(String consumerGroup, String topic, int queueId, long commitOffset) {
        try {
            StorageAdapter adapter = getStorageAdapter(topic);
            if (adapter != null) {
                adapter.updateConsumerOffset(consumerGroup, topic, queueId, commitOffset);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TopicRouteInfo getRouteInfoByTopic(String topic) {
        if (virtualRouteManager != null) {
            return virtualRouteManager.getRouteInfoByTopic(topic);
        }
        return null;
    }

    public StorageAdapter getDefaultStorageAdapter() {
        return storageAdapterManager.getAdapterByTopic(null);
    }
}
