package com.mq.proxy.core.engine;

import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class ClientConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ClientConnectionManager.class);

    private final ConcurrentHashMap<Channel, ClientInfo> channelClientMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Channel> clientIdChannelMap = new ConcurrentHashMap<>();
    private final List<Consumer<Channel>> channelInactiveListeners = new CopyOnWriteArrayList<>();
    private final List<Consumer<ClientInfo>> clientInactiveListeners = new CopyOnWriteArrayList<>();

    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;

    public void addChannelInactiveListener(Consumer<Channel> listener) {
        channelInactiveListeners.add(listener);
    }

    public void addClientInactiveListener(Consumer<ClientInfo> listener) {
        clientInactiveListeners.add(listener);
    }

    public void registerProducer(Channel channel, String clientId, String producerGroup) {
        ClientInfo clientInfo = channelClientMap.computeIfAbsent(channel, k -> new ClientInfo(channel, clientId));
        clientInfo.setClientId(clientId);
        clientInfo.addProducerGroup(producerGroup);
        clientInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        clientIdChannelMap.put(clientId, channel);
        log.info("Producer registered: clientId={}, group={}, channel={}", clientId, producerGroup, channel.remoteAddress());
    }

    public void registerConsumer(Channel channel, String clientId, String consumerGroup,
                                  String consumeType, String messageModel, String consumeFromWhere,
                                  Set<HeartbeatData.SubscriptionData> subscriptionDataSet) {
        ClientInfo clientInfo = channelClientMap.computeIfAbsent(channel, k -> new ClientInfo(channel, clientId));
        clientInfo.setClientId(clientId);
        clientInfo.addConsumerGroup(consumerGroup);
        clientInfo.setGroupConsumeType(consumerGroup, consumeType);
        clientInfo.setGroupMessageModel(consumerGroup, messageModel);
        clientInfo.setGroupConsumeFromWhere(consumerGroup, consumeFromWhere);
        // 保留全局字段更新（向后兼容）
        if (consumeType != null) clientInfo.setConsumeType(consumeType);
        if (messageModel != null) clientInfo.setMessageModel(messageModel);
        if (consumeFromWhere != null) clientInfo.setConsumeFromWhere(consumeFromWhere);
        if (subscriptionDataSet != null) {
            clientInfo.updateSubscriptions(consumerGroup, subscriptionDataSet);
        }
        clientInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        clientIdChannelMap.put(clientId, channel);
        log.info("Consumer registered: clientId={}, group={}, messageModel={}, channel={}",
                clientId, consumerGroup, clientInfo.getGroupMessageModel(consumerGroup), channel.remoteAddress());
    }

    public ClientInfo unregisterClient(Channel channel, String clientId, String producerGroup, String consumerGroup) {
        ClientInfo clientInfo = channelClientMap.get(channel);
        if (clientInfo == null) {
            clientInfo = findClientById(clientId);
        }
        if (clientInfo == null) {
            return null;
        }
        ClientInfo snapshot = clientInfo.snapshot();

        if (producerGroup != null) {
            clientInfo.removeProducerGroup(producerGroup);
        }
        if (consumerGroup != null) {
            clientInfo.removeConsumerGroup(consumerGroup);
            clientInfo.removeSubscriptions(consumerGroup);
            clientInfo.removeGroupMetadata(consumerGroup);
        }

        if (clientInfo.isEmpty()) {
            channelClientMap.remove(channel);
            clientIdChannelMap.remove(clientInfo.getClientId());
            log.info("Client unregistered and removed: clientId={}, channel={}", clientInfo.getClientId(), channel.remoteAddress());
        } else {
            log.info("Client partially unregistered: clientId={}, remaining producerGroups={}, consumerGroups={}",
                    clientInfo.getClientId(), clientInfo.getProducerGroups(), clientInfo.getConsumerGroups());
        }
        return snapshot;
    }

    public void onChannelInactive(Channel channel) {
        ClientInfo clientInfo = channelClientMap.remove(channel);
        if (clientInfo != null) {
            clientIdChannelMap.remove(clientInfo.getClientId());
            log.info("Channel inactive, client removed: clientId={}, channel={}, producerGroups={}, consumerGroups={}",
                    clientInfo.getClientId(), channel.remoteAddress(),
                    clientInfo.getProducerGroups(), clientInfo.getConsumerGroups());
            for (Consumer<ClientInfo> listener : clientInactiveListeners) {
                listener.accept(clientInfo);
            }
        } else {
            // clientInfo已被scanNotActiveChannel移除，但仍需清理可能残留的clientIdChannelMap映射
            // 通过遍历查找该channel对应的stale entry
            clientIdChannelMap.entrySet().removeIf(entry -> entry.getValue() == channel);
        }
        for (Consumer<Channel> listener : channelInactiveListeners) {
            listener.accept(channel);
        }
    }

    public void scanNotActiveChannel() {
        for (ConcurrentHashMap.Entry<Channel, ClientInfo> entry : channelClientMap.entrySet()) {
            Channel channel = entry.getKey();
            ClientInfo clientInfo = entry.getValue();
            if (!channel.isActive()) {
                channelClientMap.remove(channel);
                clientIdChannelMap.remove(clientInfo.getClientId());
                log.info("Scanned and removed inactive channel: clientId={}, channel={}",
                        clientInfo.getClientId(), channel.remoteAddress());
                for (Consumer<ClientInfo> listener : clientInactiveListeners) {
                    listener.accept(clientInfo);
                }
                continue;
            }
            long diff = System.currentTimeMillis() - clientInfo.getLastUpdateTimestamp();
            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                channelClientMap.remove(channel);
                clientIdChannelMap.remove(clientInfo.getClientId());
                channel.close();
                log.info("Scanned and removed expired channel: clientId={}, channel={}, expiredMs={}",
                        clientInfo.getClientId(), channel.remoteAddress(), diff);
                for (Consumer<ClientInfo> listener : clientInactiveListeners) {
                    listener.accept(clientInfo);
                }
            }
        }
    }

    public Set<String> getAllProducerGroups() {
        Set<String> groups = new HashSet<>();
        for (ClientInfo clientInfo : channelClientMap.values()) {
            groups.addAll(clientInfo.getProducerGroups());
        }
        return groups;
    }

    public Set<String> getAllConsumerGroups() {
        Set<String> groups = new HashSet<>();
        for (ClientInfo clientInfo : channelClientMap.values()) {
            groups.addAll(clientInfo.getConsumerGroups());
        }
        return groups;
    }

    public List<HeartbeatData.ConsumerData> getAllConsumerData() {
        List<HeartbeatData.ConsumerData> result = new ArrayList<>();
        for (ClientInfo clientInfo : channelClientMap.values()) {
            for (String group : clientInfo.getConsumerGroups()) {
                HeartbeatData.ConsumerData consumerData = new HeartbeatData.ConsumerData();
                consumerData.setGroupName(group);
                consumerData.setConsumeType(clientInfo.getGroupConsumeType(group));
                consumerData.setMessageModel(clientInfo.getGroupMessageModel(group));
                consumerData.setConsumeFromWhere(clientInfo.getGroupConsumeFromWhere(group));
                Set<HeartbeatData.SubscriptionData> subs = clientInfo.getSubscriptions(group);
                if (subs != null) {
                    Set<HeartbeatData.SubscriptionData> newSubs = new HashSet<>();
                    for (HeartbeatData.SubscriptionData sub : subs) {
                        HeartbeatData.SubscriptionData newSub = new HeartbeatData.SubscriptionData();
                        newSub.setTopic(sub.getTopic());
                        newSub.setSubString(sub.getSubString());
                        newSub.setSubVersion(sub.getSubVersion());
                        newSub.setClassFilterMode(sub.isClassFilterMode());
                        newSub.setExpressionType(sub.getExpressionType());
                        populateTagsAndCodes(newSub, sub.getSubString());
                        newSubs.add(newSub);
                    }
                    consumerData.setSubscriptionDataSet(newSubs);
                }
                result.add(consumerData);
            }
        }
        return result;
    }

    public List<HeartbeatData.ProducerData> getAllProducerData() {
        List<HeartbeatData.ProducerData> result = new ArrayList<>();
        for (ClientInfo clientInfo : channelClientMap.values()) {
            for (String group : clientInfo.getProducerGroups()) {
                HeartbeatData.ProducerData producerData = new HeartbeatData.ProducerData();
                producerData.setGroupName(group);
                result.add(producerData);
            }
        }
        return result;
    }

    public boolean hasAnyClients() {
        return !channelClientMap.isEmpty();
    }

    public boolean isBroadcastGroup(String consumerGroup) {
        for (ClientInfo clientInfo : channelClientMap.values()) {
            if (clientInfo.getConsumerGroups().contains(consumerGroup)) {
                String model = clientInfo.getGroupMessageModel(consumerGroup);
                if ("BROADCASTING".equals(model)) {
                    return true;
                }
            }
        }
        return false;
    }

    public int getChannelCount() {
        return channelClientMap.size();
    }

    public List<ClientInfo> getAllClientInfos() {
        return new ArrayList<>(channelClientMap.values());
    }

    public ClientInfo getClientInfo(String clientId) {
        return findClientById(clientId);
    }

    private ClientInfo findClientById(String clientId) {
        if (clientId == null) return null;
        Channel channel = clientIdChannelMap.get(clientId);
        if (channel != null) {
            return channelClientMap.get(channel);
        }
        for (ClientInfo info : channelClientMap.values()) {
            if (clientId.equals(info.getClientId())) {
                return info;
            }
        }
        return null;
    }

    private void populateTagsAndCodes(HeartbeatData.SubscriptionData subData, String subString) {
        if (subString == null || subString.isEmpty() || "*".equals(subString)) {
            subData.setTagsSet(Collections.emptySet());
            subData.setCodeSet(Collections.emptySet());
            return;
        }
        Set<String> tagsSet = new HashSet<>();
        Set<Integer> codeSet = new HashSet<>();
        if (subString.contains("||")) {
            String[] tags = subString.split("\\|\\|");
            for (String tag : tags) {
                String trimmed = tag.trim();
                if (!trimmed.isEmpty()) {
                    tagsSet.add(trimmed);
                    codeSet.add(trimmed.hashCode());
                }
            }
        } else {
            tagsSet.add(subString);
            codeSet.add(subString.hashCode());
        }
        subData.setTagsSet(tagsSet);
        subData.setCodeSet(codeSet);
    }

    public static class ClientInfo {
        private final Channel channel;
        private String clientId;
        private final Set<String> producerGroups = Collections.synchronizedSet(new HashSet<>());
        private final Set<String> consumerGroups = Collections.synchronizedSet(new HashSet<>());
        private final ConcurrentHashMap<String, Set<HeartbeatData.SubscriptionData>> subscriptionTable = new ConcurrentHashMap<>();
        private volatile String consumeType = "CONSUME_PASSIVELY";
        private volatile String messageModel = "CLUSTERING";
        private volatile String consumeFromWhere = "CONSUME_FROM_LAST_OFFSET";
        private volatile long lastUpdateTimestamp = System.currentTimeMillis();
        // per-group 消费模式映射
        private final ConcurrentHashMap<String, String> groupConsumeTypeTable = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, String> groupMessageModelTable = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, String> groupConsumeFromWhereTable = new ConcurrentHashMap<>();

        public ClientInfo(Channel channel, String clientId) {
            this.channel = channel;
            this.clientId = clientId;
        }

        public void addProducerGroup(String group) { producerGroups.add(group); }
        public void removeProducerGroup(String group) { producerGroups.remove(group); }
        public void addConsumerGroup(String group) { consumerGroups.add(group); }
        public void removeConsumerGroup(String group) { consumerGroups.remove(group); }

        public void updateSubscriptions(String group, Set<HeartbeatData.SubscriptionData> subs) {
            subscriptionTable.put(group, new HashSet<>(subs));
        }
        public void removeSubscriptions(String group) { subscriptionTable.remove(group); }
        public Set<HeartbeatData.SubscriptionData> getSubscriptions(String group) { return subscriptionTable.get(group); }

        // per-group 消费模式方法
        public void setGroupConsumeType(String group, String consumeType) {
            if (consumeType != null) groupConsumeTypeTable.put(group, consumeType);
        }
        public String getGroupConsumeType(String group) {
            return groupConsumeTypeTable.getOrDefault(group, consumeType);
        }
        public void setGroupMessageModel(String group, String messageModel) {
            if (messageModel != null) groupMessageModelTable.put(group, messageModel);
        }
        public String getGroupMessageModel(String group) {
            return groupMessageModelTable.getOrDefault(group, messageModel);
        }
        public void setGroupConsumeFromWhere(String group, String consumeFromWhere) {
            if (consumeFromWhere != null) groupConsumeFromWhereTable.put(group, consumeFromWhere);
        }
        public String getGroupConsumeFromWhere(String group) {
            return groupConsumeFromWhereTable.getOrDefault(group, consumeFromWhere);
        }
        public void removeGroupMetadata(String group) {
            groupConsumeTypeTable.remove(group);
            groupMessageModelTable.remove(group);
            groupConsumeFromWhereTable.remove(group);
        }

        public boolean isEmpty() { return producerGroups.isEmpty() && consumerGroups.isEmpty(); }

        public Channel getChannel() { return channel; }
        public String getClientId() { return clientId; }
        public void setClientId(String clientId) { this.clientId = clientId; }
        public Set<String> getProducerGroups() { return producerGroups; }
        public Set<String> getConsumerGroups() { return consumerGroups; }
        public String getConsumeType() { return consumeType; }
        public void setConsumeType(String consumeType) { this.consumeType = consumeType; }
        public String getMessageModel() { return messageModel; }
        public void setMessageModel(String messageModel) { this.messageModel = messageModel; }
        public String getConsumeFromWhere() { return consumeFromWhere; }
        public void setConsumeFromWhere(String consumeFromWhere) { this.consumeFromWhere = consumeFromWhere; }
        public long getLastUpdateTimestamp() { return lastUpdateTimestamp; }
        public void setLastUpdateTimestamp(long lastUpdateTimestamp) { this.lastUpdateTimestamp = lastUpdateTimestamp; }

        private ClientInfo snapshot() {
            ClientInfo copy = new ClientInfo(channel, clientId);
            copy.producerGroups.addAll(this.producerGroups);
            copy.consumerGroups.addAll(this.consumerGroups);
            for (ConcurrentHashMap.Entry<String, Set<HeartbeatData.SubscriptionData>> entry : this.subscriptionTable.entrySet()) {
                copy.subscriptionTable.put(entry.getKey(), new HashSet<>(entry.getValue()));
            }
            copy.consumeType = this.consumeType;
            copy.messageModel = this.messageModel;
            copy.consumeFromWhere = this.consumeFromWhere;
            copy.lastUpdateTimestamp = this.lastUpdateTimestamp;
            copy.groupConsumeTypeTable.putAll(this.groupConsumeTypeTable);
            copy.groupMessageModelTable.putAll(this.groupMessageModelTable);
            copy.groupConsumeFromWhereTable.putAll(this.groupConsumeFromWhereTable);
            return copy;
        }
    }
}
