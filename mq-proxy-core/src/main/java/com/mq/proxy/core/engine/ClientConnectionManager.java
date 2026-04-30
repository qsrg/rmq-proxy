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

public class ClientConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ClientConnectionManager.class);

    private final ConcurrentHashMap<Channel, ClientInfo> channelClientMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Channel> clientIdChannelMap = new ConcurrentHashMap<>();

    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;

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
        if (consumeType != null) clientInfo.setConsumeType(consumeType);
        if (messageModel != null) clientInfo.setMessageModel(messageModel);
        if (consumeFromWhere != null) clientInfo.setConsumeFromWhere(consumeFromWhere);
        if (subscriptionDataSet != null) {
            clientInfo.updateSubscriptions(consumerGroup, subscriptionDataSet);
        }
        clientInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        clientIdChannelMap.put(clientId, channel);
        log.info("Consumer registered: clientId={}, group={}, channel={}", clientId, consumerGroup, channel.remoteAddress());
    }

    public ClientInfo unregisterClient(Channel channel, String clientId, String producerGroup, String consumerGroup) {
        ClientInfo clientInfo = channelClientMap.get(channel);
        if (clientInfo == null) {
            clientInfo = findClientById(clientId);
        }
        if (clientInfo == null) {
            return null;
        }

        if (producerGroup != null) {
            clientInfo.removeProducerGroup(producerGroup);
        }
        if (consumerGroup != null) {
            clientInfo.removeConsumerGroup(consumerGroup);
            clientInfo.removeSubscriptions(consumerGroup);
        }

        if (clientInfo.isEmpty()) {
            channelClientMap.remove(channel);
            clientIdChannelMap.remove(clientInfo.getClientId());
            log.info("Client unregistered and removed: clientId={}, channel={}", clientInfo.getClientId(), channel.remoteAddress());
        } else {
            log.info("Client partially unregistered: clientId={}, remaining producerGroups={}, consumerGroups={}",
                    clientInfo.getClientId(), clientInfo.getProducerGroups(), clientInfo.getConsumerGroups());
        }
        return clientInfo;
    }

    public void onChannelInactive(Channel channel) {
        ClientInfo clientInfo = channelClientMap.remove(channel);
        if (clientInfo != null) {
            clientIdChannelMap.remove(clientInfo.getClientId());
            log.info("Channel inactive, client removed: clientId={}, channel={}, producerGroups={}, consumerGroups={}",
                    clientInfo.getClientId(), channel.remoteAddress(),
                    clientInfo.getProducerGroups(), clientInfo.getConsumerGroups());
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
                continue;
            }
            long diff = System.currentTimeMillis() - clientInfo.getLastUpdateTimestamp();
            if (diff > CHANNEL_EXPIRED_TIMEOUT) {
                channelClientMap.remove(channel);
                clientIdChannelMap.remove(clientInfo.getClientId());
                channel.close();
                log.info("Scanned and removed expired channel: clientId={}, channel={}, expiredMs={}",
                        clientInfo.getClientId(), channel.remoteAddress(), diff);
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
                Set<HeartbeatData.SubscriptionData> subs = clientInfo.getSubscriptions(group);
                if (subs != null) {
                    consumerData.setSubscriptionDataSet(new HashSet<>(subs));
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

    public int getChannelCount() {
        return channelClientMap.size();
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
    }
}
