package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import com.mq.proxy.core.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import com.mq.proxy.core.protocol.header.UnregisterClientRequestHeader;
import com.mq.proxy.core.server.RemotingProcessor;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClientManageProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(ClientManageProcessor.class);

    private final ClientConnectionManager clientConnectionManager;
    private final ConcurrentHashMap<String, Integer> consumerGroupMemberCount = new ConcurrentHashMap<>();

    public ClientManageProcessor(ClientConnectionManager clientConnectionManager) {
        this.clientConnectionManager = clientConnectionManager;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        int requestCode = request.getCode();

        if (requestCode == RequestCode.HEART_BEAT) {
            return heartBeat(channel, request);
        } else if (requestCode == RequestCode.UNREGISTER_CLIENT) {
            return unregisterClient(channel, request);
        } else if (requestCode == RequestCode.GET_CONSUMER_LIST_BY_GROUP) {
            return getConsumerListByGroup(request);
        } else if (requestCode == RequestCode.NOTIFY_CONSUMER_IDS_CHANGED) {
            return notifyConsumerIdsChanged(request);
        } else {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, "unsupported request code");
        }
    }

    private RemotingCommand heartBeat(Channel channel, RemotingCommand request) {
        byte[] body = request.getBody();
        if (body != null && body.length > 0) {
            HeartbeatData heartbeatData = HeartbeatData.decode(body);
            String clientID = heartbeatData.getClientID();

            if (heartbeatData.getProducerDataSet() != null) {
                for (HeartbeatData.ProducerData producerData : heartbeatData.getProducerDataSet()) {
                    clientConnectionManager.registerProducer(channel, clientID, producerData.getGroupName());
                }
            }

            if (heartbeatData.getConsumerDataSet() != null) {
                for (HeartbeatData.ConsumerData consumerData : heartbeatData.getConsumerDataSet()) {
                    String group = consumerData.getGroupName();
                    clientConnectionManager.registerConsumer(channel, clientID, group,
                            null, null, null, consumerData.getSubscriptionDataSet());

                    int prevCount = consumerGroupMemberCount.getOrDefault(group, 0);
                    int currentCount = countGroupMembers(group);
                    consumerGroupMemberCount.put(group, currentCount);
                    if (currentCount != prevCount) {
                        log.info("Consumer group {} member count changed: {} -> {}, notifying all members",
                                group, prevCount, currentCount);
                        notifyGroupMembers(group);
                    }
                }
            }
        }

        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
    }

    private RemotingCommand unregisterClient(Channel channel, RemotingCommand request) {
        UnregisterClientRequestHeader requestHeader = parseUnregisterClientRequestHeader(request);
        String clientID = requestHeader.getClientID();

        String consumerGroup = requestHeader.getConsumerGroup();
        clientConnectionManager.unregisterClient(channel, clientID,
                requestHeader.getProducerGroup(), consumerGroup);

        if (consumerGroup != null) {
            int prevCount = consumerGroupMemberCount.getOrDefault(consumerGroup, 0);
            int currentCount = countGroupMembers(consumerGroup);
            consumerGroupMemberCount.put(consumerGroup, currentCount);
            if (currentCount != prevCount && currentCount > 0) {
                log.info("Consumer group {} member count changed after unregister: {} -> {}, notifying remaining members",
                        consumerGroup, prevCount, currentCount);
                notifyGroupMembers(consumerGroup);
            }
        }

        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
    }

    private RemotingCommand getConsumerListByGroup(RemotingCommand request) {
        String consumerGroup = null;
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            consumerGroup = extFields.get("consumerGroup");
        }

        List<HeartbeatData.ConsumerData> allConsumerData = clientConnectionManager.getAllConsumerData();
        java.util.List<String> consumerIdList = new java.util.ArrayList<>();
        if (consumerGroup != null) {
            for (HeartbeatData.ConsumerData data : allConsumerData) {
                if (consumerGroup.equals(data.getGroupName())) {
                    consumerIdList.add("proxy-client-" + data.getGroupName());
                }
            }
        }

        RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
        StringBuilder json = new StringBuilder("{\"consumerIdList\":[");
        for (int i = 0; i < consumerIdList.size(); i++) {
            if (i > 0) json.append(",");
            json.append("\"").append(consumerIdList.get(i)).append("\"");
        }
        json.append("]}");
        response.setBody(json.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return response;
    }

    private UnregisterClientRequestHeader parseUnregisterClientRequestHeader(RemotingCommand request) {
        UnregisterClientRequestHeader header = (UnregisterClientRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new UnregisterClientRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setClientID(extFields.get("clientID"));
            header.setProducerGroup(extFields.get("producerGroup"));
            header.setConsumerGroup(extFields.get("consumerGroup"));
        }
        return header;
    }

    public ClientConnectionManager getClientConnectionManager() {
        return clientConnectionManager;
    }

    private RemotingCommand notifyConsumerIdsChanged(RemotingCommand request) {
        // broker发来的NOTIFY_CONSUMER_IDS_CHANGED需要转发给SDK客户端
        // 但不应再次触发memberCount检测和递归通知
        NotifyConsumerIdsChangedRequestHeader header = parseNotifyConsumerIdsChangedRequestHeader(request);
        String consumerGroup = header.getConsumerGroup();

        if (consumerGroup != null) {
            log.info("Received NOTIFY_CONSUMER_IDS_CHANGED from broker for group {}, forwarding to SDK clients",
                    consumerGroup);
            notifyGroupMembers(consumerGroup);
        }

        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
    }

    private NotifyConsumerIdsChangedRequestHeader parseNotifyConsumerIdsChangedRequestHeader(RemotingCommand request) {
        NotifyConsumerIdsChangedRequestHeader header = (NotifyConsumerIdsChangedRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new NotifyConsumerIdsChangedRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setConsumerGroup(extFields.get("consumerGroup"));
        }
        return header;
    }

    private int countGroupMembers(String consumerGroup) {
        int count = 0;
        for (ClientConnectionManager.ClientInfo clientInfo : clientConnectionManager.getAllClientInfos()) {
            if (clientInfo.getConsumerGroups().contains(consumerGroup)) {
                count++;
            }
        }
        return count;
    }

    private void notifyGroupMembers(String consumerGroup) {
        RemotingCommand notification = RemotingCommand.createRequestCommand(
                RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", consumerGroup);
        notification.setExtFields(extFields);

        for (ClientConnectionManager.ClientInfo clientInfo : clientConnectionManager.getAllClientInfos()) {
            if (clientInfo.getConsumerGroups().contains(consumerGroup)) {
                Channel ch = clientInfo.getChannel();
                if (ch != null && ch.isActive()) {
                    ch.writeAndFlush(notification);
                    log.debug("Sent NOTIFY_CONSUMER_IDS_CHANGED to clientId={}, group={}",
                            clientInfo.getClientId(), consumerGroup);
                }
            }
        }
    }
}
