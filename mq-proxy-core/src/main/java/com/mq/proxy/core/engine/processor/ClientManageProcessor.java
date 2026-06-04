package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.UpstreamConsumerSessionManager;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientManageProcessor implements RemotingProcessor {

    private static final Logger log = LoggerFactory.getLogger(ClientManageProcessor.class);

    private final ClientConnectionManager clientConnectionManager;
    private UpstreamConsumerSessionManager upstreamConsumerSessionManager;
    private Runnable onConsumerRegistered;
    private final ExecutorService callbackExecutor = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(1000), new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "ClientManageCallbackThread_" + threadNumber.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    });

    public ClientManageProcessor(ClientConnectionManager clientConnectionManager) {
        this.clientConnectionManager = clientConnectionManager;
    }

    public void setOnConsumerRegistered(Runnable onConsumerRegistered) {
        this.onConsumerRegistered = onConsumerRegistered;
    }

    public void setUpstreamConsumerSessionManager(UpstreamConsumerSessionManager upstreamConsumerSessionManager) {
        this.upstreamConsumerSessionManager = upstreamConsumerSessionManager;
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
                            consumerData.getConsumeType(),
                            consumerData.getMessageModel(),
                            consumerData.getConsumeFromWhere(),
                            consumerData.getSubscriptionDataSet());
                    syncHeartbeatToBroker(clientID);

                    if ("BROADCASTING".equals(consumerData.getMessageModel())) {
                        log.info("Consumer group {} is BROADCASTING mode, skip rebalance notification", group);
                    }

                    if (onConsumerRegistered != null) {
                        callbackExecutor.submit(() -> {
                            try {
                                onConsumerRegistered.run();
                            } catch (Exception e) {
                                log.warn("Failed to trigger heartbeat forward after consumer registration: {}", e.getMessage());
                            }
                        });
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
        boolean isBroadcast = consumerGroup != null && clientConnectionManager.isBroadcastGroup(consumerGroup);

        ClientConnectionManager.ClientInfo clientInfo = clientConnectionManager.unregisterClient(channel, clientID,
                requestHeader.getProducerGroup(), consumerGroup);
        unregisterFromBroker(clientInfo);

        if (isBroadcast) {
            log.info("Consumer group {} is BROADCASTING, skip notification after unregister", consumerGroup);
        }

        return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
    }

    private RemotingCommand getConsumerListByGroup(RemotingCommand request) {
        String consumerGroup = null;
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            consumerGroup = extFields.get("consumerGroup");
        }

        List<String> consumerIdList;
        if (upstreamConsumerSessionManager != null && consumerGroup != null) {
            try {
                consumerIdList = upstreamConsumerSessionManager.getConsumerListByGroup(consumerGroup);
            } catch (Exception e) {
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, e.getMessage());
            }
        } else {
            consumerIdList = getLocalConsumerListByGroup(consumerGroup);
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
            // 广播模式不需要转发rebalance通知
            if (clientConnectionManager.isBroadcastGroup(consumerGroup)) {
                log.info("Skip NOTIFY_CONSUMER_IDS_CHANGED for BROADCASTING group {}", consumerGroup);
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
            }
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

    private void syncHeartbeatToBroker(String clientId) {
        if (upstreamConsumerSessionManager == null) {
            return;
        }
        ClientConnectionManager.ClientInfo clientInfo = clientConnectionManager.getClientInfo(clientId);
        if (clientInfo != null) {
            upstreamConsumerSessionManager.syncHeartbeat(clientInfo);
        }
    }

    private void unregisterFromBroker(ClientConnectionManager.ClientInfo clientInfo) {
        if (upstreamConsumerSessionManager != null && clientInfo != null) {
            upstreamConsumerSessionManager.unregisterClient(clientInfo);
        }
    }

    private List<String> getLocalConsumerListByGroup(String consumerGroup) {
        List<String> consumerIdList = new ArrayList<>();
        if (consumerGroup != null) {
            for (ClientConnectionManager.ClientInfo clientInfo : clientConnectionManager.getAllClientInfos()) {
                if (clientInfo.getConsumerGroups().contains(consumerGroup)) {
                    consumerIdList.add(clientInfo.getClientId());
                }
            }
        }
        return consumerIdList;
    }

    private void notifyGroupMembers(String consumerGroup) {
        for (ClientConnectionManager.ClientInfo clientInfo : clientConnectionManager.getAllClientInfos()) {
            if (clientInfo.getConsumerGroups().contains(consumerGroup)) {
                Channel ch = clientInfo.getChannel();
                if (ch != null && ch.isActive()) {
                    RemotingCommand notification = RemotingCommand.createRequestCommand(
                            RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, null);
                    HashMap<String, String> extFields = new HashMap<>();
                    extFields.put("consumerGroup", consumerGroup);
                    notification.setExtFields(extFields);

                    ch.writeAndFlush(notification).addListener(future -> {
                        if (!future.isSuccess()) {
                            log.warn("Failed to send NOTIFY_CONSUMER_IDS_CHANGED to clientId={}, group={}, error={}",
                                    clientInfo.getClientId(), consumerGroup, future.cause().getMessage());
                        }
                    });
                    log.debug("Sent NOTIFY_CONSUMER_IDS_CHANGED to clientId={}, group={}",
                            clientInfo.getClientId(), consumerGroup);
                }
            }
        }
    }
}
