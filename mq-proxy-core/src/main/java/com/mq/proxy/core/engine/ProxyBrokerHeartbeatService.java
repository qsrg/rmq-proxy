package com.mq.proxy.core.engine;

import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.LanguageCode;
import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.server.InvokeCallback;
import com.mq.proxy.core.server.NettyClientRuntime;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.RemotingProcessor;
import com.mq.proxy.core.storage.StorageAdapter;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProxyBrokerHeartbeatService implements UpstreamConsumerSessionManager {

    private static final Logger log = LoggerFactory.getLogger(ProxyBrokerHeartbeatService.class);
    private static final String SHARED_BROKER_CLIENT_ID = "__proxy_shared_broker_client__";

    private static final long HEARTBEAT_INTERVAL_MILLIS = 30000;

    private final ClientConnectionManager clientConnectionManager;
    private final StorageAdapter storageAdapter;
    private final VirtualRouteManager virtualRouteManager;
    private ScheduledExecutorService scheduledExecutor;

    private final ConcurrentHashMap<String, NettyRemotingClient> clientChannelPool = new ConcurrentHashMap<>();
    private final Set<String> inFlightHeartbeats = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    private final NettyClientConfig nettyClientConfig;
    private final NettyClientRuntime heartbeatClientRuntime;
    private NettyRemotingServer remotingServer;

    public void setRemotingServer(NettyRemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public ProxyBrokerHeartbeatService(ClientConnectionManager clientConnectionManager,
                                       StorageAdapter storageAdapter, String proxyHost, int proxyPort) {
        this(clientConnectionManager, storageAdapter, proxyHost, proxyPort, null);
    }

    public ProxyBrokerHeartbeatService(ClientConnectionManager clientConnectionManager,
                                       StorageAdapter storageAdapter, String proxyHost, int proxyPort,
                                       VirtualRouteManager virtualRouteManager) {
        this.clientConnectionManager = clientConnectionManager;
        this.storageAdapter = storageAdapter;
        this.virtualRouteManager = virtualRouteManager;
        this.nettyClientConfig = new NettyClientConfig();
        this.heartbeatClientRuntime = new NettyClientRuntime(this.nettyClientConfig, "BrokerHeartbeatClient");
    }

    public void start() {
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "BrokerHeartbeatThread_" + threadNumber.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
        this.scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    ProxyBrokerHeartbeatService.this.sendHeartbeatToAllBrokers();
                } catch (Throwable e) {
                    log.error("sendHeartbeatToAllBrokers exception", e);
                }
            }
        }, 5000, HEARTBEAT_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        log.info("ProxyBrokerHeartbeatService started, interval={}ms", HEARTBEAT_INTERVAL_MILLIS);
    }

    public void shutdown() {
        if (this.scheduledExecutor != null) {
            this.scheduledExecutor.shutdown();
        }
        for (Map.Entry<String, NettyRemotingClient> entry : clientChannelPool.entrySet()) {
            try {
                entry.getValue().shutdown();
            } catch (Exception e) {
                log.warn("Failed to shutdown client for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        clientChannelPool.clear();
        inFlightHeartbeats.clear();
        this.heartbeatClientRuntime.shutdown();
        log.info("ProxyBrokerHeartbeatService shutdown");
    }

    public void sendHeartbeat() {
        sendHeartbeatToAllBrokers();
    }

    @Override
    public void syncHeartbeat(ClientConnectionManager.ClientInfo clientInfo) {
        List<String> brokerAddrs = resolveBrokerAddrs();
        if (brokerAddrs.isEmpty() || clientInfo == null) {
            return;
        }
        sendHeartbeatForClient(clientInfo, brokerAddrs);
    }

    @Override
    public void unregisterClient(ClientConnectionManager.ClientInfo clientInfo) {
        if (clientInfo == null) {
            return;
        }
        List<String> brokerAddrs = resolveBrokerAddrs();
        List<Map<String, String>> unregisterExtFieldsList = buildUnregisterExtFields(clientInfo);
        for (String brokerAddr : brokerAddrs) {
            for (Map<String, String> extFields : unregisterExtFieldsList) {
                try {
                    RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, null);
                    request.setExtFields(new HashMap<>(extFields));
                    getOrCreateClient(clientInfo.getClientId()).invokeSync(brokerAddr, request, 5000);
                } catch (Exception e) {
                    log.warn("Proxy unregister client from broker failed, clientId={}, brokerAddr={}, extFields={}, error={}",
                            clientInfo.getClientId(), brokerAddr, extFields, e.getMessage());
                }
            }
        }
        NettyRemotingClient client = clientChannelPool.remove(clientInfo.getClientId());
        if (client != null) {
            client.shutdown();
        }
    }

    @Override
    public List<String> getConsumerListByGroup(String consumerGroup) throws Exception {
        List<String> brokerAddrs = resolveBrokerAddrs();
        if (brokerAddrs.isEmpty()) {
            return Collections.emptyList();
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", consumerGroup);
        request.setExtFields(extFields);

        RemotingCommand response = getSharedBrokerClient().invokeSync(brokerAddrs.get(0), request, 5000);
        if (response == null || response.getCode() != RemotingSysResponseCode.SUCCESS || response.getBody() == null) {
            return Collections.emptyList();
        }
        return decodeConsumerIdList(response.getBody());
    }

    private void sendHeartbeatToAllBrokers() {
        List<String> brokerAddrs = resolveBrokerAddrs();
        if (brokerAddrs.isEmpty()) {
            log.debug("No real broker addresses available, skipping heartbeat to broker");
            return;
        }

        List<ClientConnectionManager.ClientInfo> clientInfos = clientConnectionManager.getAllClientInfos();
        if (clientInfos.isEmpty()) {
            log.debug("No active clients, skipping heartbeat to broker");
            return;
        }

        log.debug("Sending heartbeat to brokers {} for {} clients", brokerAddrs, clientInfos.size());

        for (ClientConnectionManager.ClientInfo clientInfo : clientInfos) {
            sendHeartbeatForClient(clientInfo, brokerAddrs);
        }

        cleanupStaleClients(clientInfos);
    }

    private NettyRemotingClient getOrCreateClient(String clientId) {
        return clientChannelPool.computeIfAbsent(clientId, this::createClient);
    }

    private NettyRemotingClient getSharedBrokerClient() {
        return getOrCreateClient(SHARED_BROKER_CLIENT_ID);
    }

    private void sendHeartbeatForClient(ClientConnectionManager.ClientInfo clientInfo, List<String> brokerAddrs) {
        String clientId = clientInfo.getClientId();
        log.debug("sendHeartbeatForClient: clientId={}, consumerGroups={}, producerGroups={}",
                clientId, clientInfo.getConsumerGroups(), clientInfo.getProducerGroups());
        HeartbeatData heartbeatData = buildHeartbeatDataForClient(clientInfo);
        if (heartbeatData == null) {
            log.warn("sendHeartbeatForClient: heartbeatData is NULL for clientId={}, consumerGroups={}, producerGroups={}",
                    clientId, clientInfo.getConsumerGroups(), clientInfo.getProducerGroups());
            return;
        }

        byte[] body = heartbeatData.encode();

        for (String brokerAddr : brokerAddrs) {
            final String heartbeatKey = clientId + "@" + brokerAddr;
            if (!inFlightHeartbeats.add(heartbeatKey)) {
                log.debug("Skip in-flight heartbeat to broker {} for client {}", brokerAddr, clientId);
                continue;
            }
            try {
                final RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
                request.setLanguage(LanguageCode.JAVA);
                request.setBody(body);
                final NettyRemotingClient client = getOrCreateClient(clientId);
                final int consumerGroupCount = heartbeatData.getConsumerDataSet().size();
                final int producerGroupCount = heartbeatData.getProducerDataSet().size();
                client.invokeAsync(brokerAddr, request, 5000, new InvokeCallback() {
                    @Override
                    public void operationSucceed(RemotingCommand response) {
                        inFlightHeartbeats.remove(heartbeatKey);
                        if (response != null && response.getCode() == RemotingSysResponseCode.SUCCESS) {
                            log.debug("Proxy heartbeat to broker {} for client {} SUCCESS, consumerGroups={}, producerGroups={}",
                                    brokerAddr, clientId, consumerGroupCount, producerGroupCount);
                        } else {
                            log.warn("Proxy heartbeat to broker {} for client {} FAILED, responseCode={}, remark={}",
                                    brokerAddr, clientId, response != null ? response.getCode() : "null",
                                    response != null ? response.getRemark() : "null");
                        }
                    }

                    @Override
                    public void operationFail(Throwable throwable) {
                        inFlightHeartbeats.remove(heartbeatKey);
                        log.warn("Proxy heartbeat to broker {} for client {} exception: {}",
                                brokerAddr, clientId, throwable != null ? throwable.getMessage() : "unknown");
                    }
                });
            } catch (Exception e) {
                inFlightHeartbeats.remove(heartbeatKey);
                log.warn("Proxy heartbeat to broker {} for client {} exception: {}", brokerAddr, clientId, e.getMessage(), e);
            }
        }
    }

    private void cleanupStaleClients(List<ClientConnectionManager.ClientInfo> activeClientInfos) {
        Set<String> activeClientIds = new java.util.HashSet<>();
        for (ClientConnectionManager.ClientInfo info : activeClientInfos) {
            activeClientIds.add(info.getClientId());
        }
        for (Map.Entry<String, NettyRemotingClient> entry : clientChannelPool.entrySet()) {
            if (SHARED_BROKER_CLIENT_ID.equals(entry.getKey())) {
                continue;
            }
            if (!activeClientIds.contains(entry.getKey())) {
                try {
                    entry.getValue().shutdown();
                    clientChannelPool.remove(entry.getKey());
                    log.info("Cleaned up stale NettyRemotingClient for clientId={}", entry.getKey());
                } catch (Exception e) {
                    log.warn("Failed to cleanup client for {}: {}", entry.getKey(), e.getMessage());
                }
            }
        }
    }

    protected NettyRemotingClient createClientEntry(String clientId) {
        return getOrCreateClient(clientId);
    }

    protected NettyRemotingClient getSharedClientEntry() {
        return getSharedBrokerClient();
    }

    protected void cleanupClientEntries(List<ClientConnectionManager.ClientInfo> activeClientInfos) {
        cleanupStaleClients(activeClientInfos);
    }

    protected Map<String, NettyRemotingClient> getClientChannelPool() {
        return clientChannelPool;
    }

    private List<String> resolveBrokerAddrs() {
        if (virtualRouteManager != null) {
            List<String> addrs = virtualRouteManager.getAllRealBrokerAddrs();
            if (!addrs.isEmpty()) {
                return addrs;
            }
        }
        return Collections.emptyList();
    }

    private HeartbeatData buildHeartbeatDataForClient(ClientConnectionManager.ClientInfo clientInfo) {
        String clientId = clientInfo.getClientId();
        Set<String> consumerGroups = clientInfo.getConsumerGroups();
        Set<String> producerGroups = clientInfo.getProducerGroups();

        if (consumerGroups.isEmpty() && producerGroups.isEmpty()) {
            return null;
        }

        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID(clientId);

        for (String group : producerGroups) {
            HeartbeatData.ProducerData producerData = new HeartbeatData.ProducerData();
            producerData.setGroupName(group);
            heartbeatData.getProducerDataSet().add(producerData);
        }

        for (String group : consumerGroups) {
            HeartbeatData.ConsumerData consumerData = new HeartbeatData.ConsumerData();
            consumerData.setGroupName(group);
            consumerData.setConsumeType(clientInfo.getGroupConsumeType(group));
            consumerData.setMessageModel(clientInfo.getGroupMessageModel(group));
            consumerData.setConsumeFromWhere(clientInfo.getGroupConsumeFromWhere(group));

            Set<HeartbeatData.SubscriptionData> subs = clientInfo.getSubscriptions(group);
            if (subs != null) {
                Set<HeartbeatData.SubscriptionData> newSubs = new java.util.HashSet<>();
                for (HeartbeatData.SubscriptionData sub : subs) {
                    HeartbeatData.SubscriptionData newSub = new HeartbeatData.SubscriptionData();
                    newSub.setTopic(sub.getTopic());
                    newSub.setSubString(sub.getSubString());
                    newSub.setSubVersion(sub.getSubVersion());
                    newSub.setClassFilterMode(sub.isClassFilterMode());
                    newSub.setExpressionType(sub.getExpressionType() != null ? sub.getExpressionType() : "TAG");
                    populateTagsAndCodes(newSub, sub.getSubString());
                    newSubs.add(newSub);
                }
                consumerData.setSubscriptionDataSet(newSubs);
            }

            heartbeatData.getConsumerDataSet().add(consumerData);
        }

        return heartbeatData;
    }

    private List<Map<String, String>> buildUnregisterExtFields(ClientConnectionManager.ClientInfo clientInfo) {
        List<Map<String, String>> requests = new ArrayList<>();
        String clientId = clientInfo.getClientId();

        for (String producerGroup : clientInfo.getProducerGroups()) {
            HashMap<String, String> extFields = new HashMap<>();
            extFields.put("clientID", clientId);
            extFields.put("producerGroup", producerGroup);
            requests.add(extFields);
        }

        for (String consumerGroup : clientInfo.getConsumerGroups()) {
            HashMap<String, String> extFields = new HashMap<>();
            extFields.put("clientID", clientId);
            extFields.put("consumerGroup", consumerGroup);
            requests.add(extFields);
        }

        return requests;
    }

    protected NettyRemotingClient createClient(String clientId) {
        NettyRemotingClient client = new NettyRemotingClient(nettyClientConfig, heartbeatClientRuntime);
        client.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO,
                createBrokerToClientForwardProcessor(clientId, RequestCode.GET_CONSUMER_RUNNING_INFO));
        client.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED,
                createBrokerToClientForwardProcessor(clientId, RequestCode.NOTIFY_CONSUMER_IDS_CHANGED));
        client.start();
        log.info("Created dedicated NettyRemotingClient for clientId={}", clientId);
        return client;
    }

    protected RemotingProcessor createBrokerToClientForwardProcessor(String clientId, int requestCode) {
        return new BrokerToClientForwardProcessor(clientId, requestCode);
    }

    private void populateTagsAndCodes(HeartbeatData.SubscriptionData subData, String subString) {
        if (subString == null || subString.isEmpty() || "*".equals(subString)) {
            subData.setTagsSet(Collections.emptySet());
            subData.setCodeSet(Collections.emptySet());
            return;
        }
        Set<String> tagsSet = new java.util.HashSet<>();
        Set<Integer> codeSet = new java.util.HashSet<>();
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

    private List<String> decodeConsumerIdList(byte[] body) {
        String json = new String(body, java.nio.charset.StandardCharsets.UTF_8);
        int start = json.indexOf('[');
        int end = json.indexOf(']', start);
        if (start < 0 || end < 0 || end <= start) {
            return Collections.emptyList();
        }
        String content = json.substring(start + 1, end).trim();
        if (content.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> result = new ArrayList<>();
        String[] parts = content.split(",");
        for (String part : parts) {
            String value = part.trim();
            if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
                value = value.substring(1, value.length() - 1);
            }
            if (!value.isEmpty()) {
                result.add(value);
            }
        }
        return result;
    }

    private class BrokerToClientForwardProcessor implements RemotingProcessor {
        private final String clientId;
        private final int requestCode;

        BrokerToClientForwardProcessor(String clientId, int requestCode) {
            this.clientId = clientId;
            this.requestCode = requestCode;
        }

        @Override
        public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
            if (remotingServer == null) {
                log.warn("RemotingServer not set, cannot forward request code={} for clientId={}", requestCode, clientId);
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                        "proxy remotingServer not available");
            }

            ClientConnectionManager.ClientInfo clientInfo = null;
            for (ClientConnectionManager.ClientInfo info : clientConnectionManager.getAllClientInfos()) {
                if (clientId.equals(info.getClientId())) {
                    clientInfo = info;
                    break;
                }
            }

            if (clientInfo == null) {
                log.warn("Client not found for clientId={}, cannot forward request code={}", clientId, requestCode);
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                        "client " + clientId + " not online");
            }

            Channel consumerChannel = clientInfo.getChannel();
            if (consumerChannel == null || !consumerChannel.isActive()) {
                log.warn("Consumer channel not active for clientId={}, cannot forward request code={}", clientId, requestCode);
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                        "client " + clientId + " channel not active");
            }

            try {
                RemotingCommand forwardRequest = RemotingCommand.createRequestCommand(requestCode, null);
                forwardRequest.setExtFields(request.getExtFields());
                forwardRequest.setBody(request.getBody());
                if (requestCode == RequestCode.NOTIFY_CONSUMER_IDS_CHANGED) {
                    remotingServer.invokeOneway(consumerChannel, forwardRequest, 10000);
                    log.info("Forwarded oneway request code={} from Broker to consumer clientId={}",
                            requestCode, clientId);
                    return null;
                }

                RemotingCommand response = remotingServer.invokeSync(consumerChannel, forwardRequest, 10000);
                log.info("Forwarded request code={} from Broker to consumer clientId={}, responseCode={}",
                        requestCode, clientId, response != null ? response.getCode() : "null");
                return response;
            } catch (Exception e) {
                log.warn("Failed to forward request code={} to consumer clientId={}: {}", requestCode, clientId, e.getMessage());
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                        "forward to client " + clientId + " failed: " + e.getMessage());
            }
        }
    }
}
