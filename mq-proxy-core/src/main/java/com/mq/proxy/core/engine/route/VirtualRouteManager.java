package com.mq.proxy.core.engine.route;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.GetRouteInfoRequestHeader;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.model.TopicRouteInfo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VirtualRouteManager {

    private static final Logger log = LoggerFactory.getLogger(VirtualRouteManager.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final NettyRemotingClient namesrvClient;
    private String namesrvAddr;
    private String proxyAddr;
    private final ConcurrentHashMap<String, TopicRouteInfo> routeCache = new ConcurrentHashMap<>();
    private long routeCacheExpireMillis = 30000;
    private final ConcurrentHashMap<String, Long> routeCacheTimestamp = new ConcurrentHashMap<>();
    private volatile boolean started = false;
    private final ConcurrentHashMap<String, String> brokerNameToRealAddr = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> brokerNameAndIdToRealAddr = new ConcurrentHashMap<>();

    public VirtualRouteManager() {
        this.namesrvClient = new NettyRemotingClient(new NettyClientConfig());
    }

    public VirtualRouteManager(NettyRemotingClient namesrvClient) {
        this.namesrvClient = namesrvClient;
    }

    public void start(String namesrvAddr, String proxyHost, int proxyPort) {
        this.proxyAddr = proxyHost + ":" + proxyPort;
        this.namesrvAddr = namesrvAddr;
        NettyClientConfig clientConfig = new NettyClientConfig();
        clientConfig.setNamesrvAddr(namesrvAddr);
        this.namesrvClient.start();
        this.started = true;
    }

    public void shutdown() {
        if (this.namesrvClient != null) {
            this.namesrvClient.shutdown();
        }
        this.routeCache.clear();
        this.routeCacheTimestamp.clear();
        this.brokerNameToRealAddr.clear();
        this.brokerNameAndIdToRealAddr.clear();
        this.started = false;
    }

    public TopicRouteInfo getRouteInfoByTopic(String topic) {
        TopicRouteInfo cached = this.routeCache.get(topic);
        if (cached != null && !isRouteCacheExpired(topic)) {
            return cached;
        }

        TopicRouteInfo realRoute = fetchRouteFromNameServer(topic);
        if (realRoute == null) {
            return null;
        }

        TopicRouteInfo virtualRoute = convertToVirtualRoute(realRoute, topic);
        this.routeCache.put(topic, virtualRoute);
        this.routeCacheTimestamp.put(topic, System.currentTimeMillis());
        return virtualRoute;
    }

    public void refreshRouteCache() {
        this.routeCacheTimestamp.clear();
    }

    public String getRealBrokerAddr(String brokerName) {
        if (brokerName == null) {
            return null;
        }
        return this.brokerNameToRealAddr.get(brokerName);
    }

    public String getRealBrokerAddr(String brokerName, long brokerId) {
        if (brokerName == null) {
            return null;
        }
        String key = brokerName + ":" + brokerId;
        String addr = this.brokerNameAndIdToRealAddr.get(key);
        if (addr != null) {
            return addr;
        }
        return this.brokerNameToRealAddr.get(brokerName);
    }

    public List<String> getAllRealBrokerAddrs() {
        if (this.brokerNameToRealAddr.isEmpty()) {
            discoverBrokers();
        }
        return new ArrayList<>(this.brokerNameToRealAddr.values());
    }

    public void discoverBrokers() {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        request.makeCustomHeaderToNet();

        try {
            RemotingCommand response = this.namesrvClient.invokeSync(this.namesrvAddr, request, 3000);
            if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
                parseClusterInfoAndPopulateCache(response.getBody());
            } else {
                log.warn("Failed to discover brokers, response code: {}", response.getCode());
            }
        } catch (Exception e) {
            log.warn("Failed to discover brokers from NameServer: {}", e.getMessage());
        }
    }

    private void parseClusterInfoAndPopulateCache(byte[] data) {
        try {
            String json = new String(data, StandardCharsets.UTF_8);
            json = RouteInfoSerializer.fixNumericKeys(json);
            JsonNode root = MAPPER.readTree(json);
            JsonNode brokerAddrTable = root.get("brokerAddrTable");
            if (brokerAddrTable != null) {
                Iterator<Map.Entry<String, JsonNode>> fields = brokerAddrTable.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String brokerName = entry.getKey();
                    JsonNode brokerData = entry.getValue();
                    JsonNode brokerAddrs = brokerData.get("brokerAddrs");
                    if (brokerAddrs != null) {
                        Iterator<Map.Entry<String, JsonNode>> addrFields = brokerAddrs.fields();
                        while (addrFields.hasNext()) {
                            Map.Entry<String, JsonNode> addrEntry = addrFields.next();
                            long brokerId = Long.parseLong(addrEntry.getKey());
                            String addr = addrEntry.getValue().asText();
                            this.brokerNameAndIdToRealAddr.put(brokerName + ":" + brokerId, addr);
                            if (brokerId == 0L) {
                                this.brokerNameToRealAddr.put(brokerName, addr);
                            }
                        }
                    }
                }
            }
            log.info("Discovered {} broker(s) from NameServer", this.brokerNameToRealAddr.size());
        } catch (Exception e) {
            log.warn("Failed to parse cluster info: {}", e.getMessage());
        }
    }

    public String findBrokerNameByTopicAndQueueId(String topic, int queueId) {
        TopicRouteInfo routeInfo = this.routeCache.get(topic);
        if (routeInfo == null || routeInfo.getQueueDatas() == null) {
            return null;
        }
        int currentQueueId = 0;
        for (TopicRouteInfo.QueueData queueData : routeInfo.getQueueDatas()) {
            int writeQueueNums = queueData.getWriteQueueNums();
            if (queueId >= currentQueueId && queueId < currentQueueId + writeQueueNums) {
                return queueData.getBrokerName();
            }
            currentQueueId += writeQueueNums;
        }
        if (!routeInfo.getQueueDatas().isEmpty()) {
            return routeInfo.getQueueDatas().get(0).getBrokerName();
        }
        return null;
    }

    public String getProxyAddr() {
        return this.proxyAddr;
    }

    public void setRouteCacheExpireMillis(long routeCacheExpireMillis) {
        this.routeCacheExpireMillis = routeCacheExpireMillis;
    }

    public NettyRemotingClient getNamesrvClient() {
        return this.namesrvClient;
    }

    public String getNamesrvAddr() {
        return this.namesrvAddr;
    }

    private boolean isRouteCacheExpired(String topic) {
        Long timestamp = this.routeCacheTimestamp.get(topic);
        if (timestamp == null) {
            return true;
        }
        return (System.currentTimeMillis() - timestamp) > this.routeCacheExpireMillis;
    }

    private TopicRouteInfo fetchRouteFromNameServer(String topic) {
        GetRouteInfoRequestHeader header = new GetRouteInfoRequestHeader();
        header.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, header);
        request.makeCustomHeaderToNet();

        try {
            RemotingCommand response = this.namesrvClient.invokeSync(this.namesrvAddr, request, 3000);
            if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
                return RouteInfoSerializer.decodeTopicRouteInfo(response.getBody());
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    TopicRouteInfo convertToVirtualRoute(TopicRouteInfo realRoute, String topic) {
        TopicRouteInfo virtualRoute = new TopicRouteInfo();
        virtualRoute.setTopic(topic);
        virtualRoute.setOrderTopicConf(realRoute.getOrderTopicConf());
        virtualRoute.setQueueDatas(realRoute.getQueueDatas());
        virtualRoute.setFilterServerTable(realRoute.getFilterServerTable());

        java.util.List<TopicRouteInfo.BrokerData> virtualBrokerDatas = new java.util.ArrayList<>();
        for (TopicRouteInfo.BrokerData brokerData : realRoute.getBrokerDatas()) {
            String brokerName = brokerData.getBrokerName();

            for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                String realAddr = entry.getValue();
                if (realAddr != null && !realAddr.isEmpty()) {
                    this.brokerNameAndIdToRealAddr.put(brokerName + ":" + entry.getKey(), realAddr);
                    if (entry.getKey() == 0L) {
                        this.brokerNameToRealAddr.put(brokerName, realAddr);
                    }
                }
            }

            if (!this.brokerNameToRealAddr.containsKey(brokerName)) {
                String firstAddr = brokerData.getBrokerAddrs().values().iterator().next();
                if (firstAddr != null && !firstAddr.isEmpty()) {
                    this.brokerNameToRealAddr.put(brokerName, firstAddr);
                }
            }

            TopicRouteInfo.BrokerData virtualBrokerData = new TopicRouteInfo.BrokerData();
            virtualBrokerData.setBrokerName(brokerName);
            Map<Long, String> virtualAddrs = new HashMap<>();
            for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                virtualAddrs.put(entry.getKey(), this.proxyAddr);
            }
            virtualBrokerData.setBrokerAddrs(virtualAddrs);
            virtualBrokerDatas.add(virtualBrokerData);
        }
        virtualRoute.setBrokerDatas(virtualBrokerDatas);

        return virtualRoute;
    }
}
