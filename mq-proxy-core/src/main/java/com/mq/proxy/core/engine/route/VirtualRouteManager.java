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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
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
        this.namesrvClient.setNamesrvAddr(namesrvAddr);
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

        return refreshRouteInfoByTopic(topic);
    }

    public TopicRouteInfo refreshRouteInfoByTopic(String topic) {
        TopicRouteInfo cached = this.routeCache.get(topic);
        TopicRouteInfo realRoute = fetchRouteFromNameServer(topic);
        if (realRoute == null) {
            return cached;
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
        if (this.brokerNameAndIdToRealAddr.isEmpty()) {
            discoverBrokers();
        }
        Set<String> addrs = new LinkedHashSet<>(this.brokerNameAndIdToRealAddr.values());
        if (addrs.isEmpty()) {
            addrs.addAll(this.brokerNameToRealAddr.values());
        }
        return new ArrayList<>(addrs);
    }

    public void discoverBrokers() {
        if (this.namesrvAddr == null || this.namesrvAddr.trim().isEmpty()) {
            log.warn("discoverBrokers skipped because namesrvAddr is not configured");
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        request.makeCustomHeaderToNet();

        // 支持多namesrv地址
        String[] namesrvAddrs = this.namesrvAddr.split(";");
        for (String addr : namesrvAddrs) {
            String trimmedAddr = addr.trim();
            if (trimmedAddr.isEmpty()) {
                continue;
            }
            try {
                RemotingCommand response = this.namesrvClient.invokeSync(trimmedAddr, request, 3000);
                if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
                    parseClusterInfoAndPopulateCache(response.getBody());
                    return;
                }
            } catch (Exception e) {
                log.warn("discoverBrokers failed for addr={}, error={}", trimmedAddr, e.getMessage());
            }
        }
        log.warn("Failed to discover brokers from all NameServer addresses");
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
            log.info("Discovered {} master broker(s), {} broker address(es) from NameServer",
                    this.brokerNameToRealAddr.size(), this.brokerNameAndIdToRealAddr.size());
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

    private boolean isRetryOrDlqTopic(String topic) {
        return topic != null && (topic.startsWith("%RETRY%") || topic.startsWith("%DLQ%"));
    }

    private TopicRouteInfo fetchRouteFromNameServer(String topic) {
        if (this.namesrvAddr == null || this.namesrvAddr.trim().isEmpty()) {
            log.warn("fetchRouteFromNameServer skipped because namesrvAddr is not configured, topic={}", topic);
            return null;
        }

        GetRouteInfoRequestHeader header = new GetRouteInfoRequestHeader();
        header.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, header);
        request.makeCustomHeaderToNet();

        // 支持多namesrv地址，用分号分隔
        String[] namesrvAddrs = this.namesrvAddr.split(";");
        Exception lastException = null;
        for (String addr : namesrvAddrs) {
            String trimmedAddr = addr.trim();
            if (trimmedAddr.isEmpty()) {
                continue;
            }
            try {
                RemotingCommand response = this.namesrvClient.invokeSync(trimmedAddr, request, 3000);
                if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
                    return RouteInfoSerializer.decodeTopicRouteInfo(response.getBody());
                }
            } catch (Exception e) {
                lastException = e;
                log.warn("fetchRouteFromNameServer failed for addr={}, topic={}, error={}", trimmedAddr, topic, e.getMessage());
            }
        }
        if (lastException != null) {
            log.warn("All namesrv addresses failed for topic={}", topic);
        }
        return null;
    }

    TopicRouteInfo convertToVirtualRoute(TopicRouteInfo realRoute, String topic) {
        TopicRouteInfo virtualRoute = new TopicRouteInfo();
        virtualRoute.setTopic(topic);
        virtualRoute.setOrderTopicConf(realRoute.getOrderTopicConf());
        virtualRoute.setQueueDatas(realRoute.getQueueDatas());
        virtualRoute.setFilterServerTable(realRoute.getFilterServerTable());

        Set<String> brokerNamesToRefresh = new LinkedHashSet<>();
        TopicRouteInfo cachedRoute = this.routeCache.get(topic);
        if (cachedRoute != null && cachedRoute.getBrokerDatas() != null) {
            for (TopicRouteInfo.BrokerData brokerData : cachedRoute.getBrokerDatas()) {
                if (brokerData.getBrokerName() != null) {
                    brokerNamesToRefresh.add(brokerData.getBrokerName());
                }
            }
        }
        if (realRoute.getBrokerDatas() != null) {
            for (TopicRouteInfo.BrokerData brokerData : realRoute.getBrokerDatas()) {
                if (brokerData.getBrokerName() != null) {
                    brokerNamesToRefresh.add(brokerData.getBrokerName());
                }
            }
        }
        for (String brokerName : brokerNamesToRefresh) {
            clearBrokerAddrMapping(brokerName);
        }

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

    private void clearBrokerAddrMapping(String brokerName) {
        this.brokerNameToRealAddr.remove(brokerName);
        String keyPrefix = brokerName + ":";
        for (String key : this.brokerNameAndIdToRealAddr.keySet()) {
            if (key.startsWith(keyPrefix)) {
                this.brokerNameAndIdToRealAddr.remove(key);
            }
        }
    }
}
