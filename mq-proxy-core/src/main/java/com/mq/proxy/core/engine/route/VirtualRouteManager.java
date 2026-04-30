package com.mq.proxy.core.engine.route;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.header.GetRouteInfoRequestHeader;
import com.mq.proxy.core.protocol.header.RegisterBrokerRequestHeader;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.storage.model.TopicRouteInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VirtualRouteManager {

    private final NettyRemotingClient namesrvClient;
    private String namesrvAddr;
    private String proxyAddr;
    private String proxyBrokerName = "ProxyBroker";
    private long proxyBrokerId = 0L;
    private String proxyClusterName = "ProxyCluster";
    private final ConcurrentHashMap<String, TopicRouteInfo> routeCache = new ConcurrentHashMap<>();
    private long routeCacheExpireMillis = 30000;
    private final ConcurrentHashMap<String, Long> routeCacheTimestamp = new ConcurrentHashMap<>();
    private volatile boolean started = false;

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

    public boolean registerProxyToNameServer() {
        RegisterBrokerRequestHeader header = new RegisterBrokerRequestHeader();
        header.setBrokerName(this.proxyBrokerName);
        header.setBrokerAddr(this.proxyAddr);
        header.setClusterName(this.proxyClusterName);
        header.setHaServerAddr("");
        header.setBrokerId(this.proxyBrokerId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, header);
        request.makeCustomHeaderToNet();

        TopicConfigSerializeWrapper wrapper = new TopicConfigSerializeWrapper();
        request.setBody(wrapper.encode());

        try {
            RemotingCommand response = this.namesrvClient.invokeSync(this.namesrvAddr, request, 3000);
            return response.getCode() == RemotingSysResponseCode.SUCCESS;
        } catch (Exception e) {
            return false;
        }
    }

    public void unregisterProxyFromNameServer() {
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("brokerName", this.proxyBrokerName);
        extFields.put("brokerAddr", this.proxyAddr);
        extFields.put("clusterName", this.proxyClusterName);
        extFields.put("brokerId", String.valueOf(this.proxyBrokerId));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_BROKER, null);
        request.setExtFields(extFields);

        try {
            this.namesrvClient.invokeOneway(this.namesrvAddr, request, 3000);
        } catch (Exception e) {
        }
    }

    public void refreshRouteCache() {
        this.routeCacheTimestamp.clear();
    }

    public String getProxyAddr() {
        return this.proxyAddr;
    }

    public String getProxyBrokerAddr() {
        return this.proxyAddr;
    }

    public void setProxyBrokerName(String proxyBrokerName) {
        this.proxyBrokerName = proxyBrokerName;
    }

    public void setProxyClusterName(String proxyClusterName) {
        this.proxyClusterName = proxyClusterName;
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
            TopicRouteInfo.BrokerData virtualBrokerData = new TopicRouteInfo.BrokerData();
            virtualBrokerData.setBrokerName(brokerData.getBrokerName());
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
