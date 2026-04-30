package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.engine.ProcessorRegister;
import com.mq.proxy.core.engine.ProxyBrokerHeartbeatService;
import com.mq.proxy.core.engine.route.RouteInfoSerializer;
import com.mq.proxy.core.engine.route.VirtualRouteManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyRemotingServer;
import com.mq.proxy.core.server.NettyServerConfig;
import com.mq.proxy.core.storage.StorageAdapterManager;
import com.mq.proxy.core.storage.StorageConfig;
import com.mq.proxy.core.storage.model.TopicRouteInfo;
import com.mq.proxy.rocketmq.adapter.RocketMQStorageAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RocketMQIntegrationTest {

    private static final String NAMESRV_ADDR = "127.0.0.1:9876";
    private static final String TEST_TOPIC = "PROXY_INTEGRATION_TEST";
    private static final String PRODUCER_GROUP = "PID_PROXY_TEST";
    private static final String CONSUMER_GROUP = "CID_PROXY_TEST";

    private NettyRemotingClient namesrvClient;
    private String brokerAddr;
    private NettyRemotingServer proxyServer;
    private NettyRemotingClient proxyClient;
    private int proxyPort;
    private StorageAdapterManager storageAdapterManager;
    private MessageEngine messageEngine;
    private VirtualRouteManager virtualRouteManager;
    private ClientConnectionManager clientConnectionManager;
    private ProxyBrokerHeartbeatService heartbeatService;

    @Before
    public void setUp() throws Exception {
        namesrvClient = new NettyRemotingClient(new NettyClientConfig());
        namesrvClient.start();

        brokerAddr = discoverBrokerAddr();
        assertNotNull("Broker address not found, is RocketMQ running on " + NAMESRV_ADDR + "?", brokerAddr);
        System.out.println("Discovered Broker address: " + brokerAddr);

        proxyPort = findAvailablePort();

        storageAdapterManager = new StorageAdapterManager();
        StorageConfig rocketmqConfig = new StorageConfig();
        rocketmqConfig.setAdapterType("rocketmq");
        rocketmqConfig.setNamesrvAddr(NAMESRV_ADDR);
        rocketmqConfig.setBrokerAddr(brokerAddr);
        rocketmqConfig.setConnectTimeoutMillis(5000);

        RocketMQStorageAdapter rocketmqAdapter = new RocketMQStorageAdapter();
        rocketmqAdapter.initialize(rocketmqConfig);
        storageAdapterManager.registerAdapter("rocketmq", rocketmqAdapter, true);

        messageEngine = new MessageEngine(storageAdapterManager);

        virtualRouteManager = new VirtualRouteManager();
        virtualRouteManager.start(NAMESRV_ADDR, "127.0.0.1", proxyPort);
        messageEngine.setVirtualRouteManager(virtualRouteManager);

        clientConnectionManager = new ClientConnectionManager();
        heartbeatService = new ProxyBrokerHeartbeatService(clientConnectionManager, storageAdapterManager, "127.0.0.1", proxyPort);

        NettyServerConfig serverConfig = new NettyServerConfig();
        serverConfig.setListenPort(proxyPort);
        proxyServer = new NettyRemotingServer(serverConfig);
        proxyServer.setClientConnectionManager(clientConnectionManager);
        ProcessorRegister.registerProcessors(proxyServer, messageEngine, virtualRouteManager, clientConnectionManager);
        proxyServer.start();
        messageEngine.setRemotingServer(proxyServer);
        heartbeatService.start();

        proxyClient = new NettyRemotingClient(new NettyClientConfig());
        proxyClient.start();

        System.out.println("Proxy started on port " + proxyPort + ", forwarding to Broker " + brokerAddr);
    }

    @After
    public void tearDown() {
        if (heartbeatService != null) {
            heartbeatService.shutdown();
        }
        if (proxyClient != null) {
            proxyClient.shutdown();
        }
        if (proxyServer != null) {
            proxyServer.shutdown();
        }
        if (virtualRouteManager != null) {
            virtualRouteManager.shutdown();
        }
        if (storageAdapterManager != null) {
            storageAdapterManager.shutdownAll();
        }
        if (namesrvClient != null) {
            namesrvClient.shutdown();
        }
    }

    @Test
    public void testNameServerConnectivity() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", TEST_TOPIC);
        request.setExtFields(extFields);
        request.makeCustomHeaderToNet();

        RemotingCommand response = namesrvClient.invokeSync(NAMESRV_ADDR, request, 5000);
        System.out.println("NameServer response: code=" + response.getCode());
        assertTrue("NameServer should return SUCCESS or TOPIC_NOT_EXIST",
                response.getCode() == RemotingSysResponseCode.SUCCESS
                        || response.getCode() == ResponseCode.TOPIC_NOT_EXIST);
    }

    @Test
    public void testDirectBrokerSendMessage() throws Exception {
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("a", PRODUCER_GROUP);
        extFields.put("b", TEST_TOPIC);
        extFields.put("c", "TBW102");
        extFields.put("d", "4");
        extFields.put("e", String.valueOf(-1));
        extFields.put("f", "0");
        extFields.put("g", String.valueOf(System.currentTimeMillis()));
        extFields.put("h", "0");
        extFields.put("j", "0");
        extFields.put("k", "false");
        extFields.put("l", "16");
        extFields.put("m", "false");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        request.setExtFields(extFields);
        request.setBody("Direct Broker Test".getBytes("UTF-8"));

        RemotingCommand response = namesrvClient.invokeSync(brokerAddr, request, 10000);
        System.out.println("Direct broker send: code=" + response.getCode() + ", extFields=" + response.getExtFields());
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getExtFields().get("msgId"));
    }

    @Test
    public void testProxySendMessage() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("a", PRODUCER_GROUP);
        extFields.put("b", TEST_TOPIC);
        extFields.put("c", "TBW102");
        extFields.put("d", "4");
        extFields.put("e", String.valueOf(-1));
        extFields.put("f", "0");
        extFields.put("g", String.valueOf(System.currentTimeMillis()));
        extFields.put("h", "0");
        extFields.put("j", "0");
        extFields.put("k", "false");
        extFields.put("l", "16");
        extFields.put("m", "false");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        request.setExtFields(extFields);
        request.setBody("Hello Proxy Integration Test".getBytes("UTF-8"));

        RemotingCommand response = proxyClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("Proxy send: code=" + response.getCode() + ", remark=" + response.getRemark() + ", extFields=" + response.getExtFields());

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull("msgId should not be null", response.getExtFields().get("msgId"));
        assertNotNull("queueId should not be null", response.getExtFields().get("queueId"));
        assertNotNull("queueOffset should not be null", response.getExtFields().get("queueOffset"));
    }

    @Test
    public void testProxyPullMessage() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        HashMap<String, String> sendExtFields = new HashMap<>();
        sendExtFields.put("a", PRODUCER_GROUP);
        sendExtFields.put("b", TEST_TOPIC);
        sendExtFields.put("c", "TBW102");
        sendExtFields.put("d", "4");
        sendExtFields.put("e", String.valueOf(-1));
        sendExtFields.put("f", "0");
        sendExtFields.put("g", String.valueOf(System.currentTimeMillis()));
        sendExtFields.put("h", "0");
        sendExtFields.put("j", "0");
        sendExtFields.put("k", "false");
        sendExtFields.put("l", "16");
        sendExtFields.put("m", "false");

        RemotingCommand sendRequest = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, null);
        sendRequest.setExtFields(sendExtFields);
        sendRequest.setBody("Pull Test Message".getBytes("UTF-8"));

        RemotingCommand sendResponse = proxyClient.invokeSync(proxyAddr, sendRequest, 10000);
        assertEquals("Send should succeed before pull test", RemotingSysResponseCode.SUCCESS, sendResponse.getCode());

        int queueId = Integer.parseInt(sendResponse.getExtFields().get("queueId"));
        long queueOffset = Long.parseLong(sendResponse.getExtFields().get("queueOffset"));

        Thread.sleep(500);

        HashMap<String, String> pullExtFields = new HashMap<>();
        pullExtFields.put("consumerGroup", CONSUMER_GROUP);
        pullExtFields.put("topic", TEST_TOPIC);
        pullExtFields.put("queueId", String.valueOf(queueId));
        pullExtFields.put("queueOffset", String.valueOf(queueOffset));
        pullExtFields.put("maxMsgNums", "32");
        pullExtFields.put("sysFlag", "0");
        pullExtFields.put("commitOffset", "0");
        pullExtFields.put("suspendTimeoutMillis", "0");
        pullExtFields.put("subVersion", String.valueOf(System.currentTimeMillis()));
        pullExtFields.put("expressionType", "TAG");

        RemotingCommand pullRequest = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, null);
        pullRequest.setExtFields(pullExtFields);

        RemotingCommand pullResponse = proxyClient.invokeSync(proxyAddr, pullRequest, 10000);
        System.out.println("Pull response: code=" + pullResponse.getCode() + ", bodyLen=" + (pullResponse.getBody() != null ? pullResponse.getBody().length : 0));

        assertTrue("Pull should return SUCCESS, PULL_NOT_FOUND, PULL_RETRY_IMMEDIATELY or SUBSCRIPTION_GROUP_NOT_EXIST",
                pullResponse.getCode() == RemotingSysResponseCode.SUCCESS
                        || pullResponse.getCode() == ResponseCode.PULL_NOT_FOUND
                        || pullResponse.getCode() == ResponseCode.PULL_RETRY_IMMEDIATELY
                        || pullResponse.getCode() == 24);

    }

    @Test
    public void testProxyQueryConsumerOffset() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", CONSUMER_GROUP);
        extFields.put("topic", TEST_TOPIC);
        extFields.put("queueId", "0");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, null);
        request.setExtFields(extFields);

        RemotingCommand response = proxyClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("Query offset: code=" + response.getCode() + ", extFields=" + response.getExtFields());

        if (response.getCode() == RemotingSysResponseCode.SUCCESS) {
            assertNotNull("offset should not be null", response.getExtFields().get("offset"));
        }
    }

    @Test
    public void testProxyUpdateConsumerOffset() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("consumerGroup", CONSUMER_GROUP);
        extFields.put("topic", TEST_TOPIC);
        extFields.put("queueId", "0");
        extFields.put("commitOffset", "0");

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, null);
        request.setExtFields(extFields);

        RemotingCommand response = proxyClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("Update offset: code=" + response.getCode());
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
    }

    @Test
    public void testProxyGetRouteInfo() throws Exception {
        String proxyAddr = "127.0.0.1:" + proxyPort;

        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", TEST_TOPIC);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.setExtFields(extFields);

        RemotingCommand response = proxyClient.invokeSync(proxyAddr, request, 10000);
        System.out.println("Get route info: code=" + response.getCode() + ", bodyLen=" + (response.getBody() != null ? response.getBody().length : 0));

        assertTrue("Get route info should return SUCCESS or TOPIC_NOT_EXIST",
                response.getCode() == RemotingSysResponseCode.SUCCESS
                        || response.getCode() == ResponseCode.TOPIC_NOT_EXIST);

        if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
            String json = new String(response.getBody(), "UTF-8");
            assertTrue("Route info should contain brokerDatas", json.contains("brokerDatas"));
        }
    }

    private String discoverBrokerAddr() throws Exception {
        return discoverBrokerAddr(namesrvClient, NAMESRV_ADDR);
    }

    public static String discoverBrokerAddr(NettyRemotingClient client, String namesrvAddr) throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("topic", "TBW102");
        request.setExtFields(extFields);
        request.makeCustomHeaderToNet();

        try {
            RemotingCommand response = client.invokeSync(namesrvAddr, request, 5000);
            if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
                TopicRouteInfo routeInfo = RouteInfoSerializer.decodeTopicRouteInfo(response.getBody());
                if (routeInfo.getBrokerDatas() != null && !routeInfo.getBrokerDatas().isEmpty()) {
                    Map<Long, String> addrs = routeInfo.getBrokerDatas().get(0).getBrokerAddrs();
                    if (addrs != null && !addrs.isEmpty()) {
                        String masterAddr = addrs.get(0L);
                        if (masterAddr != null) {
                            return masterAddr;
                        }
                        return addrs.values().iterator().next();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to discover broker from TBW102: " + e.getMessage());
        }

        request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);
        try {
            RemotingCommand response = client.invokeSync(namesrvAddr, request, 5000);
            if (response.getCode() == RemotingSysResponseCode.SUCCESS && response.getBody() != null) {
                String json = new String(response.getBody(), "UTF-8");
                json = RouteInfoSerializer.fixNumericKeys(json);
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                Map<String, Object> clusterInfo = mapper.readValue(json, Map.class);
                Map<String, Object> brokerAddrTable = (Map<String, Object>) clusterInfo.get("brokerAddrTable");
                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Map<String, Object> firstBroker = (Map<String, Object>) brokerAddrTable.values().iterator().next();
                    Map<String, Object> brokerAddrs = (Map<String, Object>) firstBroker.get("brokerAddrs");
                    if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                        Object masterAddr = brokerAddrs.get("0");
                        if (masterAddr == null) {
                            masterAddr = brokerAddrs.values().iterator().next();
                        }
                        return masterAddr.toString();
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to discover broker from cluster info: " + e.getMessage());
        }

        return null;
    }

    private int findAvailablePort() throws Exception {
        ServerSocket ss = new ServerSocket(0);
        int port = ss.getLocalPort();
        ss.close();
        return port;
    }
}
